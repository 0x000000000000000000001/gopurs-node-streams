package Node_EventEmitter

import (
	"bytes"
	"errors"
	"gopurs/output/gopurs_runtime"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type pipeWriterProbe struct {
	bytes.Buffer
	closed int
}

func pipeEmitters(writer io.Writer) (gopurs_runtime.Value, gopurs_runtime.Value) {
	r, w := NewImpl(nil).(*EventEmitter), NewImpl(nil).(*EventEmitter)
	r.Any, w.Any = &pipeEventSource{}, writer
	return gopurs_runtime.Box(r), gopurs_runtime.Box(w)
}
func pipeOn(emitter gopurs_runtime.Value, event string, callback func(gopurs_runtime.Value)) {
	GopursUnsafeOn(emitter, event, gopurs_runtime.Func(func(value gopurs_runtime.Value) gopurs_runtime.Value {
		callback(value)
		return gopurs_runtime.Value{}
	}), nil)
}
func TestPipeEndFalseKeepsWriterOpen(t *testing.T) {
	writer := &pipeWriterProbe{}
	r, w := pipeEmitters(writer)
	finishes := 0
	pipeOn(w, "finish", func(gopurs_runtime.Value) { finishes++ })
	PipeCbImpl(r, w, gopurs_runtime.RecordDict1("end", gopurs_runtime.Bool(false)))
	GopursUnsafeEmitFn2(r, "data", gopurs_runtime.Str("text chunk"), nil)
	GopursUnsafeEmitFn1(r, "end", nil)
	if writer.String() != "text chunk" || writer.closed != 0 || finishes != 0 {
		t.Fatalf("body=%q closes=%d finishes=%d", writer.String(), writer.closed, finishes)
	}
	if n := ListenerCountImpl(r, "data", nil).(int64); n != 0 {
		t.Fatalf("pipe retained %d data listeners", n)
	}
}

type pipeFailingWriter struct {
	writes, closes int
	short          bool
	closeError     bool
}

func (w *pipeFailingWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.short {
		return len(p) - 1, nil
	}
	if w.closeError {
		return len(p), nil
	}
	return 0, errors.New("destination write failure")
}
func (w *pipeFailingWriter) Close() error {
	w.closes++
	if w.closeError {
		return errors.New("destination close failure")
	}
	return nil
}
func TestPipeWriteShortWriteAndCloseErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		writer pipeFailingWriter
	}{
		{name: "write"}, {name: "short", writer: pipeFailingWriter{short: true}},
		{name: "close", writer: pipeFailingWriter{closeError: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writer := tc.writer
			r, w := pipeEmitters(&writer)
			failures, finishes := 0, 0
			pipeOn(w, "error", func(v gopurs_runtime.Value) {
				failures++
				if tc.name == "short" && !errors.Is(extractAny(v).(error), io.ErrShortWrite) {
					t.Error("missing short-write error")
				}
			})
			pipeOn(w, "finish", func(gopurs_runtime.Value) { finishes++ })
			PipeImpl(r, w)
			GopursUnsafeEmitFn2(r, "data", gopurs_runtime.Box([]byte("bytes")), nil)
			GopursUnsafeEmitFn1(r, "end", nil)
			GopursUnsafeEmitFn2(r, "data", gopurs_runtime.Box([]byte("must be detached")), nil)
			if failures != 1 || finishes != 0 || writer.writes != 1 {
				t.Fatalf("errors=%d finish=%d writes=%d", failures, finishes, writer.writes)
			}
			if n := ListenerCountImpl(r, "data", nil).(int64); n != 0 {
				t.Error("data listener leaked")
			}
		})
	}
}
func TestPipeSourceErrorDoesNotFinishOrCloseDestination(t *testing.T) {
	writer := &pipeWriterProbe{}
	r, w := pipeEmitters(writer)
	failures, finishes := 0, 0
	pipeOn(r, "error", func(gopurs_runtime.Value) { failures++ })
	pipeOn(w, "finish", func(gopurs_runtime.Value) { finishes++ })
	PipeImpl(r, w)
	GopursUnsafeEmitFn2(r, "error", gopurs_runtime.Box(errors.New("source failed")), nil)
	GopursUnsafeEmitFn1(r, "end", nil)
	if failures != 1 || finishes != 0 || writer.closed != 0 {
		t.Fatalf("errors=%d finishes=%d closes=%d", failures, finishes, writer.closed)
	}
}

type pipeHalfCloser struct {
	pipeWriterProbe
	halfClosed int
}

type pipeBlockedErrorWriter struct {
	started, release chan struct{}
	closed           int
}

func (w *pipeBlockedErrorWriter) Write([]byte) (int, error) {
	close(w.started)
	<-w.release
	return 0, errors.New("write failed while end was waiting")
}
func (w *pipeBlockedErrorWriter) Close() error { w.closed++; return nil }
func TestPipeConcurrentEndCannotHideWriteFailure(t *testing.T) {
	writer := &pipeBlockedErrorWriter{started: make(chan struct{}), release: make(chan struct{})}
	r, w := pipeEmitters(writer)
	finishes, failures := 0, 0
	pipeOn(w, "finish", func(gopurs_runtime.Value) { finishes++ })
	pipeOn(w, "error", func(gopurs_runtime.Value) { failures++ })
	PipeImpl(r, w)
	var group sync.WaitGroup
	group.Add(2)
	go func() { defer group.Done(); GopursUnsafeEmitFn2(r, "data", gopurs_runtime.Box([]byte("data")), nil) }()
	<-writer.started
	ending := make(chan struct{})
	go func() { defer group.Done(); close(ending); GopursUnsafeEmitFn1(r, "end", nil) }()
	<-ending
	time.Sleep(2 * time.Millisecond)
	close(writer.release)
	group.Wait()
	if finishes != 0 || writer.closed != 0 || failures != 1 {
		t.Fatalf("finish=%d closes=%d errors=%d", finishes, writer.closed, failures)
	}
}

type pipeBrokenReader struct{}

func (*pipeBrokenReader) Read(p []byte) (int, error) {
	return copy(p, []byte("partial")), errors.New("reader failed")
}
func (*pipeBrokenReader) Close() error { return nil }
func TestNativePipeReadAndOpenErrors(t *testing.T) {
	for _, name := range []string{"read", "open-source", "open-destination"} {
		t.Run(name, func(t *testing.T) {
			var reader io.ReadCloser = &pipeBrokenReader{}
			var readError, writeError error
			if name == "open-source" {
				reader = nil
				readError = errors.New("source open failed")
			}
			if name == "open-destination" {
				reader = io.NopCloser(bytes.NewReader(nil))
				writeError = errors.New("destination open failed")
			}
			writer := &pipeWriterProbe{}
			r, w := gopurs_runtime.Box(CreateNativeReadable(reader, readError)), gopurs_runtime.Box(CreateNativeWritable(writer, writeError))
			failures := make(chan error, 2)
			closed := make(chan struct{}, 1)
			finished := make(chan struct{}, 1)
			pipeOn(r, "error", func(v gopurs_runtime.Value) { failures <- extractAny(v).(error) })
			pipeOn(w, "error", func(v gopurs_runtime.Value) { failures <- extractAny(v).(error) })
			pipeOn(r, "close", func(gopurs_runtime.Value) { closed <- struct{}{} })
			pipeOn(w, "finish", func(gopurs_runtime.Value) { finished <- struct{}{} })
			PipeImpl(r, w)
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("source failed to close")
			}
			select {
			case err := <-failures:
				if err == nil {
					t.Error("missing error")
				}
			default:
				t.Fatal("error was swallowed")
			}
			select {
			case <-finished:
				t.Error("failed copy emitted finish")
			default:
			}
			if writer.closed != 0 {
				t.Error("failed copy unexpectedly closed destination")
			}
			if name == "read" && writer.String() != "partial" {
				t.Errorf("lost prefix before read error: %q", writer.String())
			}
		})
	}
}

func (w *pipeHalfCloser) CloseWrite() error { w.halfClosed++; return nil }
func TestPipeHalfClosesDuplexDestination(t *testing.T) {
	writer := &pipeHalfCloser{}
	r, w := pipeEmitters(writer)
	PipeImpl(r, w)
	GopursUnsafeEmitFn1(r, "end", nil)
	if writer.halfClosed != 1 || writer.closed != 0 {
		t.Fatalf("half=%d full=%d", writer.halfClosed, writer.closed)
	}
}
func TestPipeDoesNotUseHistoricalFixturePaths(t *testing.T) {
	// The old implementation wrote these paths regardless of its arguments.
	if err := os.MkdirAll("test", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("tmp", 0700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile("test/Streams.purs", []byte("decoy source"), 0600)
	os.WriteFile("tmp/Streams.purs", []byte("keep unrelated destination"), 0600)
	writer := &pipeWriterProbe{}
	r, w := pipeEmitters(writer)
	PipeImpl(r, w)
	GopursUnsafeEmitFn2(r, "data", gopurs_runtime.Box([]byte("real source")), nil)
	GopursUnsafeEmitFn1(r, "end", nil)
	untouched, err := os.ReadFile("tmp/Streams.purs")
	if err != nil {
		t.Fatal(err)
	}
	if string(untouched) != "keep unrelated destination" || writer.String() != "real source" {
		t.Fatal("pipe used unrelated fixture paths")
	}
}
func TestNativeFilePipeCopiesMultipleChunksAndEmptyFile(t *testing.T) {
	for _, size := range []int{0, 131079} {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i * 17)
		}
		dir := t.TempDir()
		src, dst := filepath.Join(dir, "arbitrary-source.bin"), filepath.Join(dir, "arbitrary-destination.bin")
		if err := os.WriteFile(src, data, 0600); err != nil {
			t.Fatal(err)
		}
		reader, err := os.Open(src)
		if err != nil {
			t.Fatal(err)
		}
		writer, err := os.Create(dst)
		if err != nil {
			t.Fatal(err)
		}
		r, w := gopurs_runtime.Box(CreateNativeReadable(reader, nil)), gopurs_runtime.Box(CreateNativeWritable(writer, nil))
		completed := make(chan error, 2)
		closed := make(chan struct{}, 1)
		pipeOn(r, "error", func(v gopurs_runtime.Value) { completed <- extractAny(v).(error) })
		pipeOn(w, "error", func(v gopurs_runtime.Value) { completed <- extractAny(v).(error) })
		pipeOn(w, "finish", func(gopurs_runtime.Value) { completed <- nil })
		pipeOn(r, "close", func(gopurs_runtime.Value) { closed <- struct{}{} })
		PipeImpl(r, w)
		select {
		case err := <-completed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("file pipe did not finish")
		}
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("source file not closed")
		}
		actual, err := os.ReadFile(dst)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual, data) {
			t.Fatalf("copied %d bytes, want exact %d bytes", len(actual), size)
		}
		if _, err := writer.Write([]byte("after finish")); err == nil {
			t.Error("destination file left open")
		}
		if _, err := reader.Read(make([]byte, 1)); err == nil {
			t.Error("source file left open")
		}
	}
}
func TestPipeConcurrentEndOnlyFinishesOnce(t *testing.T) {
	writer := &pipeWriterProbe{}
	r, w := pipeEmitters(writer)
	finishes := 0
	pipeOn(w, "finish", func(gopurs_runtime.Value) { finishes++ })
	PipeImpl(r, w)
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() { defer group.Done(); GopursUnsafeEmitFn1(r, "end", nil) }()
	}
	group.Wait()
	if finishes != 1 || writer.closed != 1 {
		t.Fatalf("finishes=%d closes=%d", finishes, writer.closed)
	}
}
func (w *pipeWriterProbe) Close() error { w.closed++; return nil }

// Net/HTTPS already have producers. Pipe must subscribe, not start a competing read.
type pipeEventSource struct{}

func (*pipeEventSource) Read([]byte) (int, error) { panic("pipe must not read event-driven sources") }

func TestPipeTransfersActualEventChunks(t *testing.T) {
	source := NewImpl(nil).(*EventEmitter)
	source.Any = &pipeEventSource{}
	destination := NewImpl(nil).(*EventEmitter)
	writer := &pipeWriterProbe{}
	destination.Any = writer
	r, w := gopurs_runtime.Box(source), gopurs_runtime.Box(destination)
	finished := 0
	GopursUnsafeOn(w, "finish", gopurs_runtime.Func(func(_ gopurs_runtime.Value) gopurs_runtime.Value {
		finished++
		if writer.closed != 1 {
			t.Error("finish preceded writer close")
		}
		return gopurs_runtime.Value{}
	}), nil)
	PipeImpl(r, w)
	chunks := [][]byte{{0, 255, 10}, []byte("arbitrary pipe payload")}
	for _, chunk := range chunks {
		GopursUnsafeEmitFn2(r, "data", gopurs_runtime.Box(chunk), nil)
	}
	GopursUnsafeEmitFn1(r, "end", nil)
	want := bytes.Join(chunks, nil)
	if !bytes.Equal(writer.Bytes(), want) {
		t.Fatalf("pipe copied %q, want arbitrary source %q", writer.Bytes(), want)
	}
	if finished != 1 || writer.closed != 1 {
		t.Fatalf("finish=%d close=%d, want one each", finished, writer.closed)
	}
}
