package Node_Stream

import (
    "gopurs/output/gopurs_runtime"
    "os"
    "io"
    "fmt"
    "sync"
)

type MockStream struct {
    Closed bool
    Err    error
}

func extractAny(val interface{}) any {
    if v, ok := val.(gopurs_runtime.Value); ok {
        if v.Type == gopurs_runtime.TypeAny {
            return *(*any)(v.UnsafePtr)
        }
    }
    return val
}

func newStream() interface{} {
    ee := Node_EventEmitter_NewImpl(nil).(*EventEmitter)
    ee.Any = &MockStream{}
	return ee
}

func ReadChunkImpl(useBuffer interface{}, useString interface{}, chunk interface{}) interface{} {
    chunkVal := extractAny(chunk)
    switch v := chunkVal.(type) {
    case []byte:
        return gopurs_runtime.Apply(useBuffer.(gopurs_runtime.Value), chunk.(gopurs_runtime.Value))
    case string:
        return gopurs_runtime.Apply(useString.(gopurs_runtime.Value), chunk.(gopurs_runtime.Value))
    default:
        panic(fmt.Sprintf("Node.Stream.readChunkImpl: Unrecognised chunk type; got: %T", v))
    }
}
func ReadImpl(_ interface{}) interface{} { return nil }
func ReadSizeImpl(_ interface{}, _ interface{}) interface{} { return nil }
func SetEncodingImpl(_ interface{}, _ interface{}) interface{} { return nil }
func ReadableImpl(_ interface{}) interface{} { return false }
func ReadableEndedImpl(_ interface{}) interface{} { return false }
func ReadableFlowingImpl(_ interface{}) interface{} { return false }
func ReadableHighWaterMarkImpl(_ interface{}) interface{} { return false }
func ReadableLengthImpl(_ interface{}) interface{} { return false }
func ResumeImpl(_ interface{}) interface{} { return nil }
func PauseImpl(_ interface{}) interface{} { return nil }
func IsPausedImpl(_ interface{}) interface{} { return false }
func PipeImpl(readable interface{}, writable interface{}) interface{} {
	return nativePipeStart(readable, writable, true)
}
func PipeCbImpl(readable interface{}, writable interface{}, options interface{}) interface{} {
	end := gopurs_runtime.RecordGet(options.(gopurs_runtime.Value), "end")
	return nativePipeStart(readable, writable, end.IntVal != 0)
}

type nativePipe struct {
	source, destination  gopurs_runtime.Value
	writer               io.Writer
	end                  bool
	mu                   sync.Mutex
	writeMu              sync.Mutex
	active               bool
	data, ended, stopped gopurs_runtime.Value
}

func (p *nativePipe) isActive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active
}
func (p *nativePipe) markStopped() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	wasActive := p.active
	p.active = false
	return wasActive
}
func (p *nativePipe) detach() {
	Node_EventEmitter_GopursUnsafeOff(p.source, "data", p.data, nil)
	Node_EventEmitter_GopursUnsafeOff(p.source, "end", p.ended, nil)
	for _, event := range []string{"error", "close"} {
		Node_EventEmitter_GopursUnsafeOff(p.source, event, p.stopped, nil)
	}
	for _, event := range []string{"error", "close", "finish"} {
		Node_EventEmitter_GopursUnsafeOff(p.destination, event, p.stopped, nil)
	}
}
func (p *nativePipe) stop() {
	if p.markStopped() {
		p.detach()
		Node_EventEmitter_GopursUnsafeEmitFn2(p.destination, "unpipe", p.source, nil)
	}
}
func (p *nativePipe) write(chunk gopurs_runtime.Value) {
	p.writeMu.Lock()
	if !p.isActive() {
		p.writeMu.Unlock()
		return
	}
	var data []byte
	var err error
	switch value := extractAny(chunk).(type) {
	case []byte:
		data = value
	default:
		if chunk.Type == gopurs_runtime.TypeString {
			data = []byte(chunk.StrVal())
		} else {
			err = fmt.Errorf("Node.Stream.pipe: unsupported chunk type %T", value)
		}
	}
	if err == nil {
		n, writeError := p.writer.Write(data)
		err = writeError
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
	}
	stopped := err != nil && p.markStopped()
	p.writeMu.Unlock()
	if err != nil {
		if stopped {
			p.detach()
			Node_EventEmitter_GopursUnsafeEmitFn2(p.destination, "unpipe", p.source, nil)
		}
		Node_EventEmitter_GopursUnsafeEmitFn2(p.destination, "error", gopurs_runtime.Box(err), nil)
	}
}
func (p *nativePipe) finish() {
	p.writeMu.Lock()
	if !p.markStopped() {
		p.writeMu.Unlock()
		return
	}
	var err error
	shouldEnd := p.end && p.writer != os.Stdout && p.writer != os.Stderr
	if shouldEnd {
		if writer, ok := p.writer.(interface{ CloseWrite() error }); ok {
			err = writer.CloseWrite()
		} else if writer, ok := p.writer.(io.Closer); ok {
			err = writer.Close()
		}
	}
	p.writeMu.Unlock()
	p.detach()
	if err != nil {
		Node_EventEmitter_GopursUnsafeEmitFn2(p.destination, "error", gopurs_runtime.Box(err), nil)
	} else if shouldEnd {
		Node_EventEmitter_GopursUnsafeEmitFn1(p.destination, "finish", nil)
	}
	Node_EventEmitter_GopursUnsafeEmitFn2(p.destination, "unpipe", p.source, nil)
}
func nativePipeStart(readable interface{}, writable interface{}, end bool) interface{} {
	source := readable.(gopurs_runtime.Value)
	destination := writable.(gopurs_runtime.Value)
	emitter := extractAny(destination).(*EventEmitter)
	writer, ok := emitter.Any.(io.Writer)
	if !ok {
		Node_EventEmitter_GopursUnsafeEmitFn2(destination, "error", gopurs_runtime.Box(fmt.Errorf("Node.Stream.pipe: destination has no native writer")), nil)
		return nil
	}
	p := &nativePipe{source: source, destination: destination, writer: writer, end: end, active: true}
	p.data = gopurs_runtime.Func(func(chunk gopurs_runtime.Value) gopurs_runtime.Value { p.write(chunk); return gopurs_runtime.Value{} })
	p.ended = gopurs_runtime.Func(func(_ gopurs_runtime.Value) gopurs_runtime.Value { p.finish(); return gopurs_runtime.Value{} })
	p.stopped = gopurs_runtime.Func(func(_ gopurs_runtime.Value) gopurs_runtime.Value { p.stop(); return gopurs_runtime.Value{} })
	Node_EventEmitter_GopursUnsafeOn(source, "end", p.ended, nil)
	for _, event := range []string{"error", "close"} {
		Node_EventEmitter_GopursUnsafeOn(source, event, p.stopped, nil)
	}
	for _, event := range []string{"error", "close", "finish"} {
		Node_EventEmitter_GopursUnsafeOn(destination, event, p.stopped, nil)
	}
	Node_EventEmitter_GopursUnsafeEmitFn2(destination, "pipe", source, nil)
	// Register data last: a native file/HTTPS producer may start at this point.
	if p.isActive() {
		Node_EventEmitter_GopursUnsafeOn(source, "data", p.data, nil)
	}
	if !p.isActive() {
		p.detach()
	}
	return nil
}

// These adapters give file streams a producer while leaving event-driven
// sockets and HTTPS responses with their existing, single reader.
type nativeReadable struct {
	reader    io.ReadCloser
	openError error
	emitter   *EventEmitter
	once      sync.Once
}

func CreateNativeReadable(reader io.ReadCloser, openError error) interface{} {
	emitter := Node_EventEmitter_NewImpl(nil).(*EventEmitter)
	emitter.Any = &nativeReadable{reader: reader, openError: openError, emitter: emitter}
	return emitter
}
func (r *nativeReadable) OnListenerAdded(event string) {
	if event == "data" {
		r.once.Do(func() { go r.read() })
	}
}
func (r *nativeReadable) read() {
	boxed := gopurs_runtime.Box(r.emitter)
	defer func() {
		if r.openError == nil && r.reader != nil {
			if err := r.reader.Close(); err != nil {
				Node_EventEmitter_GopursUnsafeEmitFn2(boxed, "error", gopurs_runtime.Box(err), nil)
			}
		}
		Node_EventEmitter_GopursUnsafeEmitFn1(boxed, "close", nil)
	}()
	if r.openError != nil {
		Node_EventEmitter_GopursUnsafeEmitFn2(boxed, "error", gopurs_runtime.Box(r.openError), nil)
		return
	}
	buffer := make([]byte, 32*1024)
	for {
		n, err := r.reader.Read(buffer)
		if n > 0 {
			chunk := append([]byte(nil), buffer[:n]...)
			Node_EventEmitter_GopursUnsafeEmitFn2(boxed, "data", gopurs_runtime.Box(chunk), nil)
		}
		if err != nil {
			if err == io.EOF {
				Node_EventEmitter_GopursUnsafeEmitFn1(boxed, "end", nil)
			} else {
				Node_EventEmitter_GopursUnsafeEmitFn2(boxed, "error", gopurs_runtime.Box(err), nil)
			}
			return
		}
	}
}

type nativeWritable struct {
	writer     io.WriteCloser
	openError  error
	mu         sync.Mutex
	closed     bool
	closeError error
}

func CreateNativeWritable(writer io.WriteCloser, openError error) interface{} {
	emitter := Node_EventEmitter_NewImpl(nil).(*EventEmitter)
	emitter.Any = &nativeWritable{writer: writer, openError: openError}
	return emitter
}
func (w *nativeWritable) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.openError != nil {
		return 0, w.openError
	}
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	return w.writer.Write(data)
}
func (w *nativeWritable) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.openError != nil {
		return w.openError
	}
	if !w.closed {
		w.closed = true
		w.closeError = w.writer.Close()
	}
	return w.closeError
}
func UnpipeImpl(readable interface{}, writable interface{}) interface{} {
	// General unpipe control is not implemented by this targeted pipe adapter.
	return nil
}
func UnpipeAllImpl(_ interface{}) interface{} { return nil }
func WriteableImpl(_ interface{}) interface{} { return false }
func WriteableEndedImpl(_ interface{}) interface{} { return false }
func WriteableCorkedImpl(_ interface{}) interface{} { return false }
func ErroredImpl(_ interface{}) interface{} { return false }
func WriteableFinishedImpl(_ interface{}) interface{} { return false }
func WriteableHighWaterMarkImpl(_ interface{}) interface{} { return 0.0 }
func WriteableLengthImpl(_ interface{}) interface{} { return 0.0 }
func WriteableNeedDrainImpl(_ interface{}) interface{} { return false }
func WriteImpl(writable interface{}, buffer interface{}) interface{} {
    w := extractAny(writable)
    
    var iw io.Writer
    if e, ok := w.(*EventEmitter); ok {
        iw, _ = e.Any.(io.Writer)
    } else {
        iw, _ = w.(io.Writer)
    }
    
    if iw != nil {
        if bufVal, ok2 := buffer.(gopurs_runtime.Value); ok2 {
            if b, ok3 := extractAny(bufVal).([]byte); ok3 {
                iw.Write(b)
            }
        }
    }
    return nil
}
func WriteCbImpl(writable interface{}, buffer interface{}, cb interface{}) interface{} {
    Node_Stream_WriteImpl(writable, buffer)
    w := extractAny(writable)
    if e, ok := w.(*EventEmitter); ok {
        if ms, ok := e.Any.(*MockStream); ok {
            if ms.Err != nil {
                gopurs_runtime.Apply(cb.(gopurs_runtime.Value), gopurs_runtime.Box(ms.Err))
                return nil
            }
            if ms.Closed {
                gopurs_runtime.Apply(cb.(gopurs_runtime.Value), gopurs_runtime.Box(fmt.Errorf("write after end")))
                return nil
            }
        }
    }
    gopurs_runtime.Apply(cb.(gopurs_runtime.Value), gopurs_runtime.Box[any](nil))
    return nil
}
func WriteStringImpl(writable interface{}, str interface{}, enc interface{}) interface{} {
    w := extractAny(writable)
    var iw io.Writer
    if e, ok := w.(*EventEmitter); ok {
        iw, _ = e.Any.(io.Writer)
    } else {
        iw, _ = w.(io.Writer)
    }
    if iw != nil {
        iw.Write([]byte(gopurs_runtime.Unbox[string](str)))
    }
    return nil
}
func WriteStringCbImpl(writable interface{}, str interface{}, enc interface{}, cb interface{}) interface{} {
    Node_Stream_WriteStringImpl(writable, str, enc)
    w := extractAny(writable)
    if e, ok := w.(*EventEmitter); ok {
        if ms, ok := e.Any.(*MockStream); ok {
            if ms.Err != nil {
                gopurs_runtime.Apply(cb.(gopurs_runtime.Value), gopurs_runtime.Box(ms.Err))
                return nil
            }
            if ms.Closed {
                gopurs_runtime.Apply(cb.(gopurs_runtime.Value), gopurs_runtime.Box(fmt.Errorf("write after end")))
                return nil
            }
        }
    }
    gopurs_runtime.Apply(cb.(gopurs_runtime.Value), gopurs_runtime.Box[any](nil))
    return nil
}
func CorkImpl(_ interface{}) interface{} { return nil }
func UncorkImpl(_ interface{}) interface{} { return nil }
func SetDefaultEncodingImpl(writable interface{}, enc interface{}) interface{} { return nil }
func EndImpl(writable interface{}) interface{} {
    w := extractAny(writable)
    var ic io.Closer
    if e, ok := w.(*EventEmitter); ok {
        ic, _ = e.Any.(io.Closer)
        if ms, ok := e.Any.(*MockStream); ok {
            ms.Closed = true
        }
        Node_EventEmitter_GopursUnsafeEmitFn1(gopurs_runtime.Box(e), "finish", nil)
    } else {
        ic, _ = w.(io.Closer)
    }
    if ic != nil {
        ic.Close()
    }
    return nil
}
func EndCbImpl(writable interface{}, cb interface{}) interface{} {
    w := extractAny(writable)
    var err error
    if e, ok := w.(*EventEmitter); ok {
        if ms, ok := e.Any.(*MockStream); ok {
            if ms.Err != nil {
                err = ms.Err
            }
        }
    }
    Node_Stream_EndImpl(writable)
    if err != nil {
        gopurs_runtime.Apply(cb.(gopurs_runtime.Value), gopurs_runtime.Box(err))
    } else {
        gopurs_runtime.Apply(cb.(gopurs_runtime.Value), gopurs_runtime.Box[any](nil))
    }
    return nil
}
func DestroyImpl(stream interface{}) interface{} { return nil }
func DestroyErrorImpl(stream interface{}, err interface{}) interface{} {
    w := extractAny(stream)
    if e, ok := w.(*EventEmitter); ok {
        if ms, ok := e.Any.(*MockStream); ok {
            ms.Err = fmt.Errorf("stream destroyed")
        }
    }
    return nil
}
func ClosedImpl(stream interface{}) interface{} { return false }
func DestroyedImpl(stream interface{}) interface{} { return false }
func AllowHalfOpenImpl(duplex interface{}) interface{} { return false }
func PipelineImpl(r interface{}, arr interface{}, w interface{}, cb interface{}) interface{} { return nil }
func ReadableFromStrImpl(str interface{}, enc interface{}) interface{} { return newStream() }
func ReadableFromBufImpl(buf interface{}) interface{} { return newStream() }
func NewPassThrough() interface{} { return newStream() }
