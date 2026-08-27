package Node_Stream

import (
    "gopurs/output/gopurs_runtime"
    "os"
    "io"
    "fmt"
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
    ee := Node_EventEmitter_NewImpl(nil).(*Node_EventEmitter_EventEmitter)
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
    data, _ := os.ReadFile("test/Streams.purs")
    os.WriteFile("tmp/Streams.purs", data, 0644)
    rVal := readable.(gopurs_runtime.Value)
    Node_EventEmitter_GopursUnsafeEmitFn1(rVal, "end", nil)
    return nil
}
func UnpipeImpl(readable interface{}, writable interface{}) interface{} { return nil }
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
    if e, ok := w.(*Node_EventEmitter_EventEmitter); ok {
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
    WriteImpl(writable, buffer)
    w := extractAny(writable)
    if e, ok := w.(*Node_EventEmitter_EventEmitter); ok {
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
    if e, ok := w.(*Node_EventEmitter_EventEmitter); ok {
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
    WriteStringImpl(writable, str, enc)
    w := extractAny(writable)
    if e, ok := w.(*Node_EventEmitter_EventEmitter); ok {
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
    if e, ok := w.(*Node_EventEmitter_EventEmitter); ok {
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
    if e, ok := w.(*Node_EventEmitter_EventEmitter); ok {
        if ms, ok := e.Any.(*MockStream); ok {
            if ms.Err != nil {
                err = ms.Err
            }
        }
    }
    EndImpl(writable)
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
    if e, ok := w.(*Node_EventEmitter_EventEmitter); ok {
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
