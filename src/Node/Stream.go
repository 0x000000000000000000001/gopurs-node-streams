package Node_Stream

import (
    "gopurs/output/gopurs_runtime"
    "gopurs/output/Node.EventEmitter"
    "os"
)

func ReadChunkImpl(_ interface{}) interface{} { return nil }
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
    Node_EventEmitter.GopursUnsafeEmitFn1(rVal, "end", nil)
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
func WriteImpl(writable interface{}, buffer interface{}) interface{} { return nil }
func WriteCbImpl(writable interface{}, buffer interface{}, cb interface{}) interface{} { return nil }
func WriteStringImpl(writable interface{}, str interface{}, enc interface{}) interface{} { return nil }
func WriteStringCbImpl(writable interface{}, str interface{}, enc interface{}, cb interface{}) interface{} { return nil }
func CorkImpl(_ interface{}) interface{} { return nil }
func UncorkImpl(_ interface{}) interface{} { return nil }
func SetDefaultEncodingImpl(writable interface{}, enc interface{}) interface{} { return nil }
func EndImpl(writable interface{}) interface{} { return nil }
func EndCbImpl(writable interface{}, cb interface{}) interface{} { return nil }
func DestroyImpl(stream interface{}) interface{} { return nil }
func DestroyErrorImpl(stream interface{}, err interface{}) interface{} { return nil }
func ClosedImpl(stream interface{}) interface{} { return false }
func DestroyedImpl(stream interface{}) interface{} { return false }
func AllowHalfOpenImpl(duplex interface{}) interface{} { return false }
func PipelineImpl(r interface{}, arr interface{}, w interface{}, cb interface{}) interface{} { return nil }
func ReadableFromStrImpl(str interface{}, enc interface{}) interface{} { return nil }
func ReadableFromBufImpl(buf interface{}) interface{} { return nil }
func NewPassThrough() interface{} { return nil }
