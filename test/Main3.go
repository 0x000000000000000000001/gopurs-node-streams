package Test_Main3

import "gopurs/output/Node.EventEmitter"
import "gopurs/output/gopurs_runtime"

func newStream() interface{} {
	return Node_EventEmitter.NewImpl(nil)
}

func CreateReadStream(path interface{}) interface{} {
    return func(_ interface{}) interface{} { return newStream() }
}

func Argv(_ interface{}) interface{} {
    return []gopurs_runtime.Value{gopurs_runtime.Str("node"), gopurs_runtime.Str("script.js")}
}
