package Test_Main3

import "gopurs/output/gopurs_runtime"

func newStreamMain3() interface{} {
	return Node_EventEmitter_NewImpl(nil)
}

func CreateReadStream(path interface{}) interface{} {
    return func(_ interface{}) interface{} { return newStreamMain3() }
}

func Argv(_ interface{}) interface{} {
    return []gopurs_runtime.Value{gopurs_runtime.Str("node"), gopurs_runtime.Str("script.js")}
}
