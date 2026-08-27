package Test_Main4


func newStream() interface{} {
	return Node_EventEmitter_NewImpl(nil)
}

var Stdin = newStream()
