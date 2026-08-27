package Test_Main1


func newStream() interface{} {
	return Node_EventEmitter_NewImpl(nil)
}

func CreateReadStream(path interface{}) interface{} {
    return func(_ interface{}) interface{} { return newStream() }
}

func CreateWriteStream(path interface{}) interface{} {
    return func(_ interface{}) interface{} { return newStream() }
}
