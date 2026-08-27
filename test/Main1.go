package Test_Main1


func newStreamMain1() interface{} {
	return Node_EventEmitter_NewImpl(nil)
}

func CreateReadStream(path interface{}) interface{} {
    return func(_ interface{}) interface{} { return newStreamMain1() }
}

func CreateWriteStream(path interface{}) interface{} {
    return func(_ interface{}) interface{} { return newStreamMain1() }
}
