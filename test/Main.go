package Test_Main


func newStream() interface{} {
	return Node_EventEmitter_NewImpl(nil)
}

func CreateGzip(_ interface{}) interface{} {
    return newStream()
}

func CreateGunzip(_ interface{}) interface{} {
    return newStream()
}
