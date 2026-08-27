package Test_Main


func newStreamMain() interface{} {
	return Node_EventEmitter_NewImpl(nil)
}

func CreateGzip(_ interface{}) interface{} {
    return newStreamMain()
}

func CreateGunzip(_ interface{}) interface{} {
    return newStreamMain()
}
