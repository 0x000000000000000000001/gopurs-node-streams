package Test_Main

import "gopurs/output/Node.EventEmitter"

func newStream() interface{} {
	return Node_EventEmitter.NewImpl(nil)
}

func CreateGzip(_ interface{}) interface{} {
    return newStream()
}

func CreateGunzip(_ interface{}) interface{} {
    return newStream()
}
