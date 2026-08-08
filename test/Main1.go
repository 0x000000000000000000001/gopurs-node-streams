package Test_Main1

import "gopurs/output/Node.EventEmitter"

func newStream() interface{} {
	return Node_EventEmitter.NewImpl(nil)
}

func CreateReadStream(path interface{}) interface{} {
    return func(_ interface{}) interface{} { return newStream() }
}

func CreateWriteStream(path interface{}) interface{} {
    return func(_ interface{}) interface{} { return newStream() }
}
