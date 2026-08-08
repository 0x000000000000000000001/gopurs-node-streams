package Test_Main4

import "gopurs/output/Node.EventEmitter"

func newStream() interface{} {
	return Node_EventEmitter.NewImpl(nil)
}

var Stdin = newStream()
