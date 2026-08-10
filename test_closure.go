package main

import (
	"fmt"
	"unsafe"
	"runtime"
)

type Value struct {
	Type      int
	UnsafePtr unsafe.Pointer
}

var EscapeSink any

func Func(f func(Value) Value) Value {
	EscapeSink = f
	return Value{Type: 1, UnsafePtr: *(*unsafe.Pointer)(unsafe.Pointer(&f))}
}

func Apply(f Value, arg Value) Value {
	return (*(*func(Value) Value)(unsafe.Pointer(&f.UnsafePtr)))(arg)
}

func main() {
	captured := "hello world"
	v := Func(func(a Value) Value {
		fmt.Println("Captured:", captured)
		return a
	})
	
	// Force GC to run multiple times
	runtime.GC()
	runtime.GC()
	
	Apply(v, Value{})
}
