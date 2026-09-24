package wstream

import "runtime"

// heapInUse is a settled reading of the live heap. The garbage collector runs
// first because what is being measured is what a structure HOLDS, not what its
// construction allocated on the way.
func heapInUse() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}
