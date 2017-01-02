package raftkv

import (
	"bytes"
	"log"
	"runtime"
	"strconv"
)

// Debugging
const Debug = 5

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func dprintf(level int, format string, a ...interface{}) (n int, err error) {
	if level >= Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	dprintf(0, format, a...)
	return
}

func D1Printf(format string, a ...interface{}) (n int, err error) {
	dprintf(1, format, a...)
	return
}

func D2Printf(format string, a ...interface{}) (n int, err error) {
	dprintf(2, format, a...)
	return
}

func D3Printf(format string, a ...interface{}) (n int, err error) {
	dprintf(3, format, a...)
	return
}

func D4Printf(format string, a ...interface{}) (n int, err error) {
	dprintf(4, format, a...)
	return
}
