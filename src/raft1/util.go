package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = false

var logger = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)

func DPrintf(format string, a ...interface{}) {
	if Debug {
		logger.Printf(format, a...)
	}
}
