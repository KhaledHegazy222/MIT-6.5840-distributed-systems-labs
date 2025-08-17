package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"sync"
)

type Task struct {
	Idx      int
	TaskType TaskType
	Input    []string
	Output   []string
	Status   TaskStatus
	mu       *sync.Mutex
}

type TaskType int

const (
	TypeMap TaskType = iota
	TypeReduce
	TypeWait
	TypeExit
)

type TaskStatus int

const (
	StatusPending TaskStatus = iota
	StatusInProgress
	StatusFinished
)

// Add your RPC definitions here.
type FetchTaskArgs struct{}

type FetchTaskReply struct {
	Task Task
}

type MarkFinishedArgs struct {
	Idx int
}

type MarkFinishedReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
