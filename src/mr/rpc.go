package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type AssignTaskArgs struct{}

type AssignTaskReply struct {
	NReduce  int
	TaskType int
	Index    int
	Filename []string
}

type MapCompleteArgs struct {
	Index    int
	Filename []string
}

type MapCompleteReply struct{}

type ReduceCompleteArgs struct {
	Index int
}

type ReduceCompleteReply struct{}

const (
	MapTask = iota
	ReduceTask
	CompleteTask
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

