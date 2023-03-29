package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	TaskStatusReady   string = "ready"
	TaskStatusQueue   string = "queue"
	TaskStatusRunning string = "running"
	TaskStatusFinish  string = "finish"
	TaskStatusErr     string = "error"
)

const (
	MapPhase    string = "map"
	ReducePhase string = "reduce"
)

type Task struct {
	// nmap
	NumMap int
	// nreduce
	NumReduce int
	// map or reduce
	TaskPhase string
	// task index
	TaskIndex int
	// filename
	FileName string
	// isdone
	IsDone bool
}

type RequestTaskArgs struct {
	// worker status
	WorkerStatus bool
}

type RequestTaskReply struct {
	// task Task
	Task    Task
	AllDone bool
	Wait    bool
}

type ReportTaskArgs struct {
	TaskIndex    int
	WorkerStatus bool
}
type ReportTaskReply struct {
	Ack bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
