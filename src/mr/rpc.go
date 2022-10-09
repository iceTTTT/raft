package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// To remove annoying error.
type Myint int 

func (i Myint) Error() string {
	return "1"
}

// For reduce-worker to send back completion information.
type ReduceNum uint32

// M-R pair for store Map intermediate file name. Use along with location.
type Pair struct {
	M uint32
	R uint32
}

type Location struct {
	Mpair []Pair
	Filename []string
}
// Task type 0 means Map worker, 1 means Reduce worker.
type Task struct {
	Tasktype uint32
	Taskfile []string
	Tasknum uint32
	Isget bool
	Nofr uint32
}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
