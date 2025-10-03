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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

//
// Worker requests task from coordinator
//
type TaskArgs struct {
	// Empty for now - could add WorkerID for debugging if needed
}

type TaskReply struct {
	TaskType string // "map", "reduce", "wait", "exit"
	TaskID   int    // Unique task identifier
	Filename string // Input file for map tasks (empty for reduce)
	NReduce  int    // Number of reduce partitions
	NMap     int    // Total number of map tasks
}

//
// Worker reports task completion to coordinator
//
type TaskCompleteArgs struct {
	TaskType string // "map" or "reduce"
	TaskID   int    // Which task was completed
}

type TaskCompleteReply struct {
	// Empty acknowledgment
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/416-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
