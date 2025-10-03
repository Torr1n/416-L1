package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"


type Coordinator struct {
	mu               sync.Mutex  // Protects all fields below
	mapTasks         []TaskState // One entry per input file
	reduceTasks      []TaskState // One entry per reduce partition
	nReduce          int         // Number of reduce partitions
	nMap             int         // Number of map tasks
	mapPhaseComplete bool        // True when all map tasks done
	allComplete      bool        // True when all reduce tasks done
}

type TaskState struct {
	status     string    // "idle", "in-progress", "completed"
	assignedAt time.Time // For 10-second timeout detection
	filename   string    // Input file for map tasks (empty for reduce)
}

// Your code here -- RPC handlers for the worker to call.

//
// RequestTask RPC handler - assigns tasks to workers
//
func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Priority 1: Assign map task if map phase not complete
	if !c.mapPhaseComplete {
		for i := range c.mapTasks {
			if c.mapTasks[i].status == "idle" {
				// Assign this map task
				c.mapTasks[i].status = "in-progress"
				c.mapTasks[i].assignedAt = time.Now()

				reply.TaskType = "map"
				reply.TaskID = i
				reply.Filename = c.mapTasks[i].filename
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
		}
		// No idle map tasks, but map phase not done → wait
		reply.TaskType = "wait"
		return nil
	}

	// Priority 2: Assign reduce task if map phase complete
	for i := range c.reduceTasks {
		if c.reduceTasks[i].status == "idle" {
			c.reduceTasks[i].status = "in-progress"
			c.reduceTasks[i].assignedAt = time.Now()

			reply.TaskType = "reduce"
			reply.TaskID = i
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			return nil
		}
	}

	// No tasks available
	if c.allComplete {
		reply.TaskType = "exit" // Signal worker to terminate
	} else {
		reply.TaskType = "wait" // Tasks in progress, wait
	}
	return nil
}

//
// TaskComplete RPC handler - worker reports task completion
//
func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "map" {
		if args.TaskID >= 0 && args.TaskID < len(c.mapTasks) {
			c.mapTasks[args.TaskID].status = "completed"

			// Check if all map tasks completed → phase transition
			allMapsDone := true
			for i := range c.mapTasks {
				if c.mapTasks[i].status != "completed" {
					allMapsDone = false
					break
				}
			}
			if allMapsDone {
				c.mapPhaseComplete = true
			}
		}
	} else if args.TaskType == "reduce" {
		if args.TaskID >= 0 && args.TaskID < len(c.reduceTasks) {
			c.reduceTasks[args.TaskID].status = "completed"

			// Check if all reduce tasks completed → job complete
			allReducesDone := true
			for i := range c.reduceTasks {
				if c.reduceTasks[i].status != "completed" {
					allReducesDone = false
					break
				}
			}
			if allReducesDone {
				c.allComplete = true
			}
		}
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.allComplete
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapTasks = make([]TaskState, len(files))
	c.reduceTasks = make([]TaskState, nReduce)

	// Initialize map tasks (one per input file)
	for i, filename := range files {
		c.mapTasks[i] = TaskState{
			status:   "idle",
			filename: filename,
		}
	}

	// Initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = TaskState{
			status: "idle",
		}
	}

	// Spawn timeout monitoring goroutine
	go func() {
		for !c.Done() {
			time.Sleep(1 * time.Second)

			c.mu.Lock()
			// Check map tasks for timeouts
			for i := range c.mapTasks {
				if c.mapTasks[i].status == "in-progress" {
					if time.Since(c.mapTasks[i].assignedAt) > 10*time.Second {
						c.mapTasks[i].status = "idle" // Reassign timed-out task
					}
				}
			}

			// Check reduce tasks for timeouts
			for i := range c.reduceTasks {
				if c.reduceTasks[i].status == "in-progress" {
					if time.Since(c.reduceTasks[i].assignedAt) > 10*time.Second {
						c.reduceTasks[i].status = "idle" // Reassign timed-out task
					}
				}
			}
			c.mu.Unlock()
		}
	}()

	c.server()
	return &c
}
