package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// ByKey for sorting by key (from mrsequential.go lines 18-23)
//
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Worker main loop: request tasks until coordinator signals exit
	for {
		// Request task from coordinator
		args := TaskArgs{}
		reply := TaskReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok || reply.TaskType == "exit" {
			return // Coordinator unreachable or job done
		}

		if reply.TaskType == "wait" {
			time.Sleep(1 * time.Second)
			continue
		}

		if reply.TaskType == "map" {
			executeMapTask(mapf, &reply)
			// Report completion
			completeArgs := TaskCompleteArgs{
				TaskType: "map",
				TaskID:   reply.TaskID,
			}
			completeReply := TaskCompleteReply{}
			call("Coordinator.TaskComplete", &completeArgs, &completeReply)
		}

		if reply.TaskType == "reduce" {
			executeReduceTask(reducef, &reply)
			// Report completion
			completeArgs := TaskCompleteArgs{
				TaskType: "reduce",
				TaskID:   reply.TaskID,
			}
			completeReply := TaskCompleteReply{}
			call("Coordinator.TaskComplete", &completeArgs, &completeReply)
		}
	}
}

//
// executeMapTask reads the input file, calls mapf, partitions output into nReduce buckets,
// and writes intermediate files mr-X-Y
//
func executeMapTask(mapf func(string, string) []KeyValue, reply *TaskReply) {
	// Read input file (pattern from mrsequential.go lines 40-48)
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()

	// Call map function
	kva := mapf(reply.Filename, string(content))

	// Partition into nReduce buckets using ihash(key) % nReduce
	buckets := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % reply.NReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// Write intermediate files mr-X-Y with atomic rename pattern
	for y := 0; y < reply.NReduce; y++ {
		filename := fmt.Sprintf("mr-%d-%d", reply.TaskID, y)

		// Atomic write: temp file + rename
		tempfile, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}

		// Write JSON-encoded KeyValues
		enc := json.NewEncoder(tempfile)
		for _, kv := range buckets[y] {
			enc.Encode(&kv)
		}
		tempfile.Close()

		// Atomic rename (crash-safe)
		if err := os.Rename(tempfile.Name(), filename); err != nil {
			log.Fatalf("rename failed: %v", err)
		}
	}
}

//
// executeReduceTask reads all mr-*-Y intermediate files, sorts, groups by key,
// calls reducef, and writes output to mr-out-Y
//
func executeReduceTask(reducef func(string, []string) string, reply *TaskReply) {
	// Read all intermediate files for this reduce partition
	intermediate := []KeyValue{}
	for mapTask := 0; mapTask < reply.NMap; mapTask++ {
		filename := fmt.Sprintf("mr-%d-%d", mapTask, reply.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			continue // File might not exist if map task failed
		}

		// Decode JSON KeyValues
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// Sort by key (required for grouping)
	sort.Sort(ByKey(intermediate))

	// Write output file with atomic rename pattern
	oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
	tempfile, _ := os.CreateTemp(".", "mr-out-tmp-*")

	// Group by key and call reduce (pattern from mrsequential.go lines 68-84)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++ // Find consecutive identical keys
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// EXACT output format required (mrsequential.go line 81)
		fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempfile.Close()

	// Atomic rename (crash-safe)
	if err := os.Rename(tempfile.Name(), oname); err != nil {
		log.Fatalf("rename failed: %v", err)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
