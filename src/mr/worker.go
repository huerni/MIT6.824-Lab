package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
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
/**
* One way to get started is to modify mr/worker.go's Worker()
to send an RPC to the coordinator asking for a task.
* Then modify the coordinator to respond with the file name of an as-yet-unstarted map task.
* Then modify the worker to read that file and call the application Map function, as in mrsequential.go.


A reasonable naming convention for intermediate files is mr-X-Y,
where X is the Map task number, and Y is the reduce task number.

The map part of your worker can use the ihash(key) function (in worker.go)
to pick the reduce task for a given key.


**/
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

	for RequestCall(mapf, reducef) == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
// A reasonable naming convention for intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task number.
func RequestCall(mapf func(string, string) []KeyValue, reducef func(string, []string) string) bool {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	args.WorkerStatus = true
	result := call("Coordinator.HandleTaskRequest", &args, &reply)
	if result {
		if reply.AllDone == true {
			return true
		}
		if reply.Wait == true {
			return false
		}
		// do task
		if reply.Task.TaskPhase == MapPhase {
			DoMap(mapf, &reply)
		} else if reply.Task.TaskPhase == ReducePhase {
			DoReduce(reducef, &reply)
		}
		ReportCall(reply.Task.TaskIndex)
	} else {
		log.Fatal("request task failed!\n")
	}

	return false
}

func DoMap(mapf func(string, string) []KeyValue, reply *RequestTaskReply) {
	intermediate := []KeyValue{}
	file, err := os.Open(reply.Task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Task.FileName)
	}
	file.Close()
	kva := mapf(reply.Task.FileName, string(content))
	intermediate = append(intermediate, kva...)
	// Each mapper should create nReduce intermediate files for consumption by the reduce tasks.
	// The map part of your worker can use the ihash(key) function (in worker.go) to pick the reduce task for a given key ??
	interhash := make(map[int][]KeyValue)
	for _, kv := range intermediate {
		index := ihash(kv.Key) % reply.Task.NumReduce
		interhash[index] = append(interhash[index], kv)
	}

	for key, kvs := range interhash {
		target_file_name := "mr-" + strconv.Itoa(reply.Task.TaskIndex) + "-" + strconv.Itoa(key)
		target_file, err := os.Create(target_file_name)
		if err != nil {
			log.Fatalf("cannot create %v\n", target_file_name)
		}
		enc := json.NewEncoder(target_file)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode\n")
			}
		}
		target_file.Close()
	}
}

func DoReduce(reducef func(string, []string) string, reply *RequestTaskReply) {
	intermediate := []KeyValue{}
	for i := 0; i < reply.Task.NumMap; i++ {
		target_file_name := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Task.TaskIndex)
		file, err := os.Open(target_file_name)
		if err != nil {
			// fmt.Printf("no reduce index %v\n", reply.Task.TaskIndex)
			continue
		}
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
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reply.Task.TaskIndex)
	ofile, err := os.Create(oname)
	if err != nil {
		fmt.Printf("create %v faild\n", oname)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

func ReportCall(taskindex int) {
	args := ReportTaskArgs{}
	args.WorkerStatus = true
	args.TaskIndex = taskindex

	reply := ReportTaskReply{}
	result := call("Coordinator.HandleTaskReport", &args, &reply)
	if result {
		// mt.Printf("report ack id %v\n", taskindex)
	} else {
		log.Fatal("report failed!\n")
	}
}

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
