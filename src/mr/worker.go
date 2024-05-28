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
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueArray []KeyValue

func (a KeyValueArray) Len() int           { return len(a) }
func (a KeyValueArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyValueArray) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		getTaskRequest := GetTaskRequest{}
		getTaskResponse := GetTaskResponse{}
		if !call("Coordinator.GetTask", &getTaskRequest, &getTaskResponse) {
			return
		}

		switch getTaskResponse.TaskType {
		case Map:
			executeMapTask(mapf, getTaskResponse.MapFileName, getTaskResponse.TaskID, getTaskResponse.NReduce)
			call("Coordinator.CommitTask",
				&CommitTaskRequest{
					TaskType: Map,
					TaskID:   getTaskResponse.TaskID,
				},
				&CommitTaskResponse{},
			)
		case Reduce:
			executeReduceTask(reducef, getTaskResponse.ReduceFileNames, getTaskResponse.TaskID)
			call("Coordinator.CommitTask",
				&CommitTaskRequest{
					TaskType: Reduce,
					TaskID:   getTaskResponse.TaskID,
				},
				&CommitTaskResponse{},
			)
		default:
			return
		}
	}
}

func executeMapTask(mapf func(string, string) []KeyValue, mapFileName string, taskID int, nReduce int) {
	file, err := os.Open(mapFileName)
	if err != nil {
		log.Fatalf("cannot open %v", mapFileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapFileName)
	}
	file.Close()
	kva := mapf(mapFileName, string(content))

	mapRes := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		mapRes[i] = make([]KeyValue, 0)
	}

	for _, kv := range kva {
		reduceTaskID := ihash(kv.Key) % nReduce
		mapRes[reduceTaskID] = append(mapRes[reduceTaskID], kv)
	}

	for i := 0; i < nReduce; i++ {
		outFileName := GetMapResFileName(taskID, i)
		var tmpFileName string
		func() {
			outFile, err := ioutil.TempFile("", outFileName)
			if err != nil {
				log.Fatalf("cannot create temp file %v", outFileName)
			}
			defer outFile.Close()

			tmpFileName = outFile.Name()
			encoder := json.NewEncoder(outFile)
			for _, kv := range mapRes[i] {
				err := encoder.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode %v", kv)
				}
			}
		}()
		os.Rename(tmpFileName, outFileName)
	}
}

func executeReduceTask(reducef func(string, []string) string, reduceFileNames []string, taskID int) {
	mapRes := make([]KeyValue, 0)
	for _, reduceFileName := range reduceFileNames {
		func() {
			file, err := os.Open(reduceFileName)
			if err != nil {
				log.Fatalf("cannot open %v", reduceFileName)
			}
			defer file.Close()

			decoder := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := decoder.Decode(&kv); err != nil {
					break
				}
				mapRes = append(mapRes, kv)
			}
		}()
	}

	sort.Sort(KeyValueArray(mapRes))

	outFileName := fmt.Sprintf("mr-out-%v", taskID)
	var tmpFileName string
	func() {
		outFile, err := ioutil.TempFile("", outFileName)
		if err != nil {
			log.Fatalf("cannot create temp file %v", outFileName)
		}
		defer outFile.Close()

		tmpFileName = outFile.Name()
		i := 0
		for i < len(mapRes) {
			j := i + 1
			for j < len(mapRes) && mapRes[j].Key == mapRes[i].Key {
				j++
			}

			values := make([]string, 0)
			for k := i; k < j; k++ {
				values = append(values, mapRes[k].Value)
			}
			result := reducef(mapRes[i].Key, values)
			fmt.Fprintf(outFile, "%v %v\n", mapRes[i].Key, result)
			i = j
		}
	}()
	os.Rename(tmpFileName, outFileName)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
