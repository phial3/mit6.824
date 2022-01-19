package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		reply := RequestForMapTask()
		if reply != nil && reply.Code != TaskWait {
			if reply.Code == End {
				return
			}
			if reply.Task.TaskType == Map {
				doMap(reply.Task, reply.NReduce, mapf)
			} else if reply.Task.TaskType == Reduce {
				doReduce(reducef)
			}
		}
	}
}

func doMap(task *TaskInfo, nReduce int, mapf func(string, string) []KeyValue) {
	taskId := task.TaskId
	filename := task.Files[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %s", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	//根据hash
	intermediate := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		intermediate[i] = make([]KeyValue, 0)
	}
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}
	//写文件
	for i := 0; i < nReduce; i++ {
		wFilename := fmt.Sprintf("mr-tmp/mr-%v-%v", taskId, i)
		wFile, err := os.Create(wFilename)
		if err != nil {
			log.Fatalf("cannot open %v", wFilename)
		}
		enc := json.NewEncoder(wFile)
		for _, kv := range intermediate[i] {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write file: %v", wFilename)
			}
		}
	}
	//结果通知coordinator
	RpcTaskEnd(task, true)
}

func doReduce(reducef func(string, []string) string) {
	//TODO:
}

func RequestForMapTask() *GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	defer func() {
		DPrintf("get map task...%+v", reply)
	}()
	if ok := call("Coordinator.GetTask", &args, &reply); !ok {
		return nil
	} else {
		return &reply
	}
}

func RpcTaskEnd(task *TaskInfo, success bool) {
	args := TaskEndArgs{task, success}
	call("Coordinator.TaskEnd", &args, &TaskEndReply{})
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
