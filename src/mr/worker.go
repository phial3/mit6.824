package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
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

// for sorting by key.
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
				doReduce(reply.Task, reply.NReduce, reducef)
			}
		}
		time.Sleep(100 * time.Millisecond)
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
	fileMap := make(map[int]string)
	for i := 0; i < nReduce; i++ {
		wFilename := fmt.Sprintf("mr-%v-%v", taskId, i)
		fileMap[i] = wFilename
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
	RpcTaskEnd(task, true, fileMap)
}

func doReduce(task *TaskInfo, nReduce int, reducef func(string, []string) string) {
	taskId := task.TaskId
	intermediate := make([]KeyValue, 0)
	for _, filename := range task.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %s", filename)
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
	oname := fmt.Sprintf("mr-out-%d", taskId)
	ofile, _ := os.Create(oname)
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
	RpcTaskEnd(task, true, make(map[int]string))
}

func RequestForMapTask() *GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	defer func() {
		if reply.Task != nil {
			DPrintf("get map task...code:%d,task:%+v", reply.Code, *reply.Task)
		} else {
			DPrintf("get map task...code:%d,task:%+v", reply.Code, nil)
		}
	}()
	if ok := call("Coordinator.GetTask", &args, &reply); !ok {
		return nil
	} else {
		return &reply
	}
}

func RpcTaskEnd(task *TaskInfo, success bool, fileMap map[int]string) {
	args := TaskEndArgs{task, success, fileMap}
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
