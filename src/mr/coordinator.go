package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Undo = iota
	Doing
	Finish
	Fail
)

type TaskInfo struct {
	TaskId   int
	Filename string
	Status   int
}

type Coordinator struct {
	// Your definitions here.
	MapTask map[int]TaskInfo
	NReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) TaskEnd(args *MapTaskEndArgs, reply *MapTaskEndReply) error {
	DPrintf("task end[start]...taskId:%d", args.TaskId)
	taskInfo := c.MapTask[args.TaskId]
	if args.success {
		taskInfo.Status = Finish
	} else {
		taskInfo.Status = Fail
	}
	return nil
}

func (c *Coordinator) RequestMapTask(args *RequestMapTaskArgs, reply *RequestMapTaskReply) error {
	DPrintf("request map task[start]...")
	reply = &RequestMapTaskReply{
		TaskId:   -1,
		FileName: "",
		NReduce:  c.NReduce,
	}
	defer func() {
		DPrintf("request map task[end]...%+v", reply)
	}()
	for i, info := range c.MapTask {
		if info.Status == Undo {
			reply.TaskId = i
			reply.FileName = info.Filename
			reply.NReduce = c.NReduce
			info.Status = Doing
			return nil
		}
	}
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.MapTask = make(map[int]TaskInfo)
	for i, name := range files {
		c.MapTask[i] = TaskInfo{
			TaskId:   i,
			Filename: name,
			Status:   Undo,
		}
	}
	c.NReduce = nReduce
	c.server()
	return &c
}
