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
)

//任务类型
const (
	Map    = 1
	Reduce = 2
)

type TaskInfo struct {
	TaskType int
	TaskId   int
	Files    []string
}

type Coordinator struct {
	// Your definitions here.
	NextId     int
	MapTask    map[int][]TaskInfo
	ReduceTask map[int][]TaskInfo
	NReduce    int
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

func (c *Coordinator) TaskEnd(args *TaskEndArgs, reply *TaskEndReply) error {
	DPrintf("task end[start]...args:%+v", args)
	task := pollTask(c.MapTask[Doing])
	if args.success {
		offerTask(c.MapTask[Finish], task)
	} else {
		offerTask(c.MapTask[Undo], task)
	}
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	DPrintf("request map task[start]...")
	defer func() {
		DPrintf("request map task[end]...%+v", reply)
	}()
	//出队
	task := pollTask(c.MapTask[Undo])
	if task != nil {
		reply.Code = Success
		reply.Task = task
		reply.NReduce = c.NReduce
		//加入到执行中的队列
		offerTask(c.MapTask[Doing], task)
	}
	return nil
	if !c.mapTaskEnd() {
		reply.Code = TaskWait
		return nil
	}
	task = pollTask(c.ReduceTask[Undo])
	if task != nil {
		reply.Code = Success
		reply.Task = task
		reply.NReduce = c.NReduce
		//加入到执行中的队列
		offerTask(c.ReduceTask[Doing], task)
		return nil
	}
	if !c.reduceTaskEnd() {
		reply.Code = TaskWait
	}
	return nil
}

func pollTask(queue []TaskInfo) *TaskInfo {
	if len(queue) == 0 {
		return nil
	}
	task := queue[0]
	queue = queue[1:]
	return &task
}

func offerTask(queue []TaskInfo, task *TaskInfo) {
	queue = append(queue, *task)
}

func (c *Coordinator) mapTaskEnd() bool {
	return len(c.MapTask[Undo]) == 0 && len(c.MapTask[Doing]) == 0
}

func (c *Coordinator) reduceTaskEnd() bool {
	return len(c.ReduceTask[Undo]) == 0 && len(c.ReduceTask[Doing]) == 0
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
	c.NextId = 0
	c.MapTask = make(map[int][]TaskInfo)
	c.MapTask[Undo] = make([]TaskInfo, 0)
	c.MapTask[Doing] = make([]TaskInfo, 0)
	c.MapTask[Finish] = make([]TaskInfo, 0)
	for _, name := range files {
		task := TaskInfo{
			TaskType: Map,
			TaskId:   c.NextId,
			Files:    []string{name},
		}
		offerTask(c.MapTask[Undo], &task)
	}
	c.ReduceTask = make(map[int][]TaskInfo)
	c.ReduceTask[Undo] = make([]TaskInfo, 0)
	c.ReduceTask[Doing] = make([]TaskInfo, 0)
	c.ReduceTask[Finish] = make([]TaskInfo, 0)
	c.NReduce = nReduce
	c.server()
	return &c
}
