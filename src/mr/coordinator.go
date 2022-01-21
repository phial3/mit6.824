package mr

import (
	"log"
	"sync"
)
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

type TaskQueue struct {
	queue []TaskInfo
}

func MakeTaskQueue() *TaskQueue {
	return &TaskQueue{queue: []TaskInfo{}}
}

func (q *TaskQueue) poll() *TaskInfo {
	if len(q.queue) == 0 {
		return nil
	}
	task := q.queue[0]
	q.queue = q.queue[1:]
	DPrintf("poll size:%d", q.size())
	return &task
}

func (q *TaskQueue) offer(task *TaskInfo) {
	q.queue = append(q.queue, *task)
}

func (q *TaskQueue) size() int {
	return len(q.queue)
}

type Coordinator struct {
	// Your definitions here.
	mu         sync.Mutex
	NextId     int
	MapTask    map[int]*TaskQueue
	ReduceTask map[int]*TaskQueue
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
	c.mu.Lock()
	DPrintf("task end[start]...args:%+v", args)
	defer c.mu.Unlock()
	task := c.MapTask[Doing].poll()
	if args.Success {
		c.MapTask[Finish].offer(task)
	} else {
		c.MapTask[Undo].offer(task)
	}
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	DPrintf("request map task[start]...")
	defer func() {
		c.mu.Unlock()
		if reply.Task == nil {
			DPrintf("request map task[end]...code:%d", reply.Code)
		} else {
			DPrintf("request map task[end]...code:%d,task:%+v", reply.Code, *reply.Task)
		}
	}()
	//出队
	task := c.MapTask[Undo].poll()
	if task != nil {
		reply.Code = Success
		reply.Task = task
		reply.NReduce = c.NReduce
		//加入到执行中的队列
		c.MapTask[Doing].offer(task)
		return nil
	}
	if !c.mapTaskEnd() {
		reply.Code = TaskWait
		return nil
	}
	task = c.ReduceTask[Undo].poll()
	if task != nil {
		reply.Code = Success
		reply.Task = task
		reply.NReduce = c.NReduce
		//加入到执行中的队列
		c.ReduceTask[Doing].offer(task)
		return nil
	}
	if !c.reduceTaskEnd() {
		reply.Code = TaskWait
		return nil
	}
	reply.Code = End
	return nil
}

func (c *Coordinator) mapTaskEnd() bool {
	return c.MapTask[Undo].size() == 0 && c.MapTask[Doing].size() == 0
}

func (c *Coordinator) reduceTaskEnd() bool {
	return c.ReduceTask[Undo].size() == 0 && c.ReduceTask[Doing].size() == 0
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
	ret := false

	// Your code here.
	ret = c.mapTaskEnd() && c.reduceTaskEnd()
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
	c.MapTask = make(map[int]*TaskQueue)
	c.MapTask[Undo] = MakeTaskQueue()
	c.MapTask[Doing] = MakeTaskQueue()
	c.MapTask[Finish] = MakeTaskQueue()
	for _, name := range files {
		task := TaskInfo{
			TaskType: Map,
			TaskId:   c.NextId,
			Files:    []string{name},
		}
		c.NextId++
		c.MapTask[Undo].offer(&task)
	}
	DPrintf("map task size,%d", c.MapTask[Undo].size())
	c.ReduceTask = make(map[int]*TaskQueue)
	c.ReduceTask[Undo] = MakeTaskQueue()
	c.ReduceTask[Doing] = MakeTaskQueue()
	c.ReduceTask[Finish] = MakeTaskQueue()
	c.NReduce = nReduce
	c.server()
	return &c
}
