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
	mu     sync.Mutex
	NextId int
	//map task
	MapTaskQueue *TaskQueue
	MapTaskDoing map[int]TaskInfo
	//MapTaskDone  map[int]TaskInfo
	MapTaskResult map[int][]string
	//reduce task
	ReduceTaskQueue *TaskQueue
	ReduceTaskDoing map[int]TaskInfo
	//ReduceTaskDone  map[int]TaskInfo

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

func (c *Coordinator) TaskEnd(args *TaskEndArgs, reply *TaskEndReply) error {
	c.mu.Lock()
	DPrintf("task end[start]...args:%+v", args)
	defer c.mu.Unlock()
	task := args.Task
	if task.TaskType == Map {
		delete(c.MapTaskDoing, task.TaskId)
		if args.Success {
			//c.MapTaskDone[task.TaskId] = *task
			for par, file := range args.Files {
				c.MapTaskResult[par] = append(c.MapTaskResult[par], file)
			}
		} else {
			c.MapTaskQueue.offer(task)
		}
		//所有map完成，则生成reduce的JOB
		if c.mapTaskEnd() {
			for _, files := range c.MapTaskResult {
				task := TaskInfo{
					TaskType: Reduce,
					TaskId:   c.NextId,
					Files:    files,
				}
				c.NextId++
				c.ReduceTaskQueue.offer(&task)
			}
		}
	} else {
		delete(c.ReduceTaskDoing, task.TaskId)
		if args.Success {
			//DO NOTHING
		} else {
			c.ReduceTaskQueue.offer(task)
		}
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
	task := c.MapTaskQueue.poll()
	if task != nil {
		reply.Code = Success
		reply.Task = task
		reply.NReduce = c.NReduce
		//加入到执行中的队列
		c.MapTaskDoing[task.TaskId] = *task
		return nil
	}
	if !c.mapTaskEnd() {
		reply.Code = TaskWait
		return nil
	}
	task = c.ReduceTaskQueue.poll()
	if task != nil {
		reply.Code = Success
		reply.Task = task
		reply.NReduce = c.NReduce
		//加入到执行中的队列
		c.ReduceTaskDoing[task.TaskId] = *task
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
	return c.MapTaskQueue.size() == 0 && len(c.MapTaskDoing) == 0
}

func (c *Coordinator) reduceTaskEnd() bool {
	return c.ReduceTaskQueue.size() == 0 && len(c.ReduceTaskDoing) == 0
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
	c.MapTaskQueue = MakeTaskQueue()
	c.MapTaskDoing = make(map[int]TaskInfo)
	c.MapTaskResult = make(map[int][]string)
	for i := 0; i < nReduce; i++ {
		c.MapTaskResult[i] = make([]string, 0)
	}
	//c.MapTaskDone = make(map[int]TaskInfo)
	for _, name := range files {
		task := TaskInfo{
			TaskType: Map,
			TaskId:   c.NextId,
			Files:    []string{name},
		}
		c.NextId++
		c.MapTaskQueue.offer(&task)
	}
	DPrintf("map task size,%d", c.MapTaskQueue.size())
	c.ReduceTaskQueue = MakeTaskQueue()
	c.ReduceTaskDoing = make(map[int]TaskInfo)
	//c.ReduceTaskDone = make(map[int]TaskInfo)
	c.NReduce = nReduce
	c.server()
	return &c
}
