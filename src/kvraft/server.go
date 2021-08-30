package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	PutCommand    = "Put"
	AppendCommand = "Append"
	GetCommand    = "Get"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command string
	Key     string
	Value   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//这里相当于论文定义的state machine,更新data必须保证单线程
	data map[string]string
	//更新到状态机的最后的log，这里的定义跟raft的lastApply有些许不一样，这里的lastApply是真正写入状态机的下标
	lastApply int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("Get request[start]...peerId:%d,args:%v", kv.me, args)
	defer func() {
		DPrintf("Get request[finish]...peerId:%d,reply:%v", kv.me, reply)
	}()
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	//为了保证线性一致性，这里需要增加一个相当于ReadLog的做法
	op := Op{GetCommand, args.Key, ""}
	logIdx, _, success := kv.rf.Start(op)
	if !success {
		reply.Err = ErrWrongLeader
		return
	}
	//等待过半数提交
	kv.mu.Lock()
	for kv.lastApply < logIdx {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
	}
	value, exsit := kv.data[args.Key]
	if !exsit {
		reply.Err = ErrNoKey
		reply.Value = ""
		kv.mu.Unlock()
		return
	}
	reply.Err = OK
	reply.Value = value
	kv.mu.Unlock()
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("PutAppend request[start]...peerId:%d,args:%v", kv.me, args)
	defer func() {
		DPrintf("PutAppend request[finish]...peerId:%d,reply:%v", kv.me, reply)
	}()
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{args.Op, args.Key, args.Value}
	logIdx, _, success := kv.rf.Start(op)
	if !success {
		reply.Err = ErrWrongLeader
		return
	}
	//等待过半数提交
	kv.mu.Lock()
	for kv.lastApply < logIdx {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
	}
	kv.mu.Unlock()
	reply.Err = OK
	return
}

//更新状态机数据，需要保证时序，这里只能使用单线程
func (kv *KVServer) applyEntry() {
	for !kv.killed() {
		msg := <-kv.applyCh
		op := msg.Command.(Op)
		kv.mu.Lock()
		switch op.Command {
		case PutCommand:
			DPrintf("put...key:%s,values:%s", op.Key, op.Value)
			kv.data[op.Key] = op.Value
		case AppendCommand:
			DPrintf("append...key:%s,values:%s", op.Key, op.Value)
			v, ok := kv.data[op.Key]
			if !ok {
				kv.data[op.Key] = op.Value
			} else {
				kv.data[op.Key] = v + op.Value
			}
		case GetCommand:
			//no-op
		default:
			panic("unknown command" + op.Command)
		}
		kv.lastApply = msg.CommandIndex
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApply = 0

	go kv.applyEntry()
	//等待选举出一个leader
	//time.Sleep(1000*time.Duration(time.Millisecond))
	// You may need initialization code here.

	return kv
}
