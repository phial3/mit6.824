package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
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
	NOOP          = "NOOP"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command string
	Key     string
	Value   string
	//客户端ID
	ClientId int
	//这个唯一ID是每个客户端生成的唯一ID
	UniqId int64
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
	//等待提交channel，这里其实也有空间压力，正式环境肯定是需要考虑空间释放
	//You might assume that you will never see Start() return the same index twice,
	//or at the very least, that if you see the same index again, the command that
	//first returned that index must have failed. It turns out that neither of these
	//things are true, even if no servers crash.
	//简单来说，就是下面的两个假设都是不成立的
	//1.log相同的index只会出现一次
	//2.如果相同的index的log出现两次，那么第一个返回的log必然失败
	waitCh map[int][]chan raft.ApplyMsg
	//幂等保证，为每个客户端唯一一个最后提交的uniqId。这里有个大前提，每个客户端的请求是串行的
	lastApplyUniqId map[int]int64
}

func (kv *KVServer) waitCommit(logIdx int) *chan raft.ApplyMsg {
	_, ok := kv.waitCh[logIdx]
	if !ok {
		kv.waitCh[logIdx] = make([]chan raft.ApplyMsg, 0, 10)
	}
	ch := make(chan raft.ApplyMsg)
	kv.waitCh[logIdx] = append(kv.waitCh[logIdx], ch)
	return &ch
}

func (kv *KVServer) notifyCommit(logIdx int, msg *raft.ApplyMsg) {
	kv.mu.Lock()
	_, ok := kv.waitCh[logIdx]
	if !ok {
		kv.mu.Unlock()
		return
	}
	//这里可能存在一个时序的问题，有可能在创建wait chan之前就已经commit成功，我们需要把之前
	//通知所有等待的线程
	for _, ch := range kv.waitCh[logIdx] {
		kv.mu.Unlock()
		ch <- *msg
		kv.mu.Lock()
	}
	//清理空间
	//delete(kv.waitCh, logIdx)
	kv.mu.Unlock()
}

func (kv *KVServer) getLastApplyUniqId(clientId int) int64 {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	_, ok := kv.lastApplyUniqId[clientId]
	if !ok {
		kv.lastApplyUniqId[clientId] = -1
	}
	return kv.lastApplyUniqId[clientId]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("Get request[start]...peerId:%d,args:%+v", kv.me, args)
	defer func() {
		DPrintf("Get request[finish]...peerId:%d,reply:%+v", kv.me, reply)
	}()
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	//为了保证线性一致性，这里需要增加一个相当于ReadLog的做法
	op := Op{GetCommand, args.Key, "", args.ClientId, args.UniqId}
	kv.mu.Lock()
	logIdx, _, success := kv.rf.Start(op)
	if !success {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("append log to raft...peerId:%d,args:%+v,logIdx:%d", kv.me, args, logIdx)
	//log.Printf("get wait[start]...peerId:%d,logIdx:%d", kv.me, logIdx)
	//等待过半数提交
	ch := kv.waitCommit(logIdx)
	kv.mu.Unlock()
	msg := <-*ch
	//log.Printf("get wait[finish]...peerId:%d,logIdx:%d", kv.me, logIdx)
	command := msg.Command.(Op)
	if command.ClientId != op.ClientId || command.UniqId != op.UniqId {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
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
	DPrintf("PutAppend request[start]...peerId:%d,args:%+v", kv.me, args)
	defer func() {
		DPrintf("PutAppend request[finish]...peerId:%d,reply:%+v", kv.me, reply)
	}()
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{args.Op, args.Key, args.Value, args.ClientId, args.UniqId}
	//注意，这里一定要加锁，不然可能会导致commit返回比下面的wait chan创建还快的场景
	kv.mu.Lock()
	logIdx, _, success := kv.rf.Start(op)
	if !success {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("append log to raft...peerId:%d,args:%+v,logIdx:%d", kv.me, args, logIdx)
	//等待过半数提交
	//log.Printf("putAppend wait[start]...peerId:%d,logIdx:%d", kv.me, logIdx)
	ch := kv.waitCommit(logIdx)
	kv.mu.Unlock()
	msg := <-*ch
	//log.Printf("putAppend wait[finish]...peerId:%d,logIdx:%d", kv.me, logIdx)
	command := msg.Command.(Op)
	//提交日志后了，重新发生选举，然后把当前的log删除
	if command.ClientId != op.ClientId || command.UniqId != op.UniqId {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	return
}

//更新状态机数据，需要保证时序，这里只能使用单线程
func (kv *KVServer) applyEntry() {
	for !kv.killed() {
		msg := <-kv.applyCh
		DPrintf("log commit...peerId:%d,msg:%+v,logIdx:%d", kv.me, msg, msg.CommandIndex)
		handleMsg := func(msg *raft.ApplyMsg) {
			kv.mu.Lock()
			start := time.Now().Second()
			//fmt.Printf("apply entry[start]...peerId:%d,msg:%+v,logIdx:%d\n", kv.me, msg, msg.CommandIndex)
			defer func() {
				end := time.Now().Second()
				if end-start > 1 {
					fmt.Printf("apply entry[finish]...peerId:%d,msg:%+v,logIdx:%d,cost:%d\n", kv.me, msg, msg.CommandIndex, end-start)
				}
				kv.mu.Unlock()
			}()
			if msg.CommandValid {
				op := msg.Command.(Op)
				switch op.Command {
				case PutCommand:
					//针对客户端请求的幂等处理
					lastId := kv.getLastApplyUniqId(op.ClientId)
					if op.UniqId > lastId {
						//DPrintf("put...peerId:%d,key:%s,values:%s", kv.me, op.Key, op.Value)
						kv.data[op.Key] = op.Value
						kv.lastApplyUniqId[op.ClientId] = op.UniqId
					}
				case AppendCommand:
					//针对客户端请求的幂等处理
					lastId := kv.getLastApplyUniqId(op.ClientId)
					if op.UniqId > lastId {
						//DPrintf("append...peerId:%d,key:%s,values:%s", kv.me, op.Key, op.Value)
						v, ok := kv.data[op.Key]
						if !ok {
							kv.data[op.Key] = op.Value
						} else {
							kv.data[op.Key] = v + op.Value
						}
						kv.lastApplyUniqId[op.ClientId] = op.UniqId
					}
				case GetCommand, NOOP:
					//no-op
				default:
					panic("unknown command" + op.Command)
				}
				kv.lastApply = msg.CommandIndex
				if kv.maxraftstate > 0 {
					if kv.rf.GetPersister().RaftStateSize() >= kv.maxraftstate {
						//fmt.Printf("开始生成快照...peerId:%d\n", kv.me)
						//计算快照
						snapshot := kv.encodeState()
						kv.rf.Snapshot(msg.CommandIndex, snapshot)
					}
				}
				kv.mu.Unlock()
				//通知所有等待线程
				kv.notifyCommit(msg.CommandIndex, msg)
				kv.mu.Lock()
			} else if msg.SnapshotValid {
				//follower落后太多的场景需要更新快照
				DPrintf("Installsnapshot %v\n", msg.SnapshotIndex)
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					//kv.lastApply = msg.CommandIndex
					//data解析
					kv.readPersist(msg.Snapshot)
				}
			} else {
				//leader change,发送no-op
				logIdx, _, _ := kv.rf.Start(Op{NOOP, "", "", 0, 0})
				DPrintf("写入NOOP...peerId:%d,logIdx:%d", kv.me, logIdx)
			}
		}
		handleMsg(&msg)
	}
}

func (kv *KVServer) encodeState() []byte {
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	encoder.Encode(kv.data)
	encoder.Encode(kv.lastApplyUniqId)
	encoder.Encode(kv.lastApply)
	return w.Bytes()
}

func (kv *KVServer) readPersist(snapshot []byte) {
	//snapshot := kv.rf.GetPersister().ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	r := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(r)
	if decoder.Decode(&kv.data) != nil ||
		decoder.Decode(&kv.lastApplyUniqId) != nil || decoder.Decode(&kv.lastApply) != nil {
		DPrintf("read persist fail...peerId:%d", kv.me)
	} else {
		DPrintf("read persist success...peerId:%d\n", kv.me)
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
	//kv.applyCh = make(chan raft.ApplyMsg, 10)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.waitCh = make(map[int][]chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApply = 0
	kv.lastApplyUniqId = make(map[int]int64)

	snapshot := kv.rf.GetPersister().ReadSnapshot()
	kv.readPersist(snapshot)
	go kv.applyEntry()
	// You may need initialization code here.

	return kv
}
