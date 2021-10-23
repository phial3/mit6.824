package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
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
	//NOOP的时候需要判断新的Leader是否为自己，不是的话需要唤醒等待的service线程
	Leader int
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
	//waitCh map[int][]chan raft.ApplyMsg
	//幂等保证，为每个客户端唯一一个最后提交的uniqId。这里有个大前提，每个客户端的请求是串行的
	lastApplyUniqId map[int]int64
	//等待raft提交，这里有一个问题，
	//假设发生网络分区的场景，长时间都提交不了是否会有问题？
	//假设刚提交的log被新选举的leader抹除和覆盖，如何唤醒正在等待的提交的线程
	replyChan map[int]chan *CommitReply
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
	// Your code here.
	DPrintf("Get request[start]...peerId:%d,args:%+v", kv.me, args)
	defer func() {
		DPrintf("Get request[finish]...peerId:%d,reply:%+v", kv.me, reply)
	}()
	//read log
	op := Op{GetCommand, args.Key, "", args.ClientId, args.UniqId, 0}
	commitReply := kv.appendToRaft(&op)
	if commitReply.WrongLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	value, exist := kv.data[args.Key]
	kv.mu.Unlock()
	if !exist {
		reply.Err = ErrNoKey
		reply.Value = ""
		return
	}
	reply.Err = OK
	reply.Value = value
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("PutAppend request[start]...peerId:%d,args:%+v", kv.me, args)
	defer func() {
		DPrintf("PutAppend request[finish]...peerId:%d,reply:%+v", kv.me, reply)
	}()
	op := Op{args.Op, args.Key, args.Value, args.ClientId, args.UniqId, 0}
	commitReply := kv.appendToRaft(&op)
	reply.Err = OK
	if commitReply.WrongLeader {
		reply.Err = ErrWrongLeader
	}
}

//表示当前的commit log是否由新的leader产生
type CommitReply struct {
	WrongLeader bool
}

//构建log并提交到raft
func (kv *KVServer) appendToRaft(op *Op) *CommitReply {
	kv.mu.Lock()
	//defer kv.mu.Unlock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.mu.Unlock()
		return &CommitReply{true}
	}
	logIdx, _, success := kv.rf.Start(*op)
	if !success {
		kv.mu.Unlock()
		return &CommitReply{true}
	}
	DPrintf("append to raft...peerId:%d,command:%+v,logIdx:%d", kv.me, op, logIdx)
	//等待过半数提交
	ch := make(chan *CommitReply)
	kv.replyChan[logIdx] = ch
	kv.mu.Unlock()
	reply := <-ch
	//kv.mu.Lock()
	//删除key，释放空间
	//delete(kv.replyChan, logIdx)
	return reply
}

//更新状态机数据，需要保证时序，这里只能使用单线程
func (kv *KVServer) applyEntry() {
	for !kv.killed() {
		msg := <-kv.applyCh
		DPrintf("log commit...peerId:%d,msg:%+v,logIdx:%d", kv.me, msg, msg.CommandIndex)
		handleMsg := func(msg *raft.ApplyMsg) {
			kv.mu.Lock()
			//fmt.Printf("apply entry[start]...peerId:%d,msg:%+v,logIdx:%d\n", kv.me, msg, msg.CommandIndex)
			defer func() {
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
				case GetCommand:
				//do nothing
				case NOOP:
					//唤醒所有等待的线程,后面提交到raft的log都认为失败
					if op.Leader != kv.me && len(kv.replyChan) > 0 {
						DPrintf("唤醒等待提交的线程...peerId:%d", kv.me)
						/*wakeup := make([]chan *CommitReply, len(kv.replyChan))
						idx := 0
						for _, ch := range kv.replyChan {
							wakeup[idx] = ch
							idx++
						}*/
						//最好不要这么写，在使用go chan的时候如果加锁很容易会导致死锁
						for idx, ch := range kv.replyChan {
							ch <- &CommitReply{true}
							close(ch)
							delete(kv.replyChan, idx)
						}
						/*kv.mu.Unlock()
						for _, ch := range wakeup {
							ch <- &CommitReply{true}
						}*/
						//kv.mu.Lock()
						return
					}
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
				//通知所有等待线程
				if ch, exist := kv.replyChan[msg.CommandIndex]; exist {
					//kv.mu.Unlock()
					ch <- &CommitReply{false}
					close(ch)
					delete(kv.replyChan, msg.CommandIndex)
					//kv.mu.Lock()
				}
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
				logIdx, _, _ := kv.rf.Start(Op{NOOP, "", "", 0, 0, kv.me})
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
	//kv.waitCh = make(map[int][]chan raft.ApplyMsg)
	kv.replyChan = make(map[int]chan *CommitReply)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApply = 0
	kv.lastApplyUniqId = make(map[int]int64)

	snapshot := kv.rf.GetPersister().ReadSnapshot()
	kv.readPersist(snapshot)
	go kv.applyEntry()
	// You may need initialization code here.

	return kv
}
