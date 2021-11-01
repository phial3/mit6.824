package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

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

//表示当前的commit log是否由新的leader产生
type CommitReply struct {
	WrongLeader bool
}

type DataBackup struct {
	Data map[string]string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// Your definitions here.
	sm *shardctrler.Clerk
	//这个配置的同步有两种做法。1.raft group的每个节点直接通过client同步最新的配置,2.只通过leader同步配置，然后通过raft log同步到其他节点
	//只考虑使用2的方法，需要保证线性一致性(保证config生效的时序性)
	config *shardctrler.Config
	dead   int32 // set by Kill()
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

	//需要拉去的分区,key-分片ID,value-servers
	comeInShards map[int][]string
	//分片的backup，当你不再维护这个分片，需要把这个分片的数据备份起来给其他group进行拉取
	//If one of your RPC handlers includes in its reply a map (e.g. a key/value map) that's part of your server's state,
	//you may get bugs due to races. The RPC system has to read the map in order to send it to the caller,
	//but it isn't holding a lock that covers the map. Your server, however, may proceed to modify the same map while the RPC system is reading it.
	//The solution is for the RPC handler to include a copy of the map in the reply.
	//key-configNum,shard
	outShards map[int]map[int]DataBackup
	//判断那些分片能正常提供服务
	validShards [shardctrler.NShards]bool
}

func (kv *ShardKV) getLastApplyUniqId(clientId int) int64 {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	_, ok := kv.lastApplyUniqId[clientId]
	if !ok {
		kv.lastApplyUniqId[clientId] = -1
	}
	return kv.lastApplyUniqId[clientId]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// Your code here.
	DPrintf("Get request[start]...peerId:%d,args:%+v", kv.me, args)
	defer func() {
		DPrintf("Get request[finish]...peerId:%d,reply:%+v", kv.me, reply)
	}()
	//read log
	op := Op{GetCommand, args.Key, "", args.ClientId, args.UniqId, 0}
	err := kv.appendToRaft(&op)
	reply.Err = err
	if err != OK {
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Your code here.
	DPrintf("PutAppend request[start]...peerId:%d,args:%+v", kv.me, args)
	defer func() {
		DPrintf("PutAppend request[finish]...peerId:%d,reply:%+v", kv.me, reply)
	}()
	op := Op{args.Op, args.Key, args.Value, args.ClientId, args.UniqId, 0}
	err := kv.appendToRaft(&op)
	reply.Err = err
}

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	DPrintf("PullShard request[start]...peerId:%d,args:%+v", kv.me, args)
	kv.mu.Lock()
	defer func() {
		kv.mu.Unlock()
		DPrintf("PullShard request[finish]...peerId:%d,reply:%+v", kv.me, reply)
	}()
	if args.ConfigNum > kv.config.Num {
		reply.Err = ConfigOutDate
		return
	}
	backup := kv.outShards[args.ConfigNum][args.Shard]
	reply.Err = OK
	reply.Shard = args.Shard
	reply.Data = backup.Data
}

//构建log并提交到raft
func (kv *ShardKV) appendToRaft(op *Op) Err {
	kv.mu.Lock()
	//分区判断
	shard := key2shard(op.Key)
	if valid := kv.validShards[shard]; !valid {
		kv.mu.Unlock()
		return ErrWrongGroup
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	logIdx, _, success := kv.rf.Start(*op)
	if !success {
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	DPrintf("append to raft...peerId:%d,command:%+v,logIdx:%d", kv.me, op, logIdx)
	//等待过半数提交
	ch := make(chan *CommitReply)
	kv.replyChan[logIdx] = ch
	kv.mu.Unlock()
	reply := <-ch
	if reply.WrongLeader {
		return ErrWrongLeader
	} else {
		return OK
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PullShardArgs{})
	labgob.Register(PullShardReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.config = &shardctrler.Config{Num: 0, Groups: map[int][]string{}}

	kv.dead = 0
	kv.data = make(map[string]string)
	kv.replyChan = make(map[int]chan *CommitReply)
	kv.lastApply = 0
	kv.lastApplyUniqId = make(map[int]int64)
	kv.outShards = make(map[int]map[int]DataBackup)
	kv.comeInShards = make(map[int][]string)
	snapshot := kv.rf.GetPersister().ReadSnapshot()
	kv.readPersist(snapshot)
	go kv.applyEntry()
	//定时加载controller配置
	go kv.loadConfigLoop()
	//定时拉取分片数据
	go kv.pullShardLoop()
	return kv
}

//更新状态机数据，需要保证时序，这里只能使用单线程
func (kv *ShardKV) applyEntry() {
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
				//分片配置更新
				if config, ok := msg.Command.(shardctrler.Config); ok {
					if config.Num > kv.config.Num {
						for shard, gid := range config.Shards {
							curGid := kv.config.Shards[shard]
							if curGid == gid {
								//not change
								continue
							} else if curGid == kv.gid {
								//分片删除
								kv.validShards[shard] = false
								backup := DataBackup{make(map[string]string)}
								for _, key := range kv.data {
									if key2shard(key) == shard {
										backup.Data[key] = kv.data[key]
										delete(kv.data, key)
									}
								}
								if _, exist := kv.outShards[kv.config.Num]; !exist {
									kv.outShards[kv.config.Num] = make(map[int]DataBackup)
								}
								kv.outShards[kv.config.Num][shard] = backup
							} else if gid == kv.me {
								//分片增加
								kv.comeInShards[shard] = config.Groups[gid]
							}
						}
						//更新config
						kv.config = &config
					}
				} else if reply, ok := msg.Command.(PullShardReply); ok {
					for _, key := range reply.Data {
						kv.data[key] = reply.Data[key]
					}
					delete(kv.comeInShards, reply.Shard)
					kv.validShards[reply.Shard] = true
				} else if op, ok := msg.Command.(Op); ok {
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

func (kv *ShardKV) loadConfigLoop() {
	for !kv.killed() {
		loadConfig := func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			//上一次的分片还没同步完，需要继续同步
			if _, leader := kv.rf.GetState(); !leader || len(kv.comeInShards) > 0 {
				return
			}
			//直接拉最新配置，中间的配置变更跳过OK？
			//需要一个个版本拉取，因为我们需要对每次配置变更后不属于我们的shard都进行backup(为了让其他group过来拉)
			//如果跳过某些版本，过来拉取的group就获取不到对应的shard
			cur := kv.config.Num
			kv.mu.Unlock()
			config := kv.sm.Query(cur + 1)
			kv.mu.Lock()
			if config.Num != cur {
				kv.rf.Start(config)
			}
		}
		loadConfig()
		time.Sleep(100 * time.Millisecond)
	}
}

//定时拉取分片数据
func (kv *ShardKV) pullShardLoop() {
	for !kv.killed() {
		pullShard := func() {
			kv.mu.Lock()
			if len(kv.comeInShards) == 0 {
				kv.mu.Unlock()
				return
			}
			var wg sync.WaitGroup
			for shard, servers := range kv.comeInShards {
				wg.Add(1)
				//注意这里一定是拿上一个版本的backup
				go func(shard int, configNum int, servers []string) {
					defer wg.Done()
					for _, server := range servers {
						client := kv.make_end(server)
						args := PullShardArgs{shard, configNum}
						reply := PullShardReply{}
						if ok := client.Call("PULL.SHARD", &args, &reply); ok {
							if ok && reply.Err == OK {
								//更新本地配置，这里不能直接更新，需要提交到raft来保证所有节点都能够同步到
								kv.mu.Lock()
								kv.rf.Start(reply)
								/*for _, key := range reply.data {
									kv.data[key] = reply.data[key]
								}
								delete(kv.comeInShards, shard)
								kv.validShards[shard] = true*/
								kv.mu.Unlock()
								break
							}
						}
					}
				}(shard, kv.config.Num-1, servers)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		pullShard()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) encodeState() []byte {
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	encoder.Encode(kv.data)
	encoder.Encode(kv.lastApplyUniqId)
	encoder.Encode(kv.lastApply)
	return w.Bytes()
}

func (kv *ShardKV) readPersist(snapshot []byte) {
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
