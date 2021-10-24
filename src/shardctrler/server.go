package shardctrler

import (
	"6.824/raft"
	"log"
	"sync/atomic"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num
	//等待raft提交，这里有一个问题，
	//假设发生网络分区的场景，长时间都提交不了是否会有问题？
	//假设刚提交的log被新选举的leader抹除和覆盖，如何唤醒正在等待的提交的线程
	replyChan map[int]chan *CommitReply
	dead      int32 // set by Kill()
}

//表示当前的commit log是否由新的leader产生
type CommitReply struct {
	WrongLeader bool
}

const (
	JoinCommand  = "Join"
	LeaveCommand = "Leave"
	MoveCommand  = "Move"
	QueryCommand = "Query"
	NOOP         = "NOOP"
)

type Op struct {
	// Your data here.
	CmdType string
	Args    interface{}
	//NOOP的时候需要判断新的Leader是否为自己，不是的话需要唤醒等待的service线程
	Leader int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{JoinCommand, *args, 0}
	commitReply := sc.appendToRaft(&op)
	if commitReply.WrongLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{LeaveCommand, *args, 0}
	commitReply := sc.appendToRaft(&op)
	if commitReply.WrongLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{MoveCommand, *args, 0}
	commitReply := sc.appendToRaft(&op)
	if commitReply.WrongLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{QueryCommand, *args, 0}
	commitReply := sc.appendToRaft(&op)
	if commitReply.WrongLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Lock()
		if args.Num < 0 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
	}
}

//构建log并提交到raft
func (sc *ShardCtrler) appendToRaft(op *Op) *CommitReply {
	sc.mu.Lock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		sc.mu.Unlock()
		return &CommitReply{true}
	}
	logIdx, _, success := sc.rf.Start(*op)
	if !success {
		sc.mu.Unlock()
		return &CommitReply{true}
	}
	//等待过半数提交
	ch := make(chan *CommitReply)
	sc.replyChan[logIdx] = ch
	sc.mu.Unlock()
	reply := <-ch
	return reply
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	// Your code here.
	sc.dead = 0
	sc.replyChan = make(map[int]chan *CommitReply)
	go sc.applyEntry()
	return sc
}

//更新状态机数据，需要保证时序，这里只能使用单线程
func (sc *ShardCtrler) applyEntry() {
	for !sc.killed() {
		msg := <-sc.applyCh
		DPrintf("log commit...peerId:%d,msg:%+v,logIdx:%d", sc.me, msg, msg.CommandIndex)
		handleMsg := func(msg *raft.ApplyMsg) {
			sc.mu.Lock()
			defer func() {
				sc.mu.Unlock()
			}()
			if msg.CommandValid {
				op := msg.Command.(Op)
				switch op.CmdType {
				case JoinCommand:
					args := op.Args.(JoinArgs)
					last := sc.configs[len(sc.configs)-1]
					new := last.Clone()
					new.Num = len(sc.configs)
					for gid, servers := range args.Servers {
						new.Groups[gid] = servers
					}
					//重新分配
					new.Balance()
					sc.configs = append(sc.configs, *new)
				case LeaveCommand:
					args := op.Args.(LeaveArgs)
					last := sc.configs[len(sc.configs)-1]
					new := last.Clone()
					new.Num = len(sc.configs)
					for _, gid := range args.GIDs {
						delete(new.Groups, gid)
					}
					//重新分配
					new.Balance()
					sc.configs = append(sc.configs, *new)
				case MoveCommand:
					args := op.Args.(MoveArgs)
					last := sc.configs[len(sc.configs)-1]
					new := last.Clone()
					new.Num = len(sc.configs)
					new.Shards[args.Shard] = args.GID
					sc.configs = append(sc.configs, *new)
				case QueryCommand:
					//do nothing
				case NOOP:
					//唤醒所有等待的线程,后面提交到raft的log都认为失败
					if op.Leader != sc.me {
						for idx, ch := range sc.replyChan {
							ch <- &CommitReply{true}
							close(ch)
							delete(sc.replyChan, idx)
						}
						return
					}
					//no-op
				default:
					panic("unknown command" + op.CmdType)
				}
				//唤醒等待线程
				if ch, ok := sc.replyChan[msg.CommandIndex]; ok {
					ch <- &CommitReply{false}
					close(ch)
					delete(sc.replyChan, msg.CommandIndex)
				}
			} else {
				//leader change,发送no-op
				logIdx, _, _ := sc.rf.Start(Op{NOOP, nil, sc.me})
				DPrintf("写入NOOP...peerId:%d,logIdx:%d", sc.me, logIdx)
			}
		}
		handleMsg(&msg)
	}
}
