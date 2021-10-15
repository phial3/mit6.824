package shardctrler

import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

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
	op := Op{JoinCommand, args, 0}
	commitReply := sc.appendToRaft(&op)
	if commitReply.WrongLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

//构建log并提交到raft
func (sc *ShardCtrler) appendToRaft(op *Op) *CommitReply {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		return &CommitReply{true}
	}
	logIdx, _, success := sc.rf.Start(op)
	if !success {
		return &CommitReply{true}
	}
	//等待过半数提交
	ch := make(chan *CommitReply)
	sc.replyChan[logIdx] = ch
	sc.mu.Unlock()
	reply := <-ch
	sc.mu.Lock()
	//删除key，释放空间
	delete(sc.replyChan, logIdx)
	return reply
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{LeaveCommand, args, 0}
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
	op := Op{MoveCommand, args, 0}
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
	op := Op{QueryCommand, args, 0}
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

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
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
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	return sc
}
