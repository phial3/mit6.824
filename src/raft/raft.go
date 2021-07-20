package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//角色
const (
	Follower = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	//角色
	role int
	//选举定时器
	electionTimeout int64
	//心跳定时器(leader才会使用这个定时器)
	heartbeatTimeout int64

	//log
	log         []LogEntry
	commitIndex int
	//关于nextIndex和matchIndex的说明，这个问题也困扰了我好久，为什么不只保留一个nextIndex/matchIndex。不是必须，但是从理解角度会更直观
	//https://stackoverflow.com/questions/46376293/what-is-lastapplied-and-matchindex-in-raft-protocol-for-volatile-state-in-server
	nextIndex  []int
	matchIndex []int
	//lab2b测试需要
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term      int
	VoteGrant bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		DPrintf("receive vote...serverId:%d,candidate:%d,currentTerm:%d,Term:%d,reply:%t\n", rf.me, args.CandidateId, rf.currentTerm, args.Term, reply.VoteGrant)
		rf.mu.Unlock()
	}()
	reply.VoteGrant = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	//检查日志是否更新
	//Raft determines which of two logs is more up-to-date
	//by comparing the index and term of the last entries in the
	//logs. If the logs have last entries with different terms, then
	//the log with the later term is more up-to-date. If the logs
	//end with the same term, then whichever log is longer is
	//more up-to-date.
	lastLogIdx := len(rf.log) - 1
	if args.LastLogTerm < rf.log[lastLogIdx].Term || (args.LastLogTerm == rf.log[lastLogIdx].Term && args.LastLogIndex < lastLogIdx) {
		DPrintf("candidate log term is older...arg.lastLogTerm:%d,current lastLogTerm:%d,args.LastLogIndex:%d,lastLogIdx:%d", args.LastLogTerm, rf.log[lastLogIdx].Term, args.LastLogIndex, lastLogIdx)
		return
	}
	if args.Term > rf.currentTerm {
		//更新任期等信息
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	//只有当当前任期没有投票才能投
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		rf.resetElectionTimeout()
		rf.role = Follower
		rf.voteFor = args.CandidateId

		reply.VoteGrant = true
		reply.Term = args.Term
	}
}

//重新设置超时选举时钟
func (rf *Raft) resetElectionTimeout() {
	//随机超时时间150~300
	rand := MinElectionTimeout + rand.Int63n(MaxElectionTimeout-MinElectionTimeout)
	rf.electionTimeout = time.Now().UnixNano()/1e6 + rand
}

type LogEntry struct {
	Term    int
	Command interface{}
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//接收appendEntry请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("append entry receive[start]...peerId:%d,args:%+v\n", rf.me, *args)
	reply.Term = rf.currentTerm
	reply.Success = false
	defer func() {
		DPrintf("append entry receive[finish]...peerId:%d,success:%t,logLen:%d,commitIdx:%d\n", rf.me, reply.Success, len(rf.log), rf.commitIndex)
		rf.mu.Unlock()
	}()
	if args.Term < rf.currentTerm {
		DPrintf("term not match...arg.term:%d,currentTerm:%d", args.Term, rf.currentTerm)
		return
	}
	//更新当前任期并更新过期时间，这里需要前置。看figure 2的描述。不然当日志不匹配的时候还是会发起一次新的选举
	//All Servers:
	//• If commitIndex > lastApplied: increment lastApplied, apply
	//log[lastApplied] to state machine (§5.3)
	//• If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (§5.1)
	rf.role = Follower
	rf.currentTerm = args.Term
	rf.resetElectionTimeout()
	DPrintf("turn to follower...peerId:%d", rf.me)
	//日志校验
	if args.PrevLogIndex >= 0 && (args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		DPrintf("log not match...peerId:%d\n", rf.me)
		return
	}
	//更新/覆盖本地日志，不能直接删除后面的日志
	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
	//and truncating the log would mean “taking back” entries that we may have already told the leader that we have in our log.
	idx := args.PrevLogIndex + 1
	for _, entry := range args.Entries {
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[0:idx]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
		idx++
	}
	//更新commitIndex，这里需要判断下是不是来源于一个旧的请求
	if args.LeaderCommit > rf.commitIndex {
		for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
			applyMsg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
			rf.applyCh <- applyMsg
			DPrintf("log commit...peerId:%d,index:%d", rf.me, i)
		}
		rf.commitIndex = args.LeaderCommit
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type AppendEntryResult struct {
	peerId int
	reply  *AppendEntriesReply
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("request[start]...peerId:%d,command:%d\n", rf.me, command)
	logIdx := -1
	logTerm := -1
	success := false
	defer func() {
		rf.mu.Lock()
		DPrintf("request[finish]...peerId:%d,logLen:%d,commitIdx:%d,success:%t\n", rf.me, len(rf.log), rf.commitIndex, success)
		rf.mu.Unlock()
	}()
	// Your code here (2B).
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return logIdx, logTerm, success
	}
	//先写入本地
	entry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	logLen := len(rf.log)
	//更新logIdx和logTem
	logIdx = logLen - 1
	logTerm = rf.currentTerm
	success = true
	rf.mu.Unlock()
	go rf.broadcastEntry()
	//这里不需要等过半数结点提交成功？为什么，那么客户端如何知道这个数据已经成功复制到过半数结点了?
	return logIdx, logTerm, success
}

//考虑到并发，在并行发送前我们先确定当前的log index作为发送的最后的index。这样可以更简单的计算复制成功的index的数量。
//发送[nextIndex,]
func (rf *Raft) broadcastEntry() {
	defer func() {
		rf.mu.Lock()
		DPrintf("broadcastEntry[finish]...peerId:%d,logLen:%d,commitIndex:%d", rf.me, len(rf.log), rf.commitIndex)
		rf.mu.Unlock()
	}()
	rf.mu.Lock()
	//统一在这里生成请求参数，保证每个请求的lastIdx和term在请求过程中不会被修改，起到一个类似快照的作用
	lastIdx := len(rf.log) - 1
	argArr := make([]AppendEntriesArgs, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		argArr[i] = rf.makeAppendEntryArgs(i)
	}
	rf.mu.Unlock()
	//同步到其他副本，根据每个副本的next index进行同步，这是由于每个副本的进度都有可能不相同
	appendChan := make(chan AppendEntryResult, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peerId int) {
			//args := rf.makeAppendEntryArgs(peerId, lastIdx)
			args := argArr[peerId]
			reply := AppendEntriesReply{}
			defer func() {
				appendChan <- AppendEntryResult{peerId: peerId, reply: &reply}
			}()
			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
				if reply.Success {
					rf.mu.Lock()
					rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						//变为follow，重新选举
						rf.role = Follower
						rf.currentTerm = args.Term
						rf.resetElectionTimeout()
					} else {
						//nextIndex回退一步
						rf.nextIndex[peerId]--
					}
					rf.mu.Unlock()
				}
			}
		}(i)
	}
	//结果处理，这一段要好好理解一下
	//• If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	//set commitIndex = N (§5.3, §5.4)

	//复制成功的副本数量
	replica := 1
	for i := 1; i < len(rf.peers); i++ {
		res := <-appendChan
		if res.reply != nil && res.reply.Success {
			replica++
			if replica > len(rf.peers)/2 {
				rf.mu.Lock()
				//并发的场景，有可能这时候的commit index已经被修改
				if lastIdx > rf.commitIndex {
					//通知cfg
					for i := rf.commitIndex + 1; i <= lastIdx; i++ {
						applyMsg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
						rf.applyCh <- applyMsg
						DPrintf("log commit...peerId:%d,index:%d", rf.me, i)
					}
					rf.commitIndex = lastIdx
				}
				rf.mu.Unlock()
				return
			}
		}
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.leaderElection()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

//心跳时间间隔
const HeartbeatInterval = 100

//选举超时随机范围
const MinElectionTimeout = 150
const MaxElectionTimeout = 300

const LogInitSize = 1000

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.role = Follower
	rf.currentTerm = 0
	rf.resetElectionTimeout()
	rf.voteFor = -1
	//测试用例
	rf.applyCh = applyCh

	//lab2B，这里有个关键点，log的序号是从1开始的，所以这里要填一个0来补
	rf.log = make([]LogEntry, 0, LogInitSize)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: 0})
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.initNextIndex()
	rf.mu.Unlock()
	go func() {
		for !rf.killed() {
			rf.sendHeartbeat()
			time.Sleep(10 * time.Millisecond)
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	DPrintf("raft start...server:%d\n", me)
	return rf
}

//nextIndex[] for each server, index of the next log entry
//to send to that server (initialized to leader
//last log index + 1)
//matchIndex[] for each server, index of highest log entry
//known to be replicated on server
//(initialized to 0, increases monotonically)
func (rf *Raft) initNextIndex() {
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
}

func (rf *Raft) makeAppendEntryArgs(peerId int) AppendEntriesArgs {
	nextIdx := rf.nextIndex[peerId]
	//下标从1开始的好处来了
	prevLogIndex := nextIdx - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	arg := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm, Entries: rf.log[nextIdx:], LeaderCommit: rf.commitIndex}
	DPrintf("build appendEntry...peerId:%d,leaderId:%d,prevLogIndex:%d,commitIndex:%d\n", peerId, rf.me, prevLogIndex, rf.commitIndex)
	return arg
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	now := time.Now().UnixNano() / 1e6
	if rf.role != Leader || now < rf.heartbeatTimeout {
		rf.mu.Unlock()
		return
	}
	//更新下一次发送心跳时间
	rf.heartbeatTimeout = now + HeartbeatInterval
	DPrintf("send heartbeat[start]...peerId:%d,term:%d\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	//这里异步执行效率会更高，不然可能会影响到下一次心跳
	go rf.broadcastEntry()
}

func (rf *Raft) leaderElection() {
	//注意这里不能加全局锁，依赖rpc的地方加锁会导致锁的粒度非常大，性能急剧下降
	voteChan := make(chan VoteResult, len(rf.peers))
	now := time.Now().UnixNano() / 1e6
	rf.mu.Lock()
	if rf.role == Leader || now <= rf.electionTimeout {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("start election...server:%d,role:%d,term:%d\n", rf.me, rf.role, rf.currentTerm)
		rf.role = Candidate
		rf.currentTerm++
		//给自己投票并更新心跳超时时钟
		rf.voteFor = rf.me
		rf.resetElectionTimeout()
		//发送选举给其他服务器
		rf.requestVote(voteChan)
	}()
	//等待rpc请求的地方不能加锁
	rf.receiveVote(voteChan)
}

type VoteResult struct {
	peerId int
	reply  *RequestVoteReply
}

func (rf *Raft) requestVote(voteChan chan VoteResult) {
	DPrintf("request vote[start] ...id:%d,role:%d,term:%d\n", rf.me, rf.role, rf.currentTerm)
	//包含自己的票数
	//只需要等待过半的票数，不然一个结点的故障会导致整个投票过程超时
	lastLogIdx := len(rf.log) - 1
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIdx, LastLogTerm: rf.log[lastLogIdx].Term}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			DPrintf("send request vote...candidateId:%d,to:%d,term:%d\n", rf.me, i, args.Term)
			resp := rf.sendRequestVote(i, &args, &reply)
			DPrintf("get resp...candidateId:%d,to:%d,resp:%t\n", rf.me, i, reply.VoteGrant)
			if resp {
				voteChan <- VoteResult{peerId: i, reply: &reply}
			} else {
				voteChan <- VoteResult{peerId: i, reply: nil}
			}
		}(i)
	}
}

func (rf *Raft) receiveVote(vote chan VoteResult) {
	//算上自己本身的一票
	cnt := 1
	rf.mu.Lock()
	maxTerm := rf.currentTerm
	rf.mu.Unlock()
	for i := 1; i < len(rf.peers); i++ {
		DPrintf("wait for vote...peerId:%d,i:%d\n", rf.me, i)
		select {
		case res := <-vote:
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("get vote\n")
				if res.reply != nil {
					if res.reply.VoteGrant {
						cnt++
					} else if res.reply.Term > maxTerm {
						maxTerm = res.reply.Term
					}
				}
			}()
		}
		//提前结束
		if cnt > len(rf.peers)/2 {
			break
		}
	}
	func() {
		rf.mu.Lock()
		defer func() {
			DPrintf("request vote[finish]...serverId:%d,term:%d,vote:%d,maxTerm:%d\n", rf.me, rf.currentTerm, cnt, maxTerm)
			rf.mu.Unlock()
		}()
		//如果角色发生了变化，则忽略投票结果,有可能收到一个更高任期的心跳
		if rf.role != Candidate {
			return
		}
		//被投票的term还要更高，主动降为follow，加速整个投票的过程
		if maxTerm > rf.currentTerm {
			rf.role = Follower
			rf.currentTerm = maxTerm
			rf.voteFor = -1
			return
		}
		if cnt > len(rf.peers)/2 {
			rf.role = Leader
			//更新心跳时间，尽快触发发送心跳
			rf.heartbeatTimeout = time.Now().UnixNano() / 1e6
			//更新nextIndex
			rf.initNextIndex()
			DPrintf("become leader...server:%d,term:%d\n", rf.me, rf.currentTerm)
		}
	}()
}
