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
	"fmt"
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
	nextIndex   []int
	matchIndex  []int
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
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGrant = false
		reply.Term = rf.currentTerm
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
	} else {
		reply.VoteGrant = false
		reply.Term = args.Term
	}
	fmt.Printf("receive vote...serverId:%d,candidate:%d,currentTerm:%d,Term:%d,reply:%t\n", rf.me, args.CandidateId, rf.currentTerm, args.Term, reply.VoteGrant)
}

//重新设置超时选举时钟
func (rf *Raft) resetElectionTimeout() {
	//随机超时时间150~300
	rand := MinElectionTimeout + rand.Int63n(MaxElectionTimeout-MinElectionTimeout)
	rf.electionTimeout = time.Now().UnixNano()/1e6 + rand
}

type LogEntry struct {
	term    int
	command interface{}
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
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	//更新本地日志
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
			return
		}
	}
	for _, entry := range args.Entries {
		rf.log = append(rf.log, entry)
	}
	//更新当前任期并更新过期时间
	rf.role = Follower
	rf.currentTerm = args.Term
	rf.resetElectionTimeout()
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
	fmt.Printf("request[start]...peerid:%d\n", rf.me)
	// Your code here (2B).
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	//先写入本地
	entry := LogEntry{term: rf.currentTerm, command: command}
	rf.log = append(rf.log, entry)
	logLen := len(rf.log)
	rf.mu.Unlock()
	//同步到其他副本，根据每个副本的next index进行同步，这是由于每个副本的进度都有可能不相同
	replyChan := make(chan bool, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peerId int, logLen int) {
			args := rf.makeAppendEntryArgs(peerId)
			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				replyChan <- false
				return
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
				if reply.Success {
					rf.mu.Lock()
					rf.nextIndex[peerId]++
					rf.matchIndex[peerId]++
					rf.mu.Unlock()
				} else {
					//nextIndex回退一步
					rf.mu.Lock()
					rf.nextIndex[peerId]--
					rf.matchIndex[peerId]--
					rf.mu.Unlock()
				}
			}
			replyChan <- true
		}(i, logLen)
	}
	cnt := 1
	for i := 1; i < len(rf.peers); i++ {
		res := <-replyChan
		if res {
			cnt++
		}
		if cnt > len(rf.peers)/2 {
			break
		}
	}
	//结果处理
	return func() (int, int, bool) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != Leader {
			return -1, -1, false
		}
		return logLen - 1, entry.term, true
	}()
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

	//lab2B
	rf.log = make([]LogEntry, 0, LogInitSize)
	rf.commitIndex = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = -1
	}
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
	fmt.Printf("raft start...server:%d\n", me)
	return rf
}

func (rf *Raft) makeAppendEntryArgs(peerId int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	prevLogIndex := rf.matchIndex[peerId]
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = rf.log[prevLogIndex].term
	}
	var entries []LogEntry
	if prevLogIndex+1 < len(rf.log) {
		entries = rf.log[prevLogIndex+1:]
	}
	arg := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: rf.commitIndex}
	return arg
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	now := time.Now().UnixNano() / 1e6
	if rf.role != Leader || now >= rf.heartbeatTimeout {
		return
	}
	//更新下一次发送心跳时间
	rf.heartbeatTimeout = now + HeartbeatInterval
	fmt.Printf("send heatbeat[start]...serverid:%d,term:%d\n", rf.me, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		//自己不需要发送心跳
		if i != rf.me {
			go func(peerId int) {
				//rpc的过程不能加锁
				//rf.mu.Lock()
				//defer rf.mu.Unlock()
				reply := AppendEntriesReply{}
				args := rf.makeAppendEntryArgs(peerId)
				ok := rf.sendAppendEntries(peerId, &args, &reply)
				rf.mu.Lock()
				if ok && reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					rf.role = Follower
					rf.resetElectionTimeout()
				}
				rf.mu.Unlock()
			}(i)
		}
	}
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
		fmt.Printf("heartbeat timeout...server:%d,role:%d,term:%d\n", rf.me, rf.role, rf.currentTerm)
		rf.role = Candidate
		rf.currentTerm++
		//给自己投票并更新心跳超时时钟
		rf.voteFor = rf.me
		rf.resetElectionTimeout()
		//fmt.Printf("sendRequest vote...server:%d,term:%d\n", rf.me, rf.currentTerm)
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
	fmt.Printf("request vote[start] ...id:%d,role:%d,term:%d\n", rf.me, rf.role, rf.currentTerm)
	//包含自己的票数
	//只需要等待过半的票数，不然一个结点的故障会导致整个投票过程超时
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			fmt.Printf("send request vote...candidateId:%d,to:%d,term:%d\n", rf.me, i, args.Term)
			resp := rf.sendRequestVote(i, &args, &reply)
			fmt.Printf("get resp...candidateId:%d,to:%d,resp:%t\n", rf.me, i, reply.VoteGrant)
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
		fmt.Printf("wait for vote...peerId:%d,i:%d\n", rf.me, i)
		select {
		case res := <-vote:
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				fmt.Printf("get vote\n")
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
			fmt.Printf("request vote[finish]...serverId:%d,term:%d,vote:%d,maxTerm:%d\n", rf.me, rf.currentTerm, cnt, maxTerm)
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
			fmt.Printf("become leader...server:%d,term:%d\n", rf.me, rf.currentTerm)
		}
	}()
}
