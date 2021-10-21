package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

var nextClientId = 1

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	//保存上一次的leader，优先对上一次leader发起请求
	leader int
	//说白了就是为了保证幂等，但是这里的幂等实现是不严谨的，服务端的幂等实现仅仅保存上一次最后的uniqId，能这么用是基于下面说的场景
	//Your scheme for duplicate detection should free server memory quickly,
	//for example by having each RPC imply that the client has seen the reply for its previous RPC.
	//It's OK to assume that a client will make only one call into a Clerk at a time.
	clientId   int
	lastUniqId int64
}

func (ck *Clerk) genUniqId() int64 {
	ck.lastUniqId++
	return ck.lastUniqId
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.clientId = nextClientId
	nextClientId++
	ck.lastUniqId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	uniqId := ck.genUniqId()
	args := GetArgs{key, ck.clientId, uniqId}
	reply := GetReply{}
	ck.sendReq(&args, &reply, "KVServer.Get", func(reply interface{}) bool {
		r := reply.(*GetReply)
		return r.Err == OK || r.Err == ErrNoKey
	})
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	uniqId := ck.genUniqId()
	args := PutAppendArgs{key, value, op, ck.clientId, uniqId}
	reply := PutAppendReply{}
	ck.sendReq(&args, &reply, "KVServer.PutAppend", func(reply interface{}) bool {
		r := reply.(*PutAppendReply)
		return r.Err == OK
	})
}

func (ck *Clerk) sendReq(args interface{}, reply interface{}, method string, handleReply func(reply interface{}) bool) {
	DPrintf("client request...args:%+v", args)
	//这种写法其实是不严谨的，but实验要求这么写，先简单实现。
	for {
		//优先尝试上一次成功的server
		for i := 0; i < len(ck.servers); i++ {
			peerId := (ck.leader + i) % len(ck.servers)
			ok := ck.servers[peerId].Call(method, args, reply)
			if ok && handleReply(reply) {
				ck.leader = peerId
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
