package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	//保存上一次的leader
	leader int
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
	args := GetArgs{key}
	reply := GetReply{}
	DPrintf("client get...key:%s", key)
	ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
	if ok && reply.Err == OK {
		return reply.Value
	}
	for {
		for peerId := range ck.servers {
			ok := ck.servers[peerId].Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == OK {
					ck.leader = peerId
					return reply.Value
				} else if reply.Err == ErrNoKey {
					ck.leader = peerId
					return ""
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
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
	args := PutAppendArgs{key, value, op}
	reply := PutAppendReply{}
	DPrintf("client putAppend...key:%s,value:%s,op:%s", key, value, op)
	ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
	if ok && reply.Err == OK {
		return
	}
	//由于服务端有可能都还没选举出leader，需要等待
	for {
		for peerId := range ck.servers {
			ok := ck.servers[peerId].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				ck.leader = peerId
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
