package shardctrler

import "sort"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (config *Config) Clone() *Config {
	shards := [NShards]int{}
	for i := 0; i < NShards; i++ {
		shards[i] = config.Shards[i]
	}
	groups := make(map[int][]string)
	for k, v := range config.Groups {
		groups[k] = v
	}
	return &Config{config.Num, shards, groups}
}

//一致性哈希应该是最优解，这里先采用最粗暴的取模的方式
func (config *Config) Balance() {
	gids := make([]int, len(config.Groups))
	idx := 0
	for gid := range config.Groups {
		gids[idx] = gid
		idx++
	}
	//需要根据gid做一次排序，不然不同机器可能会得到不同的结果
	sort.Ints(gids)
	if len(config.Groups) > 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = gids[i%len(config.Groups)]
		}
	}
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
