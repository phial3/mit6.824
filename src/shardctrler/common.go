package shardctrler

import (
	"sort"
)

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

//一致性哈希，只是这里的slot只有10
//然而一致性哈希其实不满足需求，本文要求的是尽可能地平均，并且移动最少。由于分片数是确定的，因此我们实际上有更好的做法
func (config *Config) ConsistentHashBalance() {
	if len(config.Groups) == 0 {
		return
	}
	//对group做一次hash
	mp := make(map[int]int)
	for gid := range config.Groups {
		mp[gid%NShards] = gid
	}
	for i := 0; i < NShards; i++ {
		//找到右边的第一个group
		for j := 0; j < NShards; j++ {
			slot := (i + j) % NShards
			if gid, exist := mp[slot]; exist {
				config.Shards[i] = gid
				break
			}
		}
	}
}

type Group struct {
	Gid    int
	Shards []int
}

//这里的分片逻辑写得有点恶心，没有BST的数据结构写起来有点难受
func (config *Config) Balance() {
	size := len(config.Groups)
	if size == 0 {
		return
	}
	//计算每个分组更新后的分配的分片数量
	avg := NShards / size
	mod := NShards % size
	cnt := make([]int, size)
	for i := 0; i < size; i++ {
		cnt[i] = avg
		if mod > 0 {
			cnt[i]++
			mod--
		}
	}
	//key-gid,value-分片ID
	groups := make(map[int][]int)
	for gid := range config.Groups {
		groups[gid] = make([]int, 0)
	}
	//统计被删除的后需要重新分配的分片
	left := make([]int, 0)
	for i := 0; i < NShards; i++ {
		gid := config.Shards[i]
		if _, exist := config.Groups[gid]; !exist {
			left = append(left, i)
		} else {
			groups[gid] = append(groups[gid], i)
		}
	}
	//为了确保不同副本的结果固定，需要做一次排序
	groupList := make([]*Group, len(groups))
	idx := 0
	for gid := range groups {
		groupList[idx] = &Group{gid, groups[gid]}
		idx++
	}
	sort.Slice(groupList, func(i, j int) bool {
		l1 := len(groupList[i].Shards)
		l2 := len(groupList[j].Shards)
		//分片大的排在前面
		if l1 != l2 {
			return l1 > l2
		}
		return groupList[i].Gid < groupList[j].Gid
	})
	//多退少补
	for i := 0; i < len(groups); i++ {
		group := groupList[i]
		c := cnt[i]
		if len(group.Shards) == c {
			continue
		} else if len(group.Shards) > c {
			remove := len(group.Shards) - c
			//删除
			left = append(left, group.Shards[0:remove]...)
			group.Shards = group.Shards[remove:]
		} else {
			add := c - len(group.Shards)
			group.Shards = append(group.Shards, left[0:add]...)
			left = left[add:]
		}
	}
	//修改结果
	for _, group := range groupList {
		for _, shard := range group.Shards {
			config.Shards[shard] = group.Gid
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

	ClientId int
	UniqId   int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int

	ClientId int
	UniqId   int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

	ClientId int
	UniqId   int64
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
