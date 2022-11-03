package shardkv

import "sync"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrSending     = "ErrSending"
	ErrGetting     = "ErrGetting"
	ErrWrongConfig = "ErrWrongConfig"
	ErrLowConfig   = "ErrLowConfig"
)

type Err string

var clientid int
var mu	     sync.Mutex

type Key struct {
	Cid  int
	Snum int
}
// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Unikey Key
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Unikey Key
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardArgs struct {

	Storemap map[string]string
	Unimap   map[Key]bool
	Retmap   map[Key]string
	Shard	 int
	Num	 int
	Gid      int
}

type ShardReply struct {
	Err	Err
	ConfigNum int
}
