package kvraft

import "sync"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

var mu sync.Mutex
var snum int64
// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Snum  int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
	// Notify the leader this server know
	MayLeader int
}

type GetArgs struct {
	Key string
	Snum int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string

	MayLeader int
}
