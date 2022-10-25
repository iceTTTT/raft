package kvraft

import "sync"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

var mu sync.Mutex
var clientid int

type Key struct {
	Cid		int
	Snum	int
}
// Put or Append
type PutAppendArgs struct {
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
	// Notify the leader this server know
	MayLeader int
}

type GetArgs struct {
	Key string
	Unikey Key
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string

	MayLeader int
}
