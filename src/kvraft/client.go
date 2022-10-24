package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"
import "6.824/raft"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	xleader int
	mu      sync.Mutex
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
	ck.xleader = int(nrand()) % len(ck.servers)
	// You'll have to add code here.
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
func (ck *Clerk) GetCall(x int, ok *bool, reply *GetReply, args *GetArgs) {
	r := GetReply{}
	*ok = ck.servers[x].Call("KVServer.Get", args, &r)
	*reply = r
}

func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()	
	defer ck.mu.Unlock()
	args := GetArgs{}
	args.Key = key
	mu.Lock()
	args.Snum = snum
	snum++
	mu.Unlock()

	var ok bool
	var reply GetReply
	x := ck.xleader
	for {
		x = ck.xleader
		ck.mu.Unlock()	
		raft.Printo(raft.DClient, "Client send Get call \n")
		ck.GetCall(x, &ok, &reply, &args)
		for !ok {
			time.Sleep(10 * time.Millisecond)
			ck.mu.Lock()
			ck.xleader = int(nrand()) % len(ck.servers)
			x = ck.xleader
			ck.mu.Unlock()
			raft.Printo(raft.DClient, "Client send Get call in not ok loop\n")
			ck.GetCall(x, &ok, &reply, &args)
		}
		ck.mu.Lock()
		if reply.Err == Err("YES") {
			break
		}
		ck.xleader = int(nrand()) % len(ck.servers)
	}
	raft.Printo(raft.DClient, "Client got Get return key:%v got value:%v\n", key, reply.Value)
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
func (ck *Clerk) PACall(x int, a *PutAppendArgs, ok *bool, reply *PutAppendReply) {
	r := PutAppendReply{}
	*ok = ck.servers[x].Call("KVServer.PutAppend", a, &r)
	*reply = r
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	// Hold the global lock, and increase the snumber.
	mu.Lock()
	args.Snum = snum
	snum++
	mu.Unlock()

	var ok bool
	var reply PutAppendReply
	x := ck.xleader
	for {
		// Release the lock, and the function keep waiting the rpc call return. 
		x = ck.xleader
		ck.mu.Unlock()
		raft.Printo(raft.DClient, "Client send putappend call to server with op:%v key:%v value:%v\n", args.Op, args.Key, args.Value) 
		ck.PACall(x, &args, &ok, &reply)
		raft.Printo(raft.DClient, "First PACall return\n")
		for !ok {
			time.Sleep(10 * time.Millisecond)
			ck.mu.Lock()
			ck.xleader = (ck.xleader + 1) % len(ck.servers)
			x = ck.xleader
			// Same waiting functon.
			ck.mu.Unlock()
			raft.Printo(raft.DClient, "Client send putappend call in not ok loop with op:%v key:%v value:%v\n", args.Op, args.Key, args.Value)
			ck.PACall(x, &args, &ok, &reply)
			raft.Printo(raft.DClient, "PACall return\n")
		}
		ck.mu.Lock()
		if reply.Err == Err("YES") {
			break
		}
		ck.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		ck.mu.Lock()
		ck.xleader = (ck.xleader + 1) % len(ck.servers)
	}
	raft.Printo(raft.DClient, "Client PACall get final return\n")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
