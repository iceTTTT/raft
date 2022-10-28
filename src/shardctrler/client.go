package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	Cid int
	Snum int
	mu	sync.Mutex
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
	mu.Lock()
	defer mu.Unlock()
	ck.Cid = clientid
	clientid++
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &QueryArgs{}
	// Your code here.
	args.Num = num

	args.Unikey = Key{ck.Cid, ck.Snum}
	ck.Snum++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	args.Unikey = Key{ck.Cid, ck.Snum}
	ck.Snum++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	args.Unikey = Key{ck.Cid, ck.Snum}
	ck.Snum++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	args.Unikey = Key{ck.Cid, ck.Snum}
	ck.Snum++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
