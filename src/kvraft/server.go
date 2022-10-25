package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Unikey  Key
	Key  	string
	Value 	string
	Option 		string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	smap	map[Key]bool

	storemap	map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Check applied. If commit, must delete, Get must return with the most recently commited value.
	_, ok := kv.smap[args.Unikey]
	if ok {
		delete(kv.smap, args.Unikey)
	}
	reply.MayLeader = -1
	op := Op{}
	op.Unikey = args.Unikey
	op.Key = args.Key
	op.Option = "Get"
	_, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.MayLeader = kv.rf.VotedFor()
		reply.Err = Err("NO")
		return
	}
	raft.Printo(raft.DServer, "S%v got raft%v got Get call with key %v\n", kv.me, kv.rf.GetMe(), args.Key)
	_, ok = kv.smap[args.Unikey]
	for !ok && kv.rf.Isleader() && !kv.killed() {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		_, ok = kv.smap[args.Unikey]
	}
	
	if !kv.rf.Isleader() || kv.killed() {
		reply.MayLeader = kv.rf.VotedFor()
		reply.Err = Err("NO")
		return
	}
	reply.Err = Err("YES")
	_, ok = kv.storemap[args.Key]
	if !ok {
		reply.Value = ""
	} else {
		reply.Value = kv.storemap[args.Key]
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.MayLeader = -1
	op := Op{}
	op.Unikey = args.Unikey
	op.Key = args.Key	
	op.Value = args.Value
	op.Option = args.Op
	_, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.MayLeader = kv.rf.VotedFor()
		reply.Err = Err("NO")
		return 
	}
	raft.Printo(raft.DServer, "S%v got PutAppend call from with op: %v key: %v value: %v\n", kv.me, args.Op, args.Key, args.Value)
	// May be a isolated leader. Wait for the index to be commmitted.
	_, ok := kv.smap[args.Unikey]
	for !ok && kv.rf.Isleader() && !kv.killed() {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		_, ok = kv.smap[args.Unikey]
	}	
	
	if !kv.rf.Isleader() || kv.killed() {
		reply.MayLeader = kv.rf.VotedFor()
		reply.Err = Err("NO")
		return
	}
	raft.Printo(raft.DServer, "S%v return to APCall call \n", kv.me)
	reply.Err = Err("YES")
}

func (kv *KVServer) ApplyHandler() {
	kv.mu.Lock()	
	defer kv.mu.Unlock()
	for !kv.killed()  {
		select {
		case msg := <- kv.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				for k, _ := range kv.smap {
					if k.Cid == op.Unikey.Cid && k.Snum != op.Unikey.Snum {
						delete(kv.smap, k)
					}
				}
				_, ok := kv.smap[op.Unikey]
				if ok {
					// Check snapshot.
					goto cs
				}
				raft.Printo(raft.DServer, "Unikey %v got commit and apply\n", op.Unikey)
				switch op.Option {
				case "Put":
					kv.storemap[op.Key] = op.Value
				case "Append":
					_, ok := kv.storemap[op.Key]
					if !ok {
						kv.storemap[op.Key] = op.Value
					} else {
						kv.storemap[op.Key] = kv.storemap[op.Key] + op.Value
					}
				default:
				}
				kv.smap[op.Unikey] = true
				// Check snapshot.
				cs:
				if kv.maxraftstate > 0 && kv.maxraftstate <= kv.rf.GetPersister().RaftStateSize() {
					// Should make snapshot.	
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.storemap)
					e.Encode(kv.smap)
					snapshot := w.Bytes()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}
			} else {
				// Get snapshot. should execute it.
				r := bytes.NewBuffer(msg.Snapshot)
				d := labgob.NewDecoder(r)
				d.Decode(&kv.storemap)
				d.Decode(&kv.smap)
			}
		default:
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			kv.mu.Lock()
		}
	}
}
//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.storemap = make(map[string]string)
	kv.smap = make(map[Key]bool)
	// You may need initialization code here.
	if maxraftstate > 0 {
		r := bytes.NewBuffer(kv.rf.GetPersister().ReadSnapshot())
		d := labgob.NewDecoder(r)
		d.Decode(&kv.storemap)
		d.Decode(&kv.smap)
	}
	go kv.ApplyHandler()
	return kv
}
