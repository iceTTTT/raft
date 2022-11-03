package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "sync/atomic"
import "6.824/shardctrler"
import "time"
import "bytes"



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ConfigNum   int
	Option 		string
	Key			string
	Value		string
	Unikey      Key
	Config 		shardctrler.Config

	Shardnum	int
	Storemap	map[string]string
	Unimap		map[Key]bool
	Retmap		map[Key]string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	sm 			 *shardctrler.Clerk
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	config   	 shardctrler.Config
	storemap     map[int]map[string]string
	changing	 map[int]bool
	unimap		 map[int]map[Key]bool
	retmap		 map[int]map[Key]string

	dead		 int32
	// Your definitions here.
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Check if this group owns the key.
	if !kv.rf.Isleader() {
		reply.Err = ErrWrongLeader
		return
	}

	shard := key2shard(args.Key)
	// Check if the shard belong this gid in current config.
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
		
	// May still in change.
	if kv.changing[shard] {
		reply.Err = ErrGetting
		return
	}

	op := Op{}
	op.ConfigNum = kv.config.Num
	op.Unikey = args.Unikey
	op.Key = args.Key
	op.Option = "Get"

	_, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	// May be a isolated leader. Wait for the index to be commmitted.
	// Retry timeout 1.2s
	retry := 40
	_, ok := kv.unimap[shard][args.Unikey]
	for !ok && kv.rf.Isleader() && !kv.killed() {
		kv.mu.Unlock()
		retry--
		time.Sleep(30 * time.Millisecond)
		kv.mu.Lock()
		// Retry timeout.
		if retry <= 0 {
			reply.Err = ErrWrongConfig
			return
		}
		if kv.config.Num != op.ConfigNum {
			break
		}
		_, ok = kv.unimap[shard][args.Unikey]
	}	

	if kv.config.Num != op.ConfigNum {
		reply.Err = ErrWrongConfig
		return
	}
	
	if !kv.rf.Isleader() {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
	reply.Value = kv.retmap[shard][op.Unikey]
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	defer kv.mu.Unlock()
	if !kv.rf.Isleader() {
		reply.Err = ErrWrongLeader
		return
	}

	// Group check.
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	
	if kv.changing[shard] {
		reply.Err = ErrGetting
		return
	}

	op := Op{}
	op.ConfigNum = kv.config.Num
	op.Unikey = args.Unikey
	op.Key = args.Key	
	op.Value = args.Value
	op.Option = args.Op
	_, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return 
	}

	retry := 40
	// May be a isolated leader. Wait for the index to be commmitted.
	// Retry timeout 1.2s
	_, ok := kv.unimap[shard][args.Unikey]
	for !ok && kv.rf.Isleader() && !kv.killed() {
		kv.mu.Unlock()
		retry--
		time.Sleep(30 * time.Millisecond)
		kv.mu.Lock()
		// Retry timeout.
		if kv.config.Num != op.ConfigNum {
			break
		}
		_, ok = kv.unimap[shard][args.Unikey]
	}	

	if kv.config.Num != op.ConfigNum {
		reply.Err = ErrWrongConfig
		return
	}
	
	if !kv.rf.Isleader() {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
}

func (kv *ShardKV) ApplyHandler() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for !kv.killed() {
		select {
		case msg := <- kv.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				shard := key2shard(op.Key)
				if op.Option == "config" {
					// Should execute the re-configuration.
					// in normal processing comes into this region 
					// Means nothing in change any more.
					if op.Config.Num != kv.config.Num + 1 {
						break
					}
					kv.ConfigChange(op.Config)
					break
				}
				if op.Option == "Shard" {
					if op.ConfigNum != kv.config.Num {
						goto check
					}
					if !kv.changing[op.Shardnum] {
						goto check
					}
					kv.changing[op.Shardnum] = false

					// The map in the op, maybe the same in both replica. 
					// Need Deep copy.
					kv.storemap[op.Shardnum] = make(map[string]string)
					kv.unimap[op.Shardnum] = make(map[Key]bool)
					kv.retmap[op.Shardnum] = make(map[Key]string)
					for k, v := range op.Storemap {
						kv.storemap[op.Shardnum][k] = v
					}
					for k, v := range op.Unimap {
						newkey := Key{}
						newkey.Cid = k.Cid
						newkey.Snum = k.Snum
						kv.unimap[op.Shardnum][newkey] = v
					}
					for k, v := range op.Retmap {
						newkey := Key{}
						newkey.Cid = k.Cid
						newkey.Snum = k.Snum
						kv.retmap[op.Shardnum][newkey] = v
					}
					goto check
				} 

				// Only apply request in current config.
				if op.ConfigNum != kv.config.Num {
					goto check
				}

				// This if is redundent.Use for declaration.
				if kv.unimap[shard] == nil {
					goto check
				} else {
					for k, _ := range kv.unimap[shard] {
						if k.Cid == op.Unikey.Cid && k.Snum != op.Unikey.Snum {
							delete(kv.unimap[shard], k)
							delete(kv.retmap[shard], k)
						}
					}
					_, ok := kv.unimap[shard][op.Unikey]
					if ok {
						// Check snapshot.
						kv.unimap[shard][op.Unikey] = true
						goto check
					}
				}
				switch op.Option {
				case "Put":
					kv.storemap[shard][op.Key] = op.Value
				case "Append":
					_, ok := kv.storemap[shard][op.Key]
					if !ok {
						kv.storemap[shard][op.Key] = op.Value
					} else {
						kv.storemap[shard][op.Key] = kv.storemap[shard][op.Key] + op.Value
					}
					
				case "Get":
					_, ok := kv.storemap[shard][op.Key]
					if !ok {
						kv.retmap[shard][op.Unikey] = ""
					} else {
						kv.retmap[shard][op.Unikey] = kv.storemap[shard][op.Key] 
					}
				}
				kv.unimap[shard][op.Unikey] = true
				// Check snapshot.
				check:
				if kv.maxraftstate > 0 && kv.maxraftstate <= kv.rf.GetPersister().RaftStateSize() {
					// Should make snapshot.	
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.storemap)
					e.Encode(kv.unimap)
					e.Encode(kv.config)
					e.Encode(kv.changing)
					e.Encode(kv.retmap)
					snapshot := w.Bytes()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}
			} else {
				// Get snapshot. should replace the curret state.
				r := bytes.NewBuffer(msg.Snapshot)
				d := labgob.NewDecoder(r)
				kv.storemap = nil
				kv.unimap = nil
				kv.config = shardctrler.Config{}
				kv.changing = nil
				kv.retmap = nil
				d.Decode(&kv.storemap)
				d.Decode(&kv.unimap)
				d.Decode(&kv.config)
				d.Decode(&kv.changing)
				d.Decode(&kv.retmap)
				for s, change := range kv.changing {
					if change {
						// inchanging and not belong to this gid.
						if kv.config.Shards[s] != kv.gid {
							go kv.Send(s, kv.config.Shards[s], kv.config.Num)
						}
					}
				}
			}
		default:
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			kv.mu.Lock()
		}
	
	}
}

func (kv *ShardKV) CheckConfigure() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for !kv.killed() {
		// Ask ctrler for current configuration.
		// This call shall keep waiting, till gets return
		// In this go routine.
		kv.mu.Unlock()
		time.Sleep(80 * time.Millisecond)
		kv.mu.Lock()

		// Check if any change is happening , if any wait.
		for !kv.killed() {
			inchanging := false
			for _, change := range kv.changing {
				if change {
					inchanging = true
				}
			}
			if inchanging {
				kv.mu.Unlock()
				time.Sleep(50 * time.Millisecond)
				kv.mu.Lock()
			} else {
				break
			}
    	}

		config := kv.sm.Query(kv.config.Num + 1)
		if kv.config.Num + 1 == config.Num {
			op := Op{}
			op.Option = "config"
			op.Config = config
			kv.rf.Start(op)
		}
	}
}

func (kv *ShardKV) ConfigChange(config shardctrler.Config) {
	// Call this function with lock hold.
	// If configuration is older than current one just return.
	//Must change one by one.
	// In restart the current config may still in change.
	// Should wait.
	if config.Num != kv.config.Num + 1 {
		return
	}

	inchange := false
	for _, change := range kv.changing {
		if change {
			inchange = true
			break
		}
	}
	if inchange {
		// Here while we are changing the bigger should not come.
		// Must be restart.
		// And here we complete change in origin log.
		// All shard we need is made sure to be applied before this
		// May wait for send. But the map to send is in peer group. so we can just step in newconfig
		// We should delete all send change.
		for s, change := range kv.changing {
			// This s should send to peer group in current config.
			if change && kv.config.Shards[s] != kv.gid {
				delete(kv.changing, s)
				delete(kv.storemap, s)
				delete(kv.retmap, s)
				delete(kv.unimap, s)
			}
		}
		for s, _ := range kv.changing {
			kv.changing[s] = false
		}
		// Then just step in new config.
	}
	// The initialization will not get shard from other group.
	if kv.config.Num == 0 {
		for s, g := range config.Shards {
			if g == kv.gid {
				kv.storemap[s] = make(map[string]string)
				kv.unimap[s] = make(map[Key]bool)
				kv.retmap[s] = make(map[Key]string)
				kv.changing[s] = false
			}
		}
		kv.config = config
		return
	}	

	send := make(map[int]int)
	get := make([]int, 0)
	// Check the change of old shards.
	for s, _ := range kv.storemap {
		if config.Shards[s] != kv.gid {
			send[s] = config.Shards[s]
		}
	}
	// Find shard new to this group	
	for s, newgid := range config.Shards {
		if newgid == kv.gid && kv.config.Shards[s] != kv.gid {
			get = append(get, s)
		}
	}

	// In this point, really change into new config.
	kv.config = config



	for _, s := range get {
		kv.storemap[s] = make(map[string]string)
		kv.unimap[s] = make(map[Key]bool)
		kv.retmap[s] = make(map[Key]string)
		kv.changing[s] = true
	}

	for s, gid := range send {
		kv.changing[s] = true
		// Can't not delete all map here,
		// Because we cant make sure before target got the map
		// the system won't crash.
		// If crash, we lost the informaion permanently.
		go kv.Send(s, gid, kv.config.Num)
	}

}

func (kv *ShardKV) Shard(args *ShardArgs, reply *ShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.ConfigNum = kv.config.Num
	if args.Num != kv.config.Num {

		if args.Num < kv.config.Num {
			// Sender has lower config.
			reply.Err = ErrLowConfig

		} else {
			// Sender has bigger config.
			for s, change := range kv.changing {
				if change && kv.config.Shards[s] == args.Gid {
					// Don't need to send to this sender.
					delete(kv.changing, s)
				}
			}
			reply.Err = ErrWrongConfig
		}
		return
	}

	if !kv.rf.Isleader() {
		reply.Err = ErrWrongLeader
		return
	}

	if !kv.changing[args.Shard] {
		reply.Err = OK
		return
	}

	op := Op{}
	op.ConfigNum = kv.config.Num
	op.Option = "Shard"
	op.Shardnum = args.Shard
	op.Storemap = args.Storemap
	op.Retmap = args.Retmap
	op.Unimap = args.Unimap
	_, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	for kv.changing[args.Shard] && kv.rf.Isleader() && !kv.killed() {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		if kv.config.Num != op.ConfigNum {
			break
		}
	}
	if kv.config.Num != op.ConfigNum {
		reply.Err = ErrLowConfig
		return
	}	

	if !kv.rf.Isleader() {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK

}

func (kv *ShardKV) Send(s , gid, confignum int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num != confignum {
		return
	}

	args := ShardArgs{}
	args.Storemap = kv.storemap[s]
	args.Unimap = kv.unimap[s]
	args.Retmap = kv.retmap[s]
	args.Shard = s
	args.Num = kv.config.Num
	args.Gid = kv.gid
	suc := false
	for !suc {

		// Not ok.
		kv.mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		kv.mu.Lock()

		if servers, ok := kv.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply ShardReply
				// Not waiting for reply.
				kv.mu.Unlock()
				ok := srv.Call("ShardKV.Shard", &args, &reply)

				if !ok {
					time.Sleep(30 * time.Millisecond)
					kv.mu.Lock()
					continue
				}	
				if reply.Err == ErrWrongLeader {
					time.Sleep(30 * time.Millisecond)
					kv.mu.Lock()
					continue
				}
				if reply.Err == ErrWrongConfig {
					// We got large config.Wait for target to be large.
					time.Sleep(50 * time.Millisecond)
					kv.mu.Lock()
					continue
				}
				// Lowconfig, also sucessiful, dont need to send anymore.
				// Send sucessifully, delete the changing.
				// Here we can delete the map after delete the changing.
				kv.mu.Lock()
				// Config is change, may not need to delete.
				if kv.config.Num != confignum {
					return
				}

				delete(kv.changing, s)
				delete(kv.storemap, s)
				delete(kv.unimap, s)
				delete(kv.retmap, s)
				// Here send over.
				suc = true
				break
			}
		}
	}

}



func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.sm = shardctrler.MakeClerk(ctrlers)

	// First time ask for the configure.
	// Ask for the current configure.

	kv.unimap = make(map[int]map[Key]bool)
	kv.storemap = make(map[int]map[string]string)
	kv.retmap = make(map[int]map[Key]string)
	kv.changing = make(map[int]bool)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	go kv.ApplyHandler()
	go kv.CheckConfigure()
	return kv
}
