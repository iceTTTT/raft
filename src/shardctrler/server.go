package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "sync/atomic"
import "6.824/labgob"
import "time"
import "sort"

type sorttype []int

func (c sorttype) Len() int { return len(c) }

func (c sorttype) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

func (c sorttype) Less(i, j int) bool { return c[i] < c[j] }


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	unimap  map[Key]bool
	dead	int32
	configs []Config // indexed by config num
}

type Key struct {
	Cid  int
	Snum int
}

type Op struct {
	// Your data here.
	Unikey Key
	Option string
	Todo   interface{}
}

type Movesg struct {
	Shard int
	Gid   int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{}
	op.Unikey = args.Unikey
	op.Option = "join"
	op.Todo = args.Servers
	_, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}
	// Maybe a isolated leader.
	_, ok := sc.unimap[args.Unikey]
	for !ok && sc.rf.Isleader() && !sc.killed() {
		sc.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		sc.mu.Lock()
		_, ok = sc.unimap[args.Unikey]
	}

	if !sc.rf.Isleader() && sc.killed() {
		reply.WrongLeader = true
		return
	} 

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{}
	op.Unikey = args.Unikey
	op.Option = "leave"
	op.Todo = args.GIDs
	_, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}
	// Maybe a isolated leader.
	_, ok := sc.unimap[args.Unikey]
	for !ok && sc.rf.Isleader() && !sc.killed() {
		sc.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		sc.mu.Lock()
		_, ok = sc.unimap[args.Unikey]
	}

	if !sc.rf.Isleader() && sc.killed() {
		reply.WrongLeader = true
		return
	} 

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{}
	op.Unikey = args.Unikey
	op.Option = "move"
	op.Todo = Movesg{args.Shard, args.GID}
	_, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}
	// Maybe a isolated leader.
	_, ok := sc.unimap[args.Unikey]
	for !ok && sc.rf.Isleader() && !sc.killed() {
		sc.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		sc.mu.Lock()
		_, ok = sc.unimap[args.Unikey]
	}

	if !sc.rf.Isleader() && sc.killed() {
		reply.WrongLeader = true
		return
	} 

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, ok := sc.unimap[args.Unikey]
	if ok {
		delete(sc.unimap, args.Unikey)
	}
	op := Op{}
	op.Unikey = args.Unikey
	op.Option = "query"
	op.Todo = args.Num
	_, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}
	// Maybe a isolated leader.
	_, ok = sc.unimap[args.Unikey]
	for !ok && sc.rf.Isleader() && !sc.killed() {
		sc.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		sc.mu.Lock()
		_, ok = sc.unimap[args.Unikey]
	}

	if !sc.rf.Isleader() && sc.killed() {
		reply.WrongLeader = true
		return
	} 

	if  args.Num == -1 || args.Num >= len(sc.configs) {
		// Should return the last config.
		reply.Config = sc.configs[len(sc.configs) - 1]
		return
	}
	reply.Config = sc.configs[args.Num]
}

func (sc *ShardCtrler) ApplyHandler() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for !sc.killed() {
		select {
		case msg := <- sc.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				for key, _ := range sc.unimap {
					if key.Cid == op.Unikey.Cid && key.Snum != op.Unikey.Snum {
						delete(sc.unimap, key)
					}
				}
				_, ok := sc.unimap[op.Unikey]
				if ok {
					goto out
				}
				switch op.Option {
				case "join":
					sc.handlejoin(&op)
				case "move":
					sc.handlemove(&op)
				case "leave":
					sc.handleleave(&op)
				case "query":
					sc.handlequery(&op)
				}
				sc.unimap[op.Unikey] = true
				out:
			}
		default:
			sc.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			sc.mu.Lock()
		}
	}
}

func (sc *ShardCtrler) handlejoin(op *Op) {
	// New configuration. join new groups.
	// One command will only be executed once, it is ok to use the group.
	// The above is wrong. because all server will share the same command in op.
	// Need deep copy.
	group := op.Todo.(map[int][]string)
	newgroup := make(map[int][]string)
	for k, v := range group {
		newgroup[k] = v
	}
	group = newgroup
	// All group should get the same amount of shards, the last one get the least.
	newconfig := Config{}
	newconfig.Num = len(sc.configs)
	// Check not assigned.
	na := false
	for _, v := range sc.configs[len(sc.configs) - 1].Shards {
		if v == 0 {
			na = true
			break
		}
	}
	if na {
		ag := make([]int, 0)
		for key, _ := range group {
			ag = append(ag, key)
		}
		sort.Sort(sorttype(ag))
		totalnum := len(group)
		// Every group should be assgined toall shards
		toall := NShards / totalnum
		// Remain shards, should assgin to the pre remain group.
		remain := NShards % totalnum
		newconfig.Groups = group
		// snum represent the shards numbers
		snum := 0
		// Assign toall, plus 1. Decrease remain to 0. Gid in ascending order.
		for _, key := range ag {
			num := toall	
			for num != 0 {
				num--
				newconfig.Shards[snum] = key
				snum++
			}
			if remain != 0 {
				newconfig.Shards[snum] = key
				snum++
				remain--
			}
		}
		sc.configs = append(sc.configs, newconfig)
		return
	}
	// Find all pre Gid in ascending order, and how many shards they have.
	// delete group already have, in the new map.
	shardcount := make(map[int]int)
	for gid, _ := range sc.configs[len(sc.configs) - 1].Groups {
		shardcount[gid] = 0 
	}
	pg := make([]int, 0)
	for _, gid := range sc.configs[len(sc.configs) - 1].Shards {
		shardcount[gid]++	
		if shardcount[gid] == 1 {
			pg = append(pg, gid)
		}
		// If exist in new group delete it.
		_, ok := group[gid]
		if ok {
			delete(group, gid)
		}
	}
	sort.Sort(sorttype(pg))
	ag := make([]int, 0)
	for key, _ := range group {
		ag = append(ag, key)
	}
	sort.Sort(sorttype(ag))
	newtoall := NShards / (len(pg) + len(ag))
	// Make new map.Use the group plus old groups.
	for k, v := range sc.configs[len(sc.configs) - 1].Groups {
		group[k] = v
	}

	newconfig.Groups = group
	// Copy of shards.
	for i, v := range sc.configs[len(sc.configs) - 1].Shards {
		newconfig.Shards[i] = v
	}
	// Reconfigure the Shards group 
	index := 0
	ngid := ag[index]
	threshold := newtoall
	round := 1
	for round <= 2 {
		// First round assgin new group with at most newtoall shards.

		// Second round check the old group if any got more than newtoall + 1.
		// If any, means any new group got newtoall shards in round 1. 

		// Check the old gid in ascending order.
		for _, gid := range pg {
			// Every loop , decrease the shards num of gid to newtoall + 1.
			for shardcount[gid] > newtoall + 1 {
				// Assgin shards in ascending number.
				for shardcount[ngid] >= threshold || index >= len(ag) {
					index++
					if index >= len(ag) {
						goto nextround
					}
					ngid = ag[index]
				}
				// Got a ngid in ascending order with not enough shards.
				// Find a shards in ascending order that belongs to gid to assign to ngid.
				for s, g := range newconfig.Shards {
					if g == gid {
						newconfig.Shards[s] = ngid
						shardcount[gid]--
						shardcount[ngid]++
						break
					}
				}
			}
		}
		nextround:
		if round == 1 && index < len(ag) {
			// if index < len(ag) means that every old group only got newtoall + 1 shards.
			// Check new group
			break	
		}
		round++
		index = 0
		ngid = ag[index]
		threshold = newtoall + 1
	}
	// New group may lack in this scenary.
	if round == 1 {
		index = 0
		ngid = pg[index]
		for _, gid := range ag {
			// For every new group id check if lack shards.
			for shardcount[gid] < newtoall {
				for shardcount[ngid] <= newtoall || index >= len(pg) {
					index++
					if index >= len(pg) {
						goto end
					}
					ngid = pg[index]
				}	
				// new gid lack, and ngid > newtoall. Move once.
				for s, g := range newconfig.Shards {
					if g == ngid {
						newconfig.Shards[s] = gid 
						shardcount[ngid]--
						shardcount[gid]++
						break
					} 
				}
			}
		}
	}
	end:
	sc.configs = append(sc.configs, newconfig)
}

func (sc *ShardCtrler) handlemove(op *Op) {
	sg := op.Todo.(Movesg)	
	newconfig := Config{}
	newconfig.Num = len(sc.configs)
	newconfig.Groups = make(map[int][]string)
	for k, v := range sc.configs[len(sc.configs) - 1].Groups {
		newconfig.Groups[k] = v
	}
	// Copy to the array.
	for i, v := range sc.configs[len(sc.configs) - 1].Shards {
		newconfig.Shards[i] = v
	}
	newconfig.Shards[sg.Shard] = sg.Gid
	sc.configs = append(sc.configs, newconfig)
}

func (sc *ShardCtrler) handleleave(op *Op) {
	gids := op.Todo.([]int)
	egids := make([]int, 0)
	newconfig := Config{}	
	newconfig.Num = len(sc.configs)
	newconfig.Groups = make(map[int][]string)
	// Copy the most recent group.
	for k, v := range sc.configs[len(sc.configs)- 1].Groups {
		newconfig.Groups[k] = v
	}
	// Check if any leave gid are not in the most recent group.
	// Only leave those exist in previous config.
	// And delete those groups.
	for _, gid := range gids {
		_, ok := sc.configs[len(sc.configs) - 1].Groups[gid]
		if ok {
			egids = append(egids, gid)
			delete(newconfig.Groups, gid)	
		}
	}
	// If All leave.
	if len(newconfig.Groups) == 0 {
		sc.configs = append(sc.configs, newconfig)
		return
	}
	// All remaing gid in acending order.
	rg := make([]int, 0)
	for gid, _ := range newconfig.Groups {
		rg = append(rg, gid)
	}
	sort.Sort(sorttype(rg))

	shardcount := make(map[int]int)
	for gid, _ := range sc.configs[len(sc.configs) - 1].Groups {
		shardcount[gid] = 0 
	}
	for _, gid := range sc.configs[len(sc.configs) - 1].Shards {
		shardcount[gid]++
	}
	// Egid are those gid to leave in ascending order.
	sort.Sort(sorttype(egids))
	// Find the previous shards num.low and high.
	low := 10000000
	high := -1
	for _, v := range shardcount {
		if v < low {
			low = v
		}
		if v > high {
			high = v
		}
	}
	index := 0
	rgid := rg[index]
	// Copy previous shard configuration.
	for i, v := range sc.configs[len(sc.configs) - 1].Shards {
		newconfig.Shards[i] = v
	}
	if low != high {
		// Fulfill all low group. and go to low == high.
		for _, gid := range egids {
			for s, g := range newconfig.Shards {
				if g == gid {
					// Find a low rgid, and move to it.
					for shardcount[rgid] == high {
						index = index + 1
						if index >= len(rg) {
							goto out
						}
						rgid = rg[index]
					}
					newconfig.Shards[s] = rgid
					shardcount[rgid]++
				}
			}
		}
	}
	out:
	// If not all high gids have no shards in newshards at all.
	// at which circumstance. The following reconfigure will not take effect.
	// If all high, give those remaining of gids one to one to remaining gid, in acending order.
	index = 0
	rgid = rg[index]
	// Move shards of gids in ascending order.
	for _, gid := range egids {
		// Move the shards of gid to 0
		for s, g := range newconfig.Shards {
			if g == gid {
				newconfig.Shards[s] = rgid
				// Next rg
				index = (index + 1) % len(rg)
				rgid = rg[index]
			}
		}
	}
	sc.configs = append(sc.configs, newconfig)
}

func (sc *ShardCtrler) handlequery(op *Op) {
	 // Should do nothing.
	 return
}
//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(map[int][]string{})
	labgob.Register([]int{})
	labgob.Register(Movesg{})

	sc.applyCh = make(chan raft.ApplyMsg, 1)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.unimap = make(map[Key]bool)
	// Your code here.
	go sc.ApplyHandler()
	return sc
}
