package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sync"
	"sync/atomic"
	"math/rand"
	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"io"
	// "modernc.org/mathutil"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A log type.
type Log struct {
	Command interface{}
	Term int
}

type Timer struct {
	mu 		sync.Mutex
	cond 	*sync.Cond
	ticker  int
	timeout int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	timer	  Timer
	isleader  bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Non-Volatile
	currentTerm int
	votedFor int
	preindex int
	preterm  int
	log []Log

	// Volatile

	commitIndex int
	lastApplied int

	// Volatile && For-Leader use

	// Next index to send to the followers.
	nextIndex []int 
	// Highest index known to the leader.	
	matchIndex []int

	apmsg chan ApplyMsg
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type SnapshotArgs struct {
	Term int
	LeaderId int
	Lastindex int
	Lastterm int
	Offset int
	Data []byte
	Done bool
}

type SnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {

}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been Killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := 0
	term := rf.currentTerm
	isLeader := rf.isleader
	// Your code here (2B).
	if rf.isleader {
		rf.log = append(rf.log, Log{command, rf.currentTerm})
		// Persist after log change.
		rf.persist()
		index = rf.preindex + len(rf.log)
		Printo(dLog, "S%v got log length %v wit llt %v\n", rf.me, len(rf.log), rf.currentTerm)
		LastLogIndex := rf.preindex + len(rf.log)
		for i := 0; i < len(rf.peers) && !rf.Killed() && rf.isleader; i++ {
			if i == rf.me {
				continue
			}
			if LastLogIndex >= rf.nextIndex[i] {
				go rf.sendAppendEntries(i)
			}  
		}
	}
	return index, term, isLeader
}

// For AppendEntries rpc.

type AppendArgs struct {
	Leaderid   	 int
	LeaderTerm 	 int
	LeaderCommit int
	PreLogIndex  int
	PreLogTerm   int
	Entries		 []Log
}

type AppendReply struct {
	Repterm int
	Suc     bool
	// 0 represent term.
	Cause	int
	Cftterm int
	Cftindex int
}

func (rf *Raft) AppendEntries(args *AppendArgs, reply *AppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Check leader term.
	Printo(dLog, "S%v got Append from S%v\n", rf.me, args.Leaderid)
	if args.LeaderTerm >= rf.currentTerm {
		// Reset election timeout. For the real leader.
		rf.timer.cond.L.Lock()
		rf.timer.ticker = 0
		rf.timer.timeout = rand.Int() % 500 + 400
		rf.timer.cond.L.Unlock()
		rf.currentTerm = args.LeaderTerm
		// If candidates or leader Become follower
		if rf.votedFor == rf.me {
			rf.votedFor = -1
		}
		rf.isleader = false
	}

	if args.LeaderTerm < rf.currentTerm {
		reply.Repterm = rf.currentTerm
		reply.Cause = 0
		reply.Suc = false
		Printo(dLog, "S%v with term %v reject S%v for low term %v\n", rf.me, rf.currentTerm, args.Leaderid, args.LeaderTerm)
		return
	}
	reply.Cause = 1
	if len(args.Entries) > 0 { 
		// check match	append entries
		for i := len(rf.log); i >= 0; i-- {
			if i + rf.preindex != args.PreLogIndex {
				continue
			}
			if (i == 0 && rf.preterm == args.PreLogTerm) || rf.log[i - 1].Term == args.PreLogTerm {
				Printo(dLog, "S%v Match index %v append entries length%v\n", rf.me, i+rf.preindex, len(args.Entries))
				inserted := 0
				reply.Suc = true
				i++
				for ; i <= len(rf.log) && inserted < len(args.Entries); i++ {
					rf.log[i - 1] = args.Entries[inserted] 
					inserted++
				}
				// Discard bigger index with old term
				for ; i <= len(rf.log); i++ {
					if rf.log[i - 1].Term < args.LeaderTerm {
						rf.log = rf.log[ : i-1]
						break
					}
				}
				for ; inserted < len(args.Entries); inserted++ {
					rf.log = append(rf.log, args.Entries[inserted])
				}
				Printo(dLog, "S%v append to log length %v with llt %v\n", rf.me, len(rf.log), rf.log[len(rf.log)-1].Term)
				break
			}
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		newthisterm := 0
		for i := len(rf.log) - 1; i >= 0; i-- {
			if rf.log[i].Term == args.LeaderTerm {
				newthisterm = rf.preindex + i + 1
				break
			}
		}
		if newthisterm > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = newthisterm
		}
		Printo(dLog, "S%v commit index %v\n", rf.me, rf.commitIndex)
		go rf.RfApplyMsg(rf.commitIndex)
	}
	reply.Repterm = rf.currentTerm
	rf.persist()
	return 
}

func (rf *Raft) sendAppendEntries(server int) {
start:
	rf.mu.Lock()
	lli := rf.preindex + len(rf.log)
	if lli < rf.nextIndex[server] || !rf.isleader {
		rf.mu.Unlock()
		return
	}
	args := AppendArgs{}
	args.Leaderid = rf.me	
	args.LeaderTerm = rf.currentTerm
	args.LeaderCommit = rf.commitIndex
	insliceindex := rf.nextIndex[server] - rf.preindex - 1	
	if insliceindex < 0 {
		rf.mu.Unlock()
		return 
	} 
	args.PreLogIndex = rf.nextIndex[server] - 1
	args.PreLogTerm = rf.preterm
	if insliceindex != 0 {
		args.PreLogTerm = rf.log[insliceindex - 1].Term
	}
	// May cause wrong. slice may change.
	args.Entries = rf.log[insliceindex:] 
	lastindexsend := rf.preindex + len(rf.log) 
	rf.mu.Unlock()
	// Send rpc
	reply := AppendReply{}
	ok := false
	if !ok && !rf.Killed() && rf.Isleader() {
		Printo(dLog, "S%v send append to S%v with term %v\n", rf.me, server, args.LeaderTerm)
		ok = rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		if !ok && !rf.Killed() && rf.Isleader() {
			time.Sleep(20 * time.Millisecond)
			goto start
		}
	}
	if !rf.Isleader() {
		return
	}
	// Check reply
	rf.mu.Lock()
	if !reply.Suc {
		if reply.Cause == 0 {
			// Become follower
			if reply.Repterm > rf.currentTerm {
				Printo(dVote, "S%v being reject by S%v and become follower", rf.me, server)
				rf.currentTerm = reply.Repterm
				rf.votedFor = -1
				rf.isleader = false
				rf.persist()
			}
			rf.mu.Unlock()
			return
		}
		// Change nextindex and restart.
		if rf.nextIndex[server] > rf.preindex + 1 {
			rf.nextIndex[server] = rf.preindex + 1
		} else {
			rf.mu.Unlock()
			return 
		}
		rf.mu.Unlock()
		Printo(dLog, "S%v Resend because unfit index\n", rf.me)
		goto start
	}	
	rf.matchIndex[server] = lastindexsend
	rf.nextIndex[server] = lastindexsend + 1
	lli = rf.preindex + len(rf.log)
	if	lli >= rf.nextIndex[server] {
		rf.mu.Unlock()
		goto start
	} 
	rf.mu.Unlock()
	go rf.Checkmatch()
}

func (rf *Raft) Checkmatch() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	matchcount := make(map[int]int)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.matchIndex[i] > rf.commitIndex {
			matchcount[rf.matchIndex[i]]++
		}
	} 
	maxindex := 0
	// Majority match && match in currentTerm.
	for index, count := range matchcount {
		if count >= len(rf.peers) / 2 && index > maxindex &&
			 rf.log[index - rf.preindex - 1].Term == rf.currentTerm {
			maxindex = index
		} 
	}
	// This is the match index.
	if maxindex != 0 {
		rf.commitIndex = maxindex	
		// send a commit heartbeat.
		Printo(dLog, "S%v as leader commit to index %v\n", rf.me, rf.commitIndex)	
		go rf.RfApplyMsg(rf.commitIndex)
	}
}

func (rf *Raft) HeartBeat() {
	for rf.Isleader() && !rf.Killed() {
		// repeat send heartbeat
		for i := 0; i < len(rf.peers) && !rf.Killed() && rf.Isleader(); i++ {
			if i == rf.me {
				continue
			}
		    // Repeat sending heartbeat till get reply.
			go func(i int, rf *Raft) {
				reply := AppendReply{}
				args := AppendArgs{}
				rf.mu.Lock()
				if rf.nextIndex[i] <= rf.preindex + len(rf.log) {
					rf.mu.Unlock()
					go rf.sendAppendEntries(i)
					return
				}
				args.Leaderid = rf.me
				args.LeaderTerm = rf.currentTerm
				args.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()
				Printo(dLog, "S%v send heartbeat to S%v\n", rf.me, i)
				ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
				if !ok {
					return
				}	
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Repterm > rf.currentTerm {
					// Become follower.
					rf.currentTerm = reply.Repterm
					rf.isleader = false
					rf.votedFor = -1
					rf.persist()
				}
			}(i, rf)
		}
		time.Sleep(150 * time.Millisecond)
	}
	
}

func (rf *Raft) RfApplyMsg(cmit int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.lastApplied < cmit && !rf.Killed() {
		la := rf.lastApplied + 1
		select {
		case rf.apmsg <- ApplyMsg{true, rf.log[la - rf.preindex - 1].Command, 
				la, false, nil, 0, 0}:
				
				rf.lastApplied++
		default:
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
		}
	}
}


// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}


// RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (2A).
	Replyid int
	Term int
	Grant bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Become follower
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.isleader = false
		rf.currentTerm = args.Term
	}

	llog := len(rf.log)
	var LastLogIndex, LastLogTerm int
	if llog != 0 {
		LastLogIndex = rf.preindex + llog
		LastLogTerm = rf.log[llog - 1].Term
	}
	
	if args.Term >= rf.currentTerm && 
	   	( args.LastLogTerm > LastLogTerm ||
			(args.LastLogTerm == LastLogTerm && args.LastLogIndex >= LastLogIndex) ) &&
			 rf.votedFor == -1 {
		Printo(dVote, "S%v in term %v with llt %v grant S%v with llt %v with term %v\n", rf.me, rf.currentTerm, LastLogTerm, args.CandidateId,
	 args.LastLogTerm, args.Term)
		rf.votedFor = args.CandidateId
		reply.Grant = true
		reply.Replyid = rf.me
		rf.isleader = false
		// Reset timeout after granting.
		rf.timer.cond.L.Lock()
		rf.timer.timeout = rand.Int() % 500 + 400
		rf.timer.ticker = 0
		rf.timer.cond.L.Unlock()
		rf.persist()
		return 
	}
	reply.Term = rf.currentTerm
	reply.Grant = false
	rf.persist()
	return 
}


// send a RequestVote RPC to a server.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, gotvote *int) bool {
	// defer wg.Done()
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	// Check reply.
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !reply.Grant {
			if reply.Term > rf.currentTerm {
				// convert to follower.
				rf.currentTerm = reply.Term
				rf.isleader = false
				if rf.votedFor == rf.me {
					rf.votedFor = -1
				} 
				rf.persist()
			}
			return ok
		}
		*gotvote++
		
		// DeBug(dVote, "S%v got %v votes\n", rf.me, *gotvote)
		if *gotvote > len(rf.peers) / 2 && !rf.isleader && rf.votedFor == rf.me {
			// Become leader.
			Printo(dVote, "S%v become leader at term %v\n", rf.me, rf.currentTerm)			
			rf.isleader = true
			// Initialize index.
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.preindex + len(rf.log) + 1 
				rf.matchIndex[i] = 0
			}
			go rf.HeartBeat()
		} 
	}
	return ok
}
 
func (rf *Raft) Election() {
		rf.mu.Lock()
		// Become candidate.
		Printo(dVote, "S%v start election\n", rf.me)
		rf.votedFor = rf.me
		rf.currentTerm++
		gotvote := 1
		llog := len(rf.log)
		var LastLogIndex, LastLogTerm int
		if llog != 0 {
			LastLogIndex = rf.preindex + llog
			LastLogTerm = rf.log[llog-1].Term
		}
		rf.persist()	
		args := RequestVoteArgs{rf.currentTerm, rf.me, LastLogIndex, LastLogTerm}
		for i := 0; i < len(rf.peers) && !rf.Killed(); i++ {
			if i == rf.me {
				continue
			}
			go rf.sendRequestVote(i, &args, &gotvote)
		}
		rf.mu.Unlock()
}
// Ticker routine.Runs all the time.
func (rf *Raft) ticker() {
	rf.timer.cond.L.Lock()
	defer rf.timer.cond.L.Unlock()
	go rf.Interval()
	for !rf.Killed() {
		// Reset timeout, Become candidate start election.
		rf.timer.timeout = rand.Int() % 500 + 400
		Printo(dTimer, "S%v got timeout %v\n", rf.me, rf.timer.timeout)
		rf.timer.ticker = 0
		// Waiting ...
		for !rf.Killed() {
			for	rf.timer.ticker < rf.timer.timeout && !rf.Killed() {
				rf.timer.cond.Wait()
			}
			if !rf.Isleader() {
				// Break start election.
				break
			}
			// Is leader reset timeout, keep waiting.
			rf.timer.timeout = rand.Int() % 500 + 400
			rf.timer.ticker = 0 
		}
		go rf.Election()	 
	} 
}

func (rf *Raft) Interval() {
	for !rf.Killed() {
		rf.timer.cond.L.Lock()
		rf.timer.ticker += 10
		rf.timer.cond.L.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.timer.cond.Signal()
	}
	rf.timer.cond.Signal()
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.timer = Timer{}
	// rf.timer.cond = sync.NewTimer(&rf.timer.mu)
	rf.timer.cond = sync.NewCond(&rf.timer.mu)
	
	rf.matchIndex = make([]int, len(peers))
	
	rf.apmsg = applyCh 
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	lastlogindex := rf.preindex + len(rf.log)
	rf.nextIndex = make([]int, len(peers)) 
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastlogindex + 1
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) Killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.isleader
}

func (rf *Raft) Isleader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.isleader
}

func (rf *Raft) Sameterm(oldterm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return oldterm == rf.currentTerm
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.preindex)
	e.Encode(rf.preterm)
	e.Encode(rf.votedFor)
	for _, log := range rf.log {
		e.Encode(log)
	}
	data := w.Bytes()
	/* llt := rf.preterm
	if len(rf.log) > 0 {
		llt = rf.log[len(rf.log) - 1].Term
	} */
	rf.persister.SaveRaftState(data)
}
//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.votedFor = -1
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.currentTerm = 0
	rf.preindex = 0
	rf.preterm = 0
	rf.votedFor = 0
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.preindex) != nil ||
	  d.Decode(&rf.preterm) != nil || d.Decode(&rf.votedFor) != nil {
		Printo(dDecode,"Decode wrong\n")
	}
	rf.log = make([]Log, 0)
	for {
		var readlog Log
		if d.Decode(&readlog) == io.EOF {
			break
		}
		rf.log = append(rf.log, readlog)
	}
}

