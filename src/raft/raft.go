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
	//	"bytes"
	"sync"
	"sync/atomic"
	"math/rand"
	//	"6.824/labgob"
	"6.824/labrpc"
	"time"
	//	"fmt"
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
	log []Log

	// Volatile

	commitIndex int
	lastApplied int

	// Volatile && For-Leader use

	// Next index to send to the followers.
	nextIndex []int 
	// Highest index known to the leader.	
	matchIndex []int
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
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

func (rf *Raft) Isleader() (ret bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.isleader
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

// For AppendEntries rpc.

type AppendArgs struct {
	Leaderid   int
	LeaderTerm int
}

type AppendReply struct {
	Repterm int
}

func (rf *Raft) HeartBeat() {
	args := AppendArgs{}
	rf.mu.Lock()
	args.Leaderid = rf.me
	args.LeaderTerm = rf.currentTerm
	rf.mu.Unlock()
	for rf.Isleader() && !rf.killed() {
		// repeat send heartbeat
		for i := 0; i < len(rf.peers) && !rf.killed() && rf.Isleader(); i++ {
			if i == rf.me {
				continue
			}
			go func(i int, rf *Raft, args *AppendArgs){
				reply := AppendReply{}
				ok := rf.peers[i].Call("Raft.AppendEntries", args, &reply)
				for !ok && !rf.killed() && rf.Isleader() {
					ok = rf.peers[i].Call("Raft.AppendEntries", args, &reply)
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Repterm > rf.currentTerm {
					// Become follower.
					rf.currentTerm = reply.Repterm
					rf.isleader = false
					rf.votedFor = -1
				}
			}(i, rf, &args)
		}
		time.Sleep(100 * time.Millisecond)
	}
	
}

func (rf *Raft) AppendEntries(args *AppendArgs, reply *AppendReply) {
	// reset election timeout.
	rf.timer.cond.L.Lock()
	rf.timer.ticker = 0
	rf.timer.timeout = rand.Int() % 300 + 500
	rf.timer.cond.L.Unlock()

	// Check leader term.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.LeaderTerm >= rf.currentTerm {
		rf.currentTerm = args.LeaderTerm
		// If candidates Become follower
		if rf.votedFor == rf.me {
			rf.votedFor = -1
			rf.isleader = false
		}
	}
	reply.Repterm = rf.currentTerm
	return 
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

	if rf.votedFor != rf.me && !rf.isleader {
		// Reset timeout.
		rf.timer.cond.L.Lock()
		rf.timer.timeout = rand.Int() % 300 + 500
		rf.timer.ticker = 0
		rf.timer.cond.L.Unlock()
	}
	llog := len(rf.log)
	var LastLogIndex, LastLogTerm int
	if llog != 0 {
		LastLogIndex = rf.preindex + llog
		LastLogTerm = rf.log[llog-1].Term
	}

	if args.Term >= rf.currentTerm && 
	   	args.LastLogTerm >= LastLogTerm && 
		args.LastLogIndex >= LastLogIndex && rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.Grant = true
		reply.Replyid = rf.me
		return 
	}
	reply.Term = rf.currentTerm
	reply.Grant = false
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
			}
			return ok
		}
		*gotvote++
		// DeBug(dVote, "S%v got %v votes\n", rf.me, *gotvote)
		if *gotvote > len(rf.peers) / 2 && !rf.isleader && rf.votedFor == rf.me {
			// Become leader.
			rf.isleader = true
			go rf.HeartBeat()
		} 
	}
	return ok
}
 
func Election(rf *Raft) {
		rf.mu.Lock()
		// Become candidate.
		rf.votedFor = rf.me
		rf.currentTerm++
		gotvote := 1
		llog := len(rf.log)
		var LastLogIndex, LastLogTerm int
		if llog != 0 {
			LastLogIndex = rf.preindex + llog
			LastLogTerm = rf.log[llog-1].Term
		}
		args := RequestVoteArgs{rf.currentTerm, rf.me, LastLogIndex, LastLogTerm}
		for i := 0; i < len(rf.peers) && !rf.killed(); i++ {
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
	go Interval(rf)
	for !rf.killed() {
		// Reset timeout, Become candidate start election.
		rf.timer.timeout = rand.Int() % 500 + 300
		rf.timer.ticker = 0
		go Election(rf)	 
		// Waiting ...
		for !rf.killed() {
			for	rf.timer.ticker < rf.timer.timeout && !rf.killed() {
				rf.timer.cond.Wait()
			}
			if !rf.Isleader() {
				// Break start election.
				break
			}
			// Is leader reset timeout, keep waiting.
			rf.timer.timeout = rand.Int() % 300 + 500
			rf.timer.ticker = 0 
		}
	} 
}

func Interval(rf *Raft) {
	for !rf.killed() {
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
	rf.votedFor = -1
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
