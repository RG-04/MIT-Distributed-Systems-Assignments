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
	"sync"
	"sync/atomic"
	"time"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int
const (
	Init State = iota
	Follower
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state 	 State // current state of the server (follower, candidate, leader)
	applyCh	 chan ApplyMsg // channel to send messages to the service

	// persistent state
	currentTerm int // latest term server has seen
	votedFor    int // candidateId that received vote in current term
	log 	   []LogEntry

	// volatile state (all servers)
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile state (leader)
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// timer
	lastHeartbeat time.Time // time of last heartbeat received

}

// returns the term of the last item in log
func (rf *Raft) lastLogTerm() int {
	// needs lock
	return rf.log[rf.lastLogIndex()].Term
}

// returns the index of the last item in log
func (rf *Raft) lastLogIndex() int {
	// eeds lock
	return len(rf.log) - 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
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
	if len(data) < 1 { // bootstrap without any state?
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


func (rf *Raft) changeState(newState State) {
	// needs lock
	rf.logDebug("%v -> %v", stateName(rf.state), stateName(newState))

	switch newState {
	case Follower:
		if rf.state != newState {
			rf.logDebug("starting election timer")
			go rf.electionTimer()
		}
		rf.state = newState
	case Candidate:
		// begin election
		rf.logDebug("starting election")
		go rf.beginElection()
	case Leader:
		// set up heartbeat loop
		if rf.state != newState {
			rf.logDebug("initializing leader states")
			rf.initLeaderStates()
			rf.logDebug("starting heartbeat timer")
			go rf.sendAppendEntriesTimer()
		}
		rf.state = newState
		// TODO: re-init volatile states for leader
	default:
		rf.logFatal("unexpected new state type: %v", stateName(newState))
	}
}

func (rf *Raft) initLeaderStates() {
	// needs lock
	lastLogIndex := rf.lastLogIndex()
	rf.nextIndex = make([]int, len(rf.peers))
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = lastLogIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchIndex[rf.me] = lastLogIndex
}

func (rf *Raft) applyEntries() {
	// needs lock
	rf.mu.Lock()
	oldLastApplied := rf.lastApplied
	rf.mu.Unlock()
	for {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			break
		}
		rf.lastApplied++
		entry := rf.log[rf.lastApplied]
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
	}
	rf.mu.Lock()
	if rf.lastApplied > oldLastApplied {
		rf.logInfo("applied entries %v to %v", oldLastApplied+1, rf.lastApplied)
	}
	rf.mu.Unlock()
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

	// Your code here (2B).
	rf.mu.Lock()
	index := rf.lastLogIndex() + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		newEntry := LogEntry{
			Term:    term,
			Command: command,
		}
		rf.logInfo("adding new entry to log at index %v: %+v", index, newEntry)
		rf.log = append(rf.log, newEntry)
		oldMatchIndex := rf.matchIndex[rf.me]
		oldNextIndex := rf.nextIndex[rf.me]
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] += 1
		rf.logDebug("matchIndex[%v] %v -> %v", rf.me, oldMatchIndex, rf.matchIndex[rf.me])
		rf.logDebug("nextIndex[%v] %v -> %v", rf.me, oldNextIndex, rf.nextIndex[rf.me])
		rf.advanceLeaderCommitIndex()
	}

	rf.mu.Unlock()
	return index, term, isLeader
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Init
	rf.applyCh = applyCh

	// persistent state
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{})

	// volatile
	rf.commitIndex = 0
	rf.lastApplied = 0

	// init
	rf.mu.Lock()
	rf.changeState(Follower)
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
