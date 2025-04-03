package raft

import "time"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logDebug("receives RequestVote for term %v from s%v: %+v", args.Term, args.CandidateId, args)

	// check if we need to update our term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeState(Follower)
	}
	
	// check if we should vote for this candidate
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	reply.Term = rf.currentTerm

	// if we haven't voted or we've voted for this candidate
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// check if candidate's log is at least as up-to-date as receiver's log
		rf.votedFor = args.CandidateId
		if args.LastLogTerm > rf.lastLogTerm() ||
			(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) {
			reply.VoteGranted = true
			rf.logDebug("votes YES to s%v", args.CandidateId)
		} else {
			reply.VoteGranted = false
			rf.logDebug("votes NO to s%v, candidate's log is stale", args.CandidateId)
		}
			
	} else {
		rf.logDebug("votes NO to s%v, already voted to s%v", args.CandidateId, rf.votedFor)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// from extended Raft:
	// If desired, the protocol can be optimized to reduce the
	// number of rejected AppendEntries RPCs. For example,
	// when rejecting an AppendEntries request, the follower
	// can include the term of the conflicting entry and the first
	// index it stores for that term. With this information, the
	// leader can decrement nextIndex to bypass all of the conflicting
	// entries in that term; one AppendEntries RPC will
	// be required for each term with conflicting entries, rather
	// than one RPC per entry. In practice, we doubt this optimization
	// is necessary, since failures happen infrequently
	// and it is unlikely that there will be many inconsistent entries.
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logDebug("receives AppendEntries with term %v from s%v: %+v", args.Term, args.LeaderId, args)

	reply.Term = rf.currentTerm

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		rf.logDebug("rejects expired AppendEntries from s%v", args.LeaderId)
		reply.Success = false
		return
	}

	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.logDebug("prev log term mismatch at index %v, rejecting AppendEntries", args.PrevLogIndex)
		reply.Success = false

		if args.PrevLogIndex < len(rf.log) {
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			// find the first index with this term
			for i := args.PrevLogIndex; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					reply.ConflictIndex = i
					break
				}
			}
		} else {
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1
		}

		rf.logDebug("conflictIndex = %v, conflictTerm = %v", reply.ConflictIndex, reply.ConflictTerm)
		return
	}

	rf.lastHeartbeat = time.Now()

	reply.Success = true
	lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)

	// if an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it

	for idx, entry := range args.Entries {
		logIndex := args.PrevLogIndex + idx + 1
		if logIndex >= len(rf.log) {
			break
		}

		if rf.log[logIndex].Term != entry.Term {
			rf.logDebug("log entry mismatch at index %v", logIndex)
			rf.logDebug("removing log entries starting from %v", logIndex)
			rf.log = rf.log[:logIndex]
			break
		}
	}

	// if we didn't remove any entry in the previous step,
	// there might be things after lastNewEntryIndex already in our log,
	// save them to append them later
	var moreEntries []LogEntry
	if lastNewEntryIndex + 1 < len(rf.log) {
		moreEntries = rf.log[lastNewEntryIndex:]
	}

	// append new entries to our log
	rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)

	// append the rest of the log entries we saved before
	rf.log = append(rf.log, moreEntries...)

	rf.logDebug("updated log: %+v", rf.log)

	// if leaderCommit > commitIndex, set commitIndex to min(leaderCommit, last log index)
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		if args.LeaderCommit < rf.lastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
		rf.logDebug("commitIndex[%v] %v -> %v", rf.me, oldCommitIndex, rf.commitIndex)
		go rf.applyEntries()
	}

	// recognize that we are a follower
	if rf.state != Follower {
		rf.logDebug("receives AppendEntries from s%v, converting to follower for term %v",
			args.LeaderId, args.Term)
		rf.changeState(Follower)
	}

	rf.currentTerm = args.Term
	rf.logDebug("AppendEntries from s%v successful", args.LeaderId)
}