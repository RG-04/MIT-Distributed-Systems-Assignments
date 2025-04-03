package raft

import (
	"time"
)

func (rf *Raft) sendAppendEntriesTimer() {
	for {
		rf.mu.Lock()
		if rf.state != Leader || rf.killed() {
			rf.mu.Unlock()
			return
		}

		rf.logDebug("heartbeat")
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int, term int) {
				for {
					rf.mu.Lock()
					if rf.currentTerm > term {
						rf.mu.Unlock()
						return
					}
					args := rf.makeAppendEntriesArgs(server)
					rf.mu.Unlock()
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, &args, &reply)
					rf.mu.Lock()
					if !ok {
						rf.mu.Unlock()
						return
					}
					if rf.currentTerm > term {
						rf.mu.Unlock()
						return
					}

					if reply.Term > rf.currentTerm {
						rf.logDebug("received AppendEntries reply with higher term %v, "+
							"converting to follower and aborting", reply.Term)
						rf.changeState(Follower)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.mu.Unlock()
						return
					}
					if reply.Success {
						oldMatchIndex := rf.matchIndex[server]
						oldNextIndex := rf.nextIndex[server]
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						if len(args.Entries) > 0 {
							rf.logDebug("matchIndex[%v] %v -> %v", server, oldMatchIndex, rf.matchIndex[server])
							rf.logDebug("nextIndex[%v] %v -> %v", server, oldNextIndex, rf.nextIndex[server])
						}
						rf.advanceLeaderCommitIndex()
						rf.mu.Unlock()
						return
					}

					// handle conflict
					newNextIndex := rf.nextIndex[server]
					for newNextIndex > 0 {
						nextNextIndex := newNextIndex - 1
						if nextNextIndex == 0 {
							// no entry with same term
							newNextIndex = reply.ConflictIndex
							break
						}

						if rf.log[nextNextIndex].Term == reply.ConflictTerm {
							// found entry with same term
							break
						}
						newNextIndex = nextNextIndex
					}
					
					rf.nextIndex[server] = newNextIndex
					rf.logDebug("AppendEntries failed for s%v, back up nextIndex to %v and retrying",
						server, rf.nextIndex[server])
					rf.mu.Unlock()
				}
			}(server, rf.currentTerm)
		}
		rf.mu.Unlock()
		time.Sleep(HeartbeatTimeout)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) makeAppendEntriesArgs(server int) AppendEntriesArgs {
	// needs log
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	lastLogIndex, lastLogTerm := rf.lastLogIndex(), rf.lastLogTerm()
	nextIndex := rf.nextIndex[server]
	if nextIndex > lastLogIndex {
		// peer has everything we have in log, no new entries to send
		args.PrevLogIndex = lastLogIndex
		args.PrevLogTerm = lastLogTerm
		return args
	}
	args.Entries = make([]LogEntry, len(rf.log[nextIndex:]))
	copy(args.Entries, rf.log[nextIndex:])
	args.PrevLogIndex = nextIndex - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	return args
}

func (rf *Raft) advanceLeaderCommitIndex() {
	// needs lock
	if rf.state != Leader {
		rf.logFatal("trying to advance commitIndex as non-leader")
	}

	newCommitIndex := rf.commitIndex
	for newCommitIndex < rf.lastLogIndex() {
		nextCommitIndex := newCommitIndex + 1
		matched := 0 // count how many servers have this index
		for server := range rf.peers {
			if rf.matchIndex[server] >= nextCommitIndex {
				matched++
			}
		}

		if matched*2 > len(rf.peers) {
			newCommitIndex = nextCommitIndex
		} else {
			break
		}
	}

	if newCommitIndex != rf.commitIndex {
		rf.logInfo("commitIndex %v -> %v", rf.commitIndex, newCommitIndex)
	}
	rf.commitIndex = newCommitIndex
	go rf.applyEntries()
}