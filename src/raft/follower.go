package raft

import "time"

func (rf *Raft) electionTimer() {
	for {
		timeoutDuration := rf.randomElectionTimeout()
		time.Sleep(timeoutDuration)
		rf.mu.Lock()
		// if no longer follower, cancel timer
		if rf.state != Follower || rf.killed() {
			rf.mu.Unlock()
			return
		}
		// if no heartbeat received, stop looping, begin election
		if time.Since(rf.lastHeartbeat) > timeoutDuration {
			rf.logDebug("election timer timeout")
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	rf.changeState(Candidate)
	rf.mu.Unlock()
}
