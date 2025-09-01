package raft

import "time"

const HEARTBEAT_INTERVAL_MS = 50

func (rf *Raft) heartBeat() {
	for !rf.killed() { // run till you die
		rf.mu.RLock()
		myRole := rf.currentRole
		rf.mu.RUnlock()

		if myRole == RoleLeader {
			rf.sendHeartBeat()
		}
		time.Sleep(time.Millisecond * HEARTBEAT_INTERVAL_MS)
	}
}

// question: do I have to cancel some running heartBeat ?
// if I received an appendEntries Call with higher term
// or votes for higher term ?
// if yes, then how to cancel it

func (rf *Raft) convertToLeader() {
	rf.leaderId = rf.me
	rf.currentRole = RoleLeader
	rf.votedFor = rf.me
	for idx := range rf.peers {
		rf.nextIdx[idx] = len(rf.logs)
		rf.matchIdx[idx] = 0
	}
}
