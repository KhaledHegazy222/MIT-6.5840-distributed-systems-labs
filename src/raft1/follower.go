package raft

import (
	"math/rand"
	"time"
)

const (
	HEATBEAT_TIMEOUT_MIN_INTERVAL_MS = 300
	HEATBEAT_TIMEOUT_MAX_INTERVAL_MS = 850
)

func (rf *Raft) checkTimeout() {
	for !rf.killed() { // run till you die
		startElections := false
		timeoutDuration := time.Duration(HEATBEAT_TIMEOUT_MIN_INTERVAL_MS+(rand.Int63()%(HEATBEAT_TIMEOUT_MAX_INTERVAL_MS-HEATBEAT_TIMEOUT_MIN_INTERVAL_MS))) * time.Millisecond
		DPrintf("[%d] my time out is %d", rf.me, timeoutDuration/1000000)
		select {
		case <-rf.resetTimoutCh:
		case <-time.After(timeoutDuration):
			// Promote yourself to candidate
			// if no heartbeat received till it timed out
			rf.mu.Lock()
			if rf.currentRole == RoleFollower {
				rf.currentRole = RoleCandidate
				startElections = true
			}
			rf.mu.Unlock()
		}
		if startElections {
			rf.startElections()
		}
	}
}

func (rf *Raft) convertToFollower() {
	rf.currentRole = RoleFollower
	rf.votedFor = -1
	rf.leaderId = -1
}
