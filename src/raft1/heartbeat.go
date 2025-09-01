package raft

import (
	"context"
	"math"
	"time"

	"6.5840/raftapi"
)

const APPEND_ENTRIES_TIMEOUT = 50

// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int
	Success bool
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	// if received a call with the same term of more, convert to follower right now

	rf.mu.Lock()
	if rf.currentTerm < args.Term {

		// convert to follower and accepts the heartBeat
		rf.convertToFollower()
		DPrintf("[%d] SETTING CURRENT TERM TO: %d", rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.mu.Unlock()

		// send reset the timeout signal
		rf.resetTimoutCh <- struct{}{}

		rf.syncWithLeader(args, reply)
		return

	} else if rf.currentTerm == args.Term {

		DPrintf("[%d] normal heartbeat, term: %d", rf.me, args.Term)
		rf.mu.Unlock()

		// send reset the timeout signal
		rf.resetTimoutCh <- struct{}{}

		rf.syncWithLeader(args, reply)

	} else {

		// Rejects the append entries
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		DPrintf("[%d] I reject new logs (stale leader)", rf.me)
		return
	}
}

func (rf *Raft) syncWithLeader(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.PrevLogIndex >= len(rf.logs) {
		DPrintf("[%d] ACCESSING %d of %d", rf.me, args.PrevLogIndex, len(rf.logs))
		// don't match return error
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// don't match return error
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("[%d] I reject new logs (conflict) prev idx: [%d] prev term: [%d] current term: %d", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.logs[args.PrevLogIndex].Term)
	} else {
		// accept the logs
		// clear up all the next logs
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)

		if rf.commitIdx < args.LeaderCommit {
			newCommitIdx := int(math.Min(float64(len(rf.logs)-1), float64(args.LeaderCommit)))
			for rf.commitIdx < newCommitIdx {
				rf.commitIdx++
				DPrintf("[%d] FOLLOWER COMMITTING: %d, of total: %d", rf.me, rf.commitIdx, len(rf.logs))
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.commitIdx].Command,
					CommandIndex: rf.commitIdx,
				}
			}
		}

		reply.Success = true
		reply.Term = args.Term
	}
}

// example code to send a AppendEntries RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeatToServer(ctx context.Context, idx int, nextBatchIdx int, replyCh chan AppendEntriesReply, cancel context.CancelFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			rf.mu.RLock()
			prevIdx := rf.nextIdx[idx] - 1 // TODO: CHANGE this if tests failed
			if prevIdx < 0 {
				DPrintf("[%d] ACCESSING NEGATIVE INDEX %v", rf.me, rf.nextIdx)
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevIdx,
				PrevLogTerm:  rf.logs[prevIdx].Term,
				Entries:      rf.logs[prevIdx+1 : nextBatchIdx+1],
				LeaderCommit: rf.commitIdx,
			}
			rf.mu.RUnlock()

			reply := AppendEntriesReply{}

			DPrintf("[%d] Sending Heart Beat to %d, waiting", rf.me, idx)

			ok := rf.sendAppendEntries(idx, &args, &reply)
			if !ok {
				time.Sleep(RPC_RETRY_INTERVAL * time.Millisecond)
				DPrintf("[%d] Sending Heart Beat to %d, failed", rf.me, idx)
				continue
			}
			DPrintf("[%d] Sending Heart Beat to %d, done", rf.me, idx)

			rf.mu.Lock()
			if rf.currentRole == RoleFollower {
				rf.mu.Unlock()
				cancel()
				return
			}
			if reply.Success {
				rf.nextIdx[idx] = prevIdx + len(args.Entries) + 1

				rf.mu.Unlock()
				replyCh <- reply
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.convertToFollower()
				cancel()
				DPrintf("[%d] Done Depromoting to follower", rf.me)
			} else {
				if rf.nextIdx[idx] == len(rf.logs) {
					DPrintf("[%d] Dropping next idx: %d, from peer: %d", rf.me, rf.nextIdx[idx], idx)
					rf.nextIdx[idx]--
				}
				droppedTerm := rf.logs[rf.nextIdx[idx]].Term
				for rf.nextIdx[idx] > 1 {
					if rf.logs[rf.nextIdx[idx]].Term == droppedTerm {
						DPrintf("[%d] Dropping next idx: %d, from peer: %d", rf.me, rf.nextIdx[idx], idx)
						rf.nextIdx[idx]--
					} else {
						break
					}
				}
				DPrintf("[%d] FAILED TO PUSH TO: %d, retrying with %d", rf.me, idx, rf.nextIdx[idx])
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	ctx, cancel := context.WithTimeout(context.Background(), APPEND_ENTRIES_TIMEOUT*time.Millisecond)
	defer cancel()
	DPrintf("[%d] Sending Heart Beat, waiting", rf.me)
	rf.mu.RLock()
	DPrintf("[%d] Sending Heart Beat, done", rf.me)

	recievedReplies := 1

	nextBatchIdx := len(rf.logs) - 1
	nextBatch := []Log{}
	if rf.commitIdx < nextBatchIdx {
		nextBatch = rf.logs[rf.commitIdx+1:]
	}
	rf.mu.RUnlock()
	replyCh := make(chan AppendEntriesReply)
	reachable := 1
	majority := (len(rf.peers) + 1) / 2
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.sendHeartBeatToServer(ctx, idx, nextBatchIdx, replyCh, cancel)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				DPrintf("[%d] heartbeat timeout", rf.me)
				return
			case reply := <-replyCh:
				rf.mu.Lock()
				if reply.Success {
					reachable++
					if reachable == len(rf.peers) {
						cancel()
						rf.mu.Unlock()
						return
					}

				} else if reply.Term > rf.currentTerm {
					reachable = 0
					rf.mu.Unlock()
					cancel()
					return
				}
				recievedReplies++
				if recievedReplies == len(rf.peers) {
					DPrintf("[%d] finshed heartbeat on time", rf.me)
					rf.mu.Unlock()
					cancel()
					return

				}
				rf.mu.Unlock()
			}
		}
	}()
	<-ctx.Done()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reachable >= majority {
		rf.acceptedLeader = true
		for _, b := range nextBatch {
			rf.commitIdx++
			DPrintf("[%d] Committing %d", rf.me, rf.commitIdx)
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.commitIdx,
				Command:      b.Command,
			}
		}
	} else {
		// deporomote yourself
		rf.convertToFollower()
		DPrintf("[%d] Done Depromoting to follower, reachability: %d", rf.me, reachable)
	}
}
