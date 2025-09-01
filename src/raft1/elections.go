package raft

import (
	"context"
	"time"
)

const ELECTIONS_TIMEOUT = 50

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// TODO: REVIEW THIS FUNCTION
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		DPrintf("[%d] REJECTING VOTES MY term is %d, his (%d) is %d ", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		DPrintf("[%d] I Should Convert to follower when asked from votes (%d) new Term is (%d)", rf.me, args.CandidateId, args.Term)
		rf.convertToFollower()
		rf.currentTerm = args.Term
		DPrintf("[%d] SETTING CURRENT TERM TO: %d", rf.me, args.Term)
	}

	// reject voting if voted for another this term
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("[%d] Already Voted to %d, rejecting: %d", rf.me, rf.votedFor, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	myLastLogTerm := 0
	myLastLogIndex := 0
	if len(rf.logs) > 0 {
		myLastLogIndex = len(rf.logs) - 1
		myLastLogTerm = rf.logs[myLastLogIndex].Term
	}

	// grant vote
	if args.LastLogTerm > myLastLogTerm || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex) {

		DPrintf("[%d] ACCEPTING VOTES MY LASTs (%d,%d), HIS (%d) LASTS (%d,%d)", rf.me, myLastLogIndex, myLastLogTerm, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = args.Term
		return
	}

	DPrintf("[%d] REJECTING VOTES MY LASTs (%d,%d), HIS (%d) LASTS (%d,%d)", rf.me, myLastLogIndex, myLastLogTerm, args.CandidateId, args.LastLogIndex, args.LastLogTerm)

	reply.VoteGranted = false
	reply.Term = rf.currentTerm // TODO: this might change later
}

// example code to send a RequestVote RPC to a server.
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestVoteToServer(ctx context.Context, idx int, ch chan RequestVoteReply) {
	lastLogIndex := 0 // zero means no logs appended before
	lastLogTerm := 0

	rf.mu.RLock()
	if len(rf.logs) > 0 {
		lastLogIndex = len(rf.logs) - 1
		lastLogTerm = rf.logs[lastLogIndex].Term
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.RUnlock()

	reply := RequestVoteReply{}
	for {
		select {
		case <-ctx.Done():
			// stop retrying if context is cancelled or times out
			return
		default:
			ok := rf.sendRequestVote(idx, &args, &reply)
			if !ok {
				time.Sleep(time.Millisecond * RPC_RETRY_INTERVAL)
				continue
			}
			ch <- reply
			return
		}
	}
}

func (rf *Raft) startElections() {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		ELECTIONS_TIMEOUT*time.Millisecond,
	)
	defer cancel()

	neededMajority := (len(rf.peers) + 1) / 2
	recievedReplies := 1
	grantedVotes := 1 // Always grant a vote for yourself

	rf.mu.Lock()
	rf.acceptedLeader = false
	rf.currentTerm++
	rf.votedFor = rf.me
	DPrintf("[%d] NEW ELECTIONS, Term: %d", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	replyCh := make(chan RequestVoteReply)

	for idx := range rf.peers {
		if idx == rf.me {
			// skip RPC call if requesting votes for myself
			continue
		}
		go rf.sendRequestVoteToServer(ctx, idx, replyCh)

	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				DPrintf("[%d] ELECTION Timeout", rf.me)
				return
			case reply := <-replyCh:
				rf.mu.Lock()
				if reply.VoteGranted {
					grantedVotes++
					if grantedVotes >= neededMajority {
						cancel()
						rf.mu.Unlock()
						return
					}
				} else if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					grantedVotes = 0
					rf.mu.Unlock()
					cancel()
					return
					// rf.convertToFollower()
				}
				recievedReplies++
				if recievedReplies == len(rf.peers) {
					DPrintf("[%d] FINISH ELECTIONS ON TIME", rf.me)
					rf.mu.Unlock()
					cancel()
					return
				}

				rf.mu.Unlock()

				// if grantedVotes == neededMajority {
				// 	DPrintf("[%d] I am LEADER with (%d votes)", rf.me, grantedVotes)
				// 	if rf.currentRole == RoleFollower {
				// 		DPrintf("[%d] RAREEEEEEEEEEE", rf.me)
				// 		cancel()
				// 		rf.mu.Unlock()
				// 		return
				// 	}
				// 	rf.convertToLeader()
				// 	cancel()
				// }
			}
		}
	}()

	<-ctx.Done()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if I am still a candidate (didn't skip the vote)
	if rf.currentRole == RoleCandidate && grantedVotes >= neededMajority {
		for idx := range rf.peers {
			rf.nextIdx[idx] = len(rf.logs)
			rf.matchIdx[idx] = 0
		}

		rf.convertToLeader()
		DPrintf("[%d] Success I am now leader with these votes: %d", rf.me, grantedVotes)
	} else {
		rf.convertToFollower()
		DPrintf("[%d] FAILED, only got this votes: %d", rf.me, grantedVotes)
	}
	// rf.resetTimoutCh <- struct{}{}
}
