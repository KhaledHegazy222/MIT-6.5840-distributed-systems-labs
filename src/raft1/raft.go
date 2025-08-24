package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Role int

const (
	RoleLeader Role = iota
	RoleCandidate
	RoleFollower
)

const (
	HEARTBEAT_INTERVAL_MS  = 20
	APPEND_ENTRIES_TIMEOUT = 50

	HEATBEAT_TIMEOUT_MIN_INTERVAL_MS = 100
	HEATBEAT_TIMEOUT_MAX_INTERVAL_MS = 200

	ELECTION_TIMEOUT_MIN_INTERVAL_MS = 50
	ELECTION_TIMEOUT_MAX_INTERVAL_MS = 500

	REQUEST_VOTE_TIMEOUT = 200
	RPC_RETRY_INTERVAL   = 10
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh       chan raftapi.ApplyMsg
	resetTimoutCh chan struct{}

	// PERSISTED STATE (should be persisted)
	currentTerm int
	votedFor    int // may change
	logs        []Log

	// VOLATILE STATE
	commitIdx   int
	lastApplied int
	leaderId    int
	currentRole Role

	// VOLATILE STATE (for leaders only)
	nextIdx  []int
	matchIdx []int
}

type Log struct {
	Command string
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	isleader := rf.leaderId == rf.me
	// isleader := rf.currentRole == RoleLeader
	return rf.currentTerm, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// reject voting if voted for another this term
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}
	myLastLogTerm := 0
	myLastLogIndex := 0
	if len(rf.logs) > 0 {
		myLastLogIndex = len(rf.logs)
		myLastLogTerm = rf.logs[myLastLogIndex-1].Term
	}

	// grant vote
	if args.LastLogTerm > myLastLogTerm || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex) {

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = args.Term
		return
	}

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
		lastLogIndex = len(rf.logs)
		lastLogTerm = rf.logs[lastLogIndex-1].Term
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
		}
	}
}

func (rf *Raft) startElections() {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		REQUEST_VOTE_TIMEOUT*time.Millisecond,
	)
	defer cancel()

	neededMajority := (len(rf.peers) + 1) / 2
	grantedVotes := 1 // Always grant a vote for yourself

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.mu.Unlock()
	replyCh := make(chan RequestVoteReply)

	for idx := range rf.peers {
		if idx == rf.me {
			// skip RPC call if requesting votes for myself
			continue
		} else {
			go rf.sendRequestVoteToServer(ctx, idx, replyCh)
		}
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case reply := <-replyCh:
				if reply.VoteGranted {
					grantedVotes++
				}
				if grantedVotes == neededMajority {
					DPrintf("MEMBER: %d is now LEADER (%d)", rf.me, grantedVotes)
					rf.mu.Lock()
					rf.currentRole = RoleLeader
					rf.leaderId = rf.me
					rf.mu.Unlock()
					cancel()
				}
			}
		}
	}()

	<-ctx.Done()
	rf.mu.Lock()
	rf.votedFor = -1
	rf.mu.Unlock()
}

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
	rf.mu.RLock()
	currentTerm := rf.currentTerm
	rf.mu.RUnlock()

	if currentTerm <= args.Term {

		// convert to follower and accepts the heartBeat
		rf.mu.Lock()

		rf.currentRole = RoleFollower
		rf.currentTerm = args.Term
		rf.leaderId = args.LeaderId
		rf.votedFor = -1
		rf.mu.Unlock()

		// send reset the timeout signal
		rf.resetTimoutCh <- struct{}{}

		// Prepare Reply
		reply.Success = true
		reply.Term = args.Term
		return

	} else {
		// Rejects the append entries
		reply.Term = currentTerm
		reply.Success = false
		return
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

func (rf *Raft) sendHeartBeatToServer(ctx context.Context, idx int, reachabilityCh chan AppendEntriesReply) {
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		// PrevLogIndex: rf.nextIdx[idx],                 // TODO: CHANGE this if tests failed
		// PrevLogTerm:  rf.logs[rf.nextIdx[idx]-1].term, // TODO: CHANGE this if tests failed
		Entries: []Log{},
		// LeaderCommit: rf.commitIdx, // TODO: CHANGE this if tests failed
	}
	reply := AppendEntriesReply{}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			ok := rf.sendAppendEntries(idx, &args, &reply)
			if !ok {
				time.Sleep(RPC_RETRY_INTERVAL * time.Millisecond)
				continue
			}
			reachabilityCh <- reply
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	ctx, cancel := context.WithTimeout(context.Background(), APPEND_ENTRIES_TIMEOUT*time.Millisecond)
	defer cancel()
	replyCh := make(chan AppendEntriesReply)
	mu := sync.RWMutex{}
	reachable := 1
	majority := (len(rf.peers) + 1) / 2
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.sendHeartBeatToServer(ctx, idx, replyCh)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case reply := <-replyCh:
				if reply.Success {
					mu.Lock()
					reachable++
					mu.Unlock()
				}
			}
		}
	}()
	<-ctx.Done()
	mu.RLock()
	defer mu.RUnlock()
	if reachable < majority {
		// deporomote yourself
		rf.mu.Lock()
		rf.currentRole = RoleFollower
		rf.leaderId = -1
		rf.mu.Unlock()
		DPrintf("I: %d, Done Depromoting to follower", rf.me)
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() { // run till you die
		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.RLock()
		isCandidate := rf.currentRole == RoleCandidate
		rf.mu.RUnlock()

		if isCandidate {
			rf.startElections()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := ELECTION_TIMEOUT_MIN_INTERVAL_MS + (rand.Int63() % (ELECTION_TIMEOUT_MAX_INTERVAL_MS - ELECTION_TIMEOUT_MIN_INTERVAL_MS))
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) checkTimeout() {
	for !rf.killed() { // run till you die
		timeoutDuration := time.Duration(HEATBEAT_TIMEOUT_MIN_INTERVAL_MS+(rand.Int63()%(HEATBEAT_TIMEOUT_MAX_INTERVAL_MS-HEATBEAT_TIMEOUT_MIN_INTERVAL_MS))) * time.Millisecond
		select {
		case <-rf.resetTimoutCh:
		case <-time.After(timeoutDuration):
			// Promote yourself to candidate
			// if no heartbeat received till it timed out
			rf.mu.Lock()
			if rf.currentRole == RoleFollower {
				rf.currentRole = RoleCandidate
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		applyCh:       applyCh,
		resetTimoutCh: make(chan struct{}),
		currentTerm:   0, // TODO: Change it to read from persister storage instead
		votedFor:      -1,
		logs:          []Log{}, // logs start by 1 as index
		commitIdx:     0,
		lastApplied:   0,
		currentRole:   RoleFollower,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeat()
	go rf.checkTimeout()

	return rf
}
