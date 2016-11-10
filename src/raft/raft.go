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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "encoding/gob"

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func getRandomElectionTimeout() int64 {
	return 150 + rand.Int63n(150)
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type NodeState int

const (
	LEADER = NodeState(iota)
	FOLLOWER
	CANDIDATE
)

const RPC_TIMEOUT = 200     // 200 milliseconds
const REPLICATE_TIMEOUT = 5 // 5 seconds

func (state NodeState) String() string {
	switch state {
	case LEADER:
		return "leader"
	case FOLLOWER:
		return "follower"
	case CANDIDATE:
		return "candidate"
	default:
		return "unkown"
	}

}

const VOTENULL = -1

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	log         []*LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state             NodeState
	electionTimeoutMs int64
	timer             *time.Timer

	heartbeatTimeoutMs int64

	numAliveNodes int

	applyCh chan ApplyMsg

	startMu sync.Mutex
}

func (rf *Raft) numLogs() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := len(rf.log) - 1
	if lastLogIndex == 0 {
		return -1
	} else {
		return rf.log[lastLogIndex].Term
	}
}

// only valid for leader
func (rf *Raft) getPrevLogIndex(idx int) int {
	return rf.nextIndex[idx] - 1
}

func (rf *Raft) getPrevLogTerm(idx int) int {
	prevLogIndex := rf.nextIndex[idx] - 1
	// DPrintf("prevLogIndex: %s", prevLogIndex)
	if prevLogIndex == 0 {
		return -1
	} else {
		return rf.log[prevLogIndex].Term
	}
}

func (rf *Raft) applyLogs() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		entry := rf.log[rf.lastApplied]
		msg := ApplyMsg{
			Index:   entry.Index,
			Command: entry.Command,
		}
		// can't use new goroutine to send the message
		// because the order will be shuffled
		rf.applyCh <- msg
	}
}

func (rf *Raft) initializeTimer() {
	rf.electionTimeoutMs = getRandomElectionTimeout()
	DPrintf("Server(%v) election timeout %v ms", rf.me, rf.electionTimeoutMs)

	d := time.Duration(rf.electionTimeoutMs) * time.Millisecond
	rf.timer = time.NewTimer(d)
}

func (rf *Raft) reinitializeTimer() {
	rf.electionTimeoutMs = getRandomElectionTimeout()
	DPrintf("Reinitialize Server(%v) election timeout to %v ms", rf.me, rf.electionTimeoutMs)

	// rf.timer = time.NewTimer(time.Duration(rf.electionTimeoutMs) * time.Millisecond)
	rf.updateTimer()
}

func (rf *Raft) updateTimer() {
	if !rf.timer.Stop() {
		select {
		case <-rf.timer.C:
		default:
		}
	}
	d := time.Duration(rf.electionTimeoutMs) * time.Millisecond
	rf.timer.Reset(d)
}

func (rf *Raft) disableTimer() {
	if !rf.timer.Stop() {
		select {
		case <-rf.timer.C:
		default:
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	if rf.numAliveNodes <= len(rf.peers)/2 {
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	// DPrintf("Persister data: %v", data)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []*LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// all server 1
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term, args.LeaderId)
	}
	if args.Term == rf.currentTerm && rf.state == CANDIDATE {
		rf.convertToFollower(args.Term, args.LeaderId)
	}
	// 2
	lastLogIndex := rf.getLastLogIndex()
	// lastLogTerm := rf.getLastLogTerm()
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.PrevLogIndex > 0 {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}
	}
	if len(args.Entries) > 0 {
		// 3, 4 overwrite conflict entry and append new entries
		for i := 0; i < len(args.Entries); i++ {
			idx1 := args.PrevLogIndex + 1 + i
			idx2 := i
			if idx1 > lastLogIndex {
				remainEntries := args.Entries[idx2:]
				rf.log = append(rf.log, remainEntries...)
				break
			}
			entry1 := rf.log[idx1]
			entry2 := args.Entries[idx2]
			if entry1.Term != entry2.Term {
				rf.log[idx1] = entry2
			}

		}
		// // 3
		// rf.log = rf.log[:args.PrevLogIndex+1]
		// // 4
		// if args.Entries != nil {
		// 	rf.log = append(rf.log, args.Entries...)
		// }
	}
	// 5
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("Server %v Update commitid %v => %v, last applied %v", rf.me, rf.commitIndex, args.LeaderCommit, rf.lastApplied)
		minIndex := args.LeaderCommit
		if minIndex > rf.getLastLogIndex() {
			minIndex = rf.getLastLogIndex()
		}
		rf.commitIndex = minIndex
	}
	// all server 1
	rf.applyLogs()

	rf.updateTimer()
	reply.Success = true
	reply.Term = rf.currentTerm
	return
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply, timeoutMs int64) bool {
	if timeoutMs == 0 {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		return ok
	} else {
		retChan := make(chan bool, 1)
		go func(reply *AppendEntriesReply, retChan chan bool) {
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			retChan <- ok
		}(reply, retChan)

		select {
		case ok := <-retChan:
			return ok
		case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
			return false
		}
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// all server 1
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term, args.CandidateId)
	}
	// 2
	if rf.voteFor != VOTENULL && rf.voteFor != args.CandidateId {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		DPrintf("Server %v's log is older than server %v, reject grant.", args.CandidateId, rf.me)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	DPrintf("%v vote %v term %v", rf.me, args.CandidateId, rf.currentTerm)
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply, timeoutMs int64) bool {
	if timeoutMs == 0 {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		return ok
	} else {
		retChan := make(chan bool, 1)
		go func(reply *RequestVoteReply, retChan chan bool) {
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			retChan <- ok
		}(reply, retChan)

		select {
		case ok := <-retChan:
			return ok
		case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
			return false
		}
	}
}

func (rf *Raft) replicateLog() bool {
	t0 := time.Now()

	rf.mu.Lock()
	totalServers := len(rf.peers)
	me := rf.me
	rf.mu.Unlock()

	retChan := make(chan bool, len(rf.peers))
	for i := 0; i < totalServers; i++ {
		if i == me {
			continue
		}
		go func(idx int) {
			for {
				//timeout
				if time.Since(t0).Seconds() >= REPLICATE_TIMEOUT {
					retChan <- false
					return
				}

				rf.mu.Lock()
				nextIdx := rf.nextIndex[idx]
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,

					PrevLogIndex: rf.getPrevLogIndex(idx),
					PrevLogTerm:  rf.getPrevLogTerm(idx),
					Entries:      rf.log[nextIdx:],

					LeaderCommit: rf.commitIndex,
				}
				// DPrintf("server %v args: %v", idx, args)
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(idx, args, reply, RPC_TIMEOUT)

				rf.mu.Lock()

				if rf.state != LEADER || rf.currentTerm != args.Term {
					rf.mu.Unlock()

					retChan <- false
					return
				}

				if ok == true {
					// DPrintf("Success: %v", reply.Success)
					if reply.Term > rf.currentTerm {
						// convert to follower
						rf.convertToFollower(reply.Term, idx)
						rf.mu.Unlock()

						retChan <- false
						return
					} else {
						if reply.Success == false {
							rf.nextIndex[idx]--
						} else {
							rf.nextIndex[idx] = nextIdx + len(args.Entries)
							rf.matchIndex[idx] = nextIdx + len(args.Entries) - 1
							rf.mu.Unlock()

							retChan <- true
							return
						}
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}

	numSuccess := 0
	numExit := 0
	for status := range retChan {
		numExit++
		if status == true {
			numSuccess++
		}
		if numSuccess >= totalServers/2 {
			return true
		}
		if numExit == totalServers-1 {
			break
		}
	}
	return false
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// rf.startMu.Lock()
	// defer rf.startMu.Unlock()

	term, isLeader := rf.GetState()

	rf.mu.Lock()
	index := rf.getLastLogIndex() + 1

	if isLeader == false {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	// append log to log slice
	entry := &LogEntry{
		Term:    rf.currentTerm,
		Index:   rf.getLastLogIndex() + 1,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.mu.Unlock()

	DPrintf("Server %v Start command %v index %v, entry: %v", rf.me, command, index, entry)
	if rf.replicateLog() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.commitIndex < entry.Index {
			rf.commitIndex = entry.Index

			rf.applyLogs()
		}
	}
	// else {
	// 	index = -1
	// }
	DPrintf("Server %v finished command %v Index: %v, Term :%v", rf.me, command, index, term)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) convertToFollower(term int, voteFor int) {
	DPrintf("Convert server %v's state(%v => follower) Term(%v => %v)", rf.me, rf.state.String(), rf.currentTerm, term)

	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.voteFor = voteFor
	// rf.updateTimer()
	rf.reinitializeTimer()
	go rf.leaderElection()
}

func (rf *Raft) convertToLeader() {
	DPrintf("Convert server %v's state(%v => leader) Term(%v)", rf.me, rf.state.String(), rf.currentTerm)

	rf.state = LEADER
	rf.voteFor = rf.me // prevent RequestVote to leader for this term
	rf.disableTimer()
	rf.numAliveNodes = len(rf.peers)

	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		quitChan := make(chan bool)

		go func(rf *Raft, idx int) {
			isAlive := true

			ticker := time.NewTicker(time.Duration(rf.heartbeatTimeoutMs) * time.Millisecond)
			for range ticker.C {
				select {
				case <-quitChan:
					return
				default:
				}

				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					return
				}
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,

					PrevLogIndex: rf.getPrevLogIndex(idx),
					PrevLogTerm:  rf.getPrevLogTerm(idx),
					Entries:      nil,

					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}

				// DPrintf("send AppendEntries(%v => %v)", rf.me, idx)
				// startTime := time.Now()
				ret := rf.sendAppendEntries(idx, args, reply, RPC_TIMEOUT)
				// elapsed := time.Since(startTime)
				// DPrintf("send heart beat(%v => %v) took %s", rf.me, idx, elapsed)

				rf.mu.Lock()

				if ret == true {
					// DPrintf("heart beat (%v => %v) success", rf.me, idx)
					if rf.state != LEADER {
						rf.mu.Unlock()
						return
					}

					if isAlive == false {
						isAlive = true
						rf.numAliveNodes++
					}

					if reply.Term > rf.currentTerm {
						// convert to follower
						rf.convertToFollower(reply.Term, VOTENULL)
						close(quitChan)
					} else {
						if reply.Success == false {
							rf.nextIndex[idx]--
						}
					}
				} else {
					// DPrintf("AppendEntries(%v => %v) fails", rf.me, idx)
					if isAlive == true {
						isAlive = false
						rf.numAliveNodes--
					}
				}
				rf.mu.Unlock()
			}
		}(rf, i)
	}
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	prevTerm := rf.currentTerm - 1
	rf.mu.Unlock()

	var needExit int32 = 0
	for {
		if atomic.LoadInt32(&needExit) == 1 {
			break
		}
		rf.mu.Lock()
		if prevTerm < rf.currentTerm-1 {
			rf.mu.Unlock()
			break
		}
		if rf.state == LEADER {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		<-rf.timer.C
		DPrintf("Server(%v) expire, term %v", rf.me, rf.currentTerm)

		rf.mu.Lock()
		prevTerm = rf.currentTerm

		// rf.updateTimer()
		rf.reinitializeTimer()
		rf.state = CANDIDATE
		rf.currentTerm++
		rf.voteFor = rf.me

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.getLastLogIndex(),
			LastLogTerm:  rf.getLastLogTerm(),
		}
		rf.mu.Unlock()

		// always vote itself.
		var numVote int32 = 1

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			go func(rf *Raft, idx int) {
				reply := &RequestVoteReply{}
				timeout := int64(float32(rf.electionTimeoutMs) * 0.8)
				ret := rf.sendRequestVote(idx, args, reply, timeout)

				if ret == true {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != CANDIDATE || rf.currentTerm != args.Term {
						atomic.StoreInt32(&needExit, 1)
						return
					}

					if reply.Term > rf.currentTerm {
						atomic.StoreInt32(&needExit, 1)
						rf.convertToFollower(reply.Term, VOTENULL)
						return
					}
					if reply.VoteGranted == true {
						atomic.AddInt32(&numVote, 1)
					}
					// if get marjor vote,convert to leader
					if atomic.LoadInt32(&numVote) > int32(len(rf.peers)/2) {
						atomic.StoreInt32(&needExit, 1)
						rf.convertToLeader()
					}
				}
			}(rf, i)
		}
	}
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

	// Your initialization code here.

	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.voteFor = VOTENULL
	rf.log = make([]*LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = FOLLOWER

	rf.initializeTimer()
	rf.heartbeatTimeoutMs = 100

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// goroute to handle leader election
	go rf.leaderElection()

	return rf
}
