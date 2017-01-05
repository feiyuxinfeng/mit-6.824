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
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "encoding/gob"

type IntSet struct {
	set map[int]bool
}

func NewIntSet() *IntSet {
	return &IntSet{
		set: make(map[int]bool),
	}
}

func (set *IntSet) Add(i int) bool {
	_, found := set.set[i]
	set.set[i] = true
	return !found //False if it existed already
}

func (set *IntSet) Delete(i int) {
	delete(set.set, i)
}

func (set *IntSet) Clear() {
	set.set = make(map[int]bool)
}

func (set *IntSet) Len() int {
	return len(set.set)
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func safeClose(ch chan bool) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func getRandomElectionTimeout() int64 {
	return 300 + rand.Int63n(200)
}

func intMax(i1, i2 int) int {
	if i1 > i2 {
		return i1
	} else {
		return i2
	}
}

func intMin(i1, i2 int) int {
	if i1 < i2 {
		return i1
	} else {
		return i2
	}
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

const RPC_TIMEOUT = 200 // 200 milliseconds

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

	failedPeers *IntSet

	applyCh chan ApplyMsg

	startMu sync.Mutex
}

func (rf *Raft) isLeader() bool {
	return (rf.state == LEADER) && (rf.failedPeers.Len() <= len(rf.peers)/2)
}

func (rf *Raft) numLogs() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := rf.getLastLogIndex()
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
	prevLogIndex := rf.getPrevLogIndex(idx)
	// DPrintf("server %v getPrevLogTerm prevLogIndex: %v", idx, prevLogIndex)
	if prevLogIndex == 0 {
		return -1
	} else {
		return rf.log[prevLogIndex].Term
	}
}

func (rf *Raft) applyLogs() {
	if rf.commitIndex > rf.lastApplied {
		D1Printf("server %v apply logs(%v => %v), term: %v", rf.me, rf.lastApplied+1, rf.commitIndex, rf.currentTerm)
	}
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

// update leader's CommitIndex if neccessary
func (rf *Raft) updateLeaderCommitIndex() {
	tmp := make([]int, len(rf.matchIndex))
	copy(tmp, rf.matchIndex)
	sort.Ints(tmp)
	idx := len(tmp) - len(tmp)/2
	val := tmp[idx]

	for i := val; i > rf.commitIndex; i-- {
		if rf.log[i].Term == rf.currentTerm {
			D1Printf("Leader Server %v Update commitid %v => %v, last applied %v, matchidx: %v",
				rf.me, rf.commitIndex, i, rf.lastApplied, rf.matchIndex)
			rf.commitIndex = i
		}
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.isLeader()

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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	// rf.dPrintSavedState()
	// DPrintf("server %v memory state=> term: %v, lastLogIndex: %v, voteFor: %v", rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.voteFor)
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

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
}

// print the saved state(for debug)
func (rf *Raft) dPrintSavedState() {
	data := rf.persister.ReadRaftState()
	var term, voteFor int
	var log []*LogEntry

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&term)
	d.Decode(&voteFor)
	d.Decode(&log)
	DPrintf("server %v saved state=> term: %v, lastLogIndex: %v, voteFor: %v", rf.me, term, len(log)-1, voteFor)
}

func (rf *Raft) dPrintInfo() {
	log.Printf("================= server %v info ================", rf.me)
	log.Printf("Server %v, term: %v, state: %v", rf.me, rf.currentTerm, rf.state.String())
	log.Printf("CommitIndex: %v, lastApplied: %v", rf.commitIndex, rf.lastApplied)
	if rf.state == LEADER {
		log.Printf("leader state: failedPeers: %v, matchIndex => %v, nextIndex => %v", rf.failedPeers, rf.matchIndex, rf.nextIndex)
	}
	for _, entry := range rf.log[1:] {
		log.Printf("[%v, %v, %v] ", entry.Index, entry.Command, entry.Term)
	}
	log.Printf("============ server %v info end ====================", rf.me)
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

	// hint for leader to update nextIndex
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// all server 1
	if (args.Term > rf.currentTerm) ||
		(args.Term == rf.currentTerm && rf.state == CANDIDATE) {
		rf.convertToFollower(args.Term, args.LeaderId)
	}
	// 2
	hasConflict := false
	conflictTerm := 0

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	argsLastLogIndex := args.PrevLogIndex + len(args.Entries)

	if lastLogIndex < args.PrevLogIndex {
		hasConflict = true
		conflictTerm = intMin(lastLogTerm, args.PrevLogTerm)
	} else {
		if args.PrevLogIndex > 0 {
			term := rf.log[args.PrevLogIndex].Term
			if term != args.PrevLogTerm {
				hasConflict = true
				conflictTerm = intMin(args.PrevLogTerm, term)

			}
		}
	}
	if hasConflict {
		reply.Success = false
		reply.Term = rf.currentTerm

		// optimization for log consist check,
		reply.ConflictTerm = conflictTerm

		// we will append first log index of conflict term to reply message
		conflictIndex := 0
		upperBound := intMin(args.PrevLogIndex, lastLogIndex)
		for i := upperBound; i > 0; i-- {
			if rf.log[i].Term == conflictTerm {
				conflictIndex = i
			} else if rf.log[i].Term < conflictTerm {
				break
			}
		}
		reply.ConflictIndex = conflictIndex
		DPrintf("server %v Update timer, term %v", rf.me, rf.currentTerm)
		rf.updateTimer()
		return
	}

	// 3, 4 overwrite conflict entry and append new entries
	if len(args.Entries) > 0 {
		needOverwrite := false
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
				needOverwrite = true
			}
		}
		// see readme 6
		// when we've already overwritten log, then we should delete logs behind argsLastLogIndex
		if lastLogIndex > argsLastLogIndex {
			if needOverwrite {
				rf.log = rf.log[:argsLastLogIndex+1]
			}
		}
		D1Printf("server %v <== %v receive log(%v-%v)", rf.me, args.LeaderId,
			args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries))
	}

	consistIndex := args.PrevLogIndex + len(args.Entries)
	updateCommitIdx := intMin(args.LeaderCommit, consistIndex)
	// DPrintf("server %v ==> LeaderCommit: %v, localCommit: %v, consistIndex: %v, numEntry: %v", rf.me, args.LeaderCommit, rf.commitIndex, consistIndex, len(args.Entries))

	// 5
	if updateCommitIdx > rf.commitIndex {
		D1Printf("Server %v Update commitid %v => %v, last applied %v", rf.me, rf.commitIndex, updateCommitIdx, rf.lastApplied)
		// minIndex := args.LeaderCommit
		// if minIndex > rf.getLastLogIndex() {
		// 	minIndex = rf.getLastLogIndex()
		// }
		// rf.commitIndex = minIndex
		rf.commitIndex = updateCommitIdx
	}
	// all server 1
	rf.applyLogs()

	// DPrintf("server %v Update timer, term %v", rf.me, rf.currentTerm)

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
	defer rf.persist()

	// 1
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// all server 1
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term, VOTENULL)
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

	rf.updateTimer()
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		reply.Term = rf.currentTerm
		return
	}
}

func (rf *Raft) replicateLog() {
	totalServers := len(rf.peers)
	originTerm := rf.currentTerm

	for i := 0; i < totalServers; i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			for {
				rf.mu.Lock()
				if rf.state != LEADER || rf.currentTerm != originTerm {
					rf.mu.Unlock()
					return
				}
				nextIdx := rf.nextIndex[idx]
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,

					PrevLogIndex: rf.getPrevLogIndex(idx),
					PrevLogTerm:  rf.getPrevLogTerm(idx),
					Entries:      rf.log[nextIdx:],

					LeaderCommit: rf.commitIndex,
				}
				if len(args.Entries) == 0 {
					rf.mu.Unlock()
					return
				}
				// DPrintf("server %v args: %v", idx, args)
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(idx, args, reply, RPC_TIMEOUT)

				rf.mu.Lock()

				if rf.state != LEADER || rf.currentTerm != originTerm {
					rf.mu.Unlock()
					return
				}

				if ok == true {
					// DPrintf("Success: %v", reply.Success)
					if reply.Term > rf.currentTerm {
						// convert to follower
						rf.convertToFollower(reply.Term, VOTENULL)
						rf.mu.Unlock()
						return
					}
					rf.failedPeers.Delete(idx)

					if reply.Success == false {
						// unconsistent log, so we need decrease nextIndex
						DPrintf("=========conflict index: %v, term: %v", reply.ConflictIndex, reply.ConflictTerm)
						if reply.ConflictTerm <= 0 || reply.ConflictIndex <= 0 {
							rf.nextIndex[idx] = intMax(1, rf.matchIndex[idx]+1)
						} else {
							nextIdx := 1
							if rf.log[reply.ConflictIndex].Term == reply.ConflictTerm {
								nextIdx = intMax(1, reply.ConflictIndex)
							} else {
								// still conflict, bypass a term
								term := reply.ConflictTerm - 1
								for i := reply.ConflictIndex; i > 0; i-- {
									if rf.log[i].Term == term {
										nextIdx = i
									} else if rf.log[i].Term < term {
										break
									}
								}
							}
							rf.nextIndex[idx] = intMax(nextIdx, rf.matchIndex[idx]+1)
						}
					} else {
						rf.nextIndex[idx] = intMax(rf.nextIndex[idx], nextIdx+len(args.Entries))
						rf.matchIndex[idx] = rf.nextIndex[idx] - 1
						rf.updateLeaderCommitIndex()
						rf.applyLogs()

						rf.mu.Unlock()

						return
					}
				} else {
					rf.failedPeers.Add(idx)
				}
				rf.mu.Unlock()
			}
		}(i)
	}
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
	rf.persist()

	D1Printf("Server %v Start command %v index %v, entry: %v", rf.me, command, index, entry)
	rf.replicateLog()

	rf.mu.Unlock()

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
	D1Printf("Convert server %v's state(%v => follower) Term(%v => %v)", rf.me, rf.state.String(), rf.currentTerm, term)

	defer rf.persist()

	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.voteFor = voteFor
	// rf.updateTimer()
	rf.initializeTimer()
	go rf.leaderElection()
}

func (rf *Raft) broadcastHeartbeat() {
	currentTerm := rf.currentTerm

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		quitChan := make(chan bool)

		go func(rf *Raft, idx int) {
			ticker := time.NewTicker(time.Duration(rf.heartbeatTimeoutMs) * time.Millisecond)
			for ; ; <-ticker.C {
				select {
				case <-quitChan:
					return
				default:
				}

				rf.mu.Lock()
				if rf.state != LEADER || rf.currentTerm != currentTerm {
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

				// DPrintf("send heartbeat(%v => %v), term: %v", rf.me, idx, rf.currentTerm)
				// startTime := time.Now()
				ret := rf.sendAppendEntries(idx, args, reply, RPC_TIMEOUT)
				// elapsed := time.Since(startTime)
				// DPrintf("send heartbeat(%v => %v) took %s, result: %v", rf.me, idx, elapsed, ret)

				rf.mu.Lock()

				if rf.state != LEADER || rf.currentTerm != currentTerm {
					rf.mu.Unlock()
					return
				}
				if ret == true {
					// DPrintf("heart beat (%v => %v) success", rf.me, idx)

					rf.failedPeers.Delete(idx)

					if reply.Term > rf.currentTerm {
						// convert to follower
						rf.convertToFollower(reply.Term, VOTENULL)
						close(quitChan)
					}
					// else {
					// 	if reply.Success == false {
					// 		rf.nextIndex[idx]--
					// 	}
					// }
				} else {
					// DPrintf("AppendEntries(%v => %v) fails", rf.me, idx)
					rf.failedPeers.Add(idx)
				}
				rf.mu.Unlock()
			}
		}(rf, i)
	}
}

func (rf *Raft) convertToLeader() {
	D1Printf("Convert server %v's state(%v => leader) Term(%v)", rf.me, rf.state.String(), rf.currentTerm)
	defer rf.persist()

	rf.state = LEADER
	rf.voteFor = rf.me // prevent RequestVote to leader for this term
	rf.timer = nil
	rf.failedPeers.Clear()

	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.broadcastHeartbeat()
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	originTerm := rf.currentTerm
	prevTerm := rf.currentTerm - 1
	timer := rf.timer
	rf.mu.Unlock()

	DPrintf("server %v create leader election goroutine %v, term: %v", rf.me, getGID(), originTerm)

	for iter := 0; ; iter++ {

		<-timer.C

		rf.mu.Lock()
		if (iter == 0 && rf.state != FOLLOWER) ||
			(iter > 0 && rf.state != CANDIDATE) ||
			(prevTerm < rf.currentTerm-1) {
			rf.mu.Unlock()
			DPrintf("server %v leader election goroutine %v exit, info mismatch, term %v", rf.me, getGID(), originTerm)
			break
		}

		D1Printf("Server(%v) expire, term %v", rf.me, rf.currentTerm)

		prevTerm = rf.currentTerm

		// rf.updateTimer()
		rf.reinitializeTimer()
		rf.state = CANDIDATE
		rf.currentTerm++
		rf.voteFor = rf.me

		rf.persist()

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

			go func(idx int, args RequestVoteArgs) {
				for {
					rf.mu.Lock()
					if rf.state != CANDIDATE || rf.currentTerm != args.Term {
						DPrintf("requestVote goroutine %v exit, term: %v, info mismatch", getGID(), args.Term)
						rf.mu.Unlock()
						return
					}
					timeout := int64(float32(rf.electionTimeoutMs) * 0.5)
					rf.mu.Unlock()

					reply := &RequestVoteReply{}
					ret := rf.sendRequestVote(idx, args, reply, timeout)
					DPrintf("goroutine %v Send requestVote(%v ==> %v), term: %v, result: %v", getGID(), rf.me, idx, args.Term, ret)

					if ret == true {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rf.state != CANDIDATE || rf.currentTerm != args.Term {
							return
						}

						if reply.Term > rf.currentTerm {
							rf.convertToFollower(reply.Term, VOTENULL)
							return
						}
						if reply.VoteGranted == true {
							atomic.AddInt32(&numVote, 1)
						}
						// if get marjor vote,convert to leader
						if atomic.LoadInt32(&numVote) > int32(len(rf.peers)/2) {
							rf.convertToLeader()
						}
						return
					}
				}
			}(i, args)
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
	// in order to work well with gob, the first element can't be nil
	rf.log[0] = &LogEntry{}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = FOLLOWER
	rf.failedPeers = NewIntSet()

	rf.initializeTimer()
	rf.heartbeatTimeoutMs = 100

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("server %v state=> term: %v, lastLogIndex: %v, voteFor: %v", rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.voteFor)

	// goroute to handle leader election
	go rf.leaderElection()

	return rf
}
