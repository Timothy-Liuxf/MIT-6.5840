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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

func debugPrintln2A(args ...interface{}) {
	// fmt.Println(args...)
}

func debugPrintln2B(args ...interface{}) {
	// fmt.Println(args...)
}

func debugPrintln2C(args ...interface{}) {
	// fmt.Println(args...)
}

func debugPrintln2D(args ...interface{}) {
	// fmt.Println(args...)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a < b {
		return b
	}
	return a
}

const (
	NULL = -1
)

const (
	BASIC_SLEEP_DURATION = 120
	SLEEP_JITTER         = 1
	BASIC_TIMEOUT        = 200
	TIMEOUT_JITTER       = 100
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	lastHeartbeatTime time.Time
	heartbeatTimeout  time.Duration
	role              Role
	lastAckTime       []time.Time
	votedMe           []bool
	applyCh           chan<- ApplyMsg
	newCommitCond     *sync.Cond
	snapshot          Snapshot
	requestSeq        int // For debug
	hasNewSnapshot    bool
}

func (rf *Raft) resetHeartbeatTimeWithoutLock() {
	rf.lastHeartbeatTime = time.Now()
	rf.heartbeatTimeout = time.Duration(BASIC_TIMEOUT+(rand.Int63()%TIMEOUT_JITTER)) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		panic("Failed to encode currentTerm: " + err.Error())
	}
	if err := e.Encode(rf.votedFor); err != nil {
		panic("Failed to encode votedFor: " + err.Error())
	}
	if err := e.Encode(rf.log); err != nil {
		panic("Failed to encode log: " + err.Error())
	}
	if err := e.Encode(rf.snapshot.LastIncludedIndex); err != nil {
		panic("Failed to encode snapshot.LastIncludedIndex: " + err.Error())
	}
	if err := e.Encode(rf.snapshot.LastIncludedTerm); err != nil {
		panic("Failed to encode snapshot.LastIncludedTerm: " + err.Error())
	}
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.snapshot.Data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	debugPrintln2C(rf.me, "readPersist")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if err := d.Decode(&currentTerm); err != nil {
		panic("Failed to decode currentTerm: " + err.Error())
	}
	if err := d.Decode(&votedFor); err != nil {
		panic("Failed to decode votedFor: " + err.Error())
	}
	if err := d.Decode(&log); err != nil {
		panic("Failed to decode log: " + err.Error())
	}
	if log == nil || len(log) == 0 {
		log = append(log, LogEntry{
			Term:    -1,
			Command: nil,
		})
	}
	if err := d.Decode(&lastIncludedIndex); err != nil {
		panic("Failed to decode lastIncludedIndex: " + err.Error())
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		panic("Failed to decode lastIncludedTerm: " + err.Error())
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.snapshot = Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              nil,
	}
	if lastIncludedTerm != NULL {
		rf.snapshot.Data = rf.persister.ReadSnapshot()
	}
	rf.commitIndex = maxInt(rf.commitIndex, lastIncludedIndex)
	rf.lastApplied = maxInt(rf.lastApplied, lastIncludedIndex)
	debugPrintln2D(rf.me, "set lastApplied to", rf.lastApplied, "current commitIndex:", rf.commitIndex, "in `readPersist`")
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debugPrintln2D(rf.me, "prepare to take snapshot at", index)
	// index++ // Do not need to skip sentinel, which is defined in `Start()`
	actualIndex := rf.toActualIndex(index)
	if actualIndex < 0 {
		return
	} else if actualIndex >= len(rf.log) {
		actualIndex = len(rf.log) - 1
		index = rf.toLogIndex(actualIndex)
	}
	debugPrintln2D(rf.me, "take snapshot at", index)
	rf.saveToSnapshotWithoutLock(index, rf.log[actualIndex].Term, snapshot)
	rf.persist()
}

func (rf *Raft) saveToSnapshotWithoutLock(index int, term int, snapshot []byte) {
	rf.log = rf.log[minInt(rf.toActualIndex(index), len(rf.log)):]
	if len(rf.log) == 0 {
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Command: nil,
		}) // Add sentinel
	} else {
		rf.log[0].Term = term // The sentinel must be updated since it may not be correct
	}
	rf.snapshot = Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Data:              snapshot,
	}
	rf.commitIndex = maxInt(rf.commitIndex, index)
	// rf.lastApplied = maxInt(rf.lastApplied, index)
	debugPrintln2D(rf.me, "lastApplied:", rf.lastApplied, "current commitIndex:", rf.commitIndex, "in `saveToSnapshotWithoutLock`")
}

func (rf *Raft) updateCurrentTermWithoutLock(newTerm int) bool {
	if newTerm != rf.currentTerm {
		rf.currentTerm = newTerm
		return true
	}
	return false
}

func (rf *Raft) updateVotedForWithoutLock(newVotedFor int) bool {
	if newVotedFor != rf.votedFor {
		rf.votedFor = newVotedFor
		return true
	}
	return false
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	LastLogTerm int
	VoteGranted bool
}

// Convert log index to actual index in log array.
func (rf *Raft) toActualIndex(index int) int {
	return index - rf.snapshot.LastIncludedIndex
}

// Convert actual index in log array to log index.
func (rf *Raft) toLogIndex(actualIndex int) int {
	return actualIndex + rf.snapshot.LastIncludedIndex
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debugPrintln2A(rf.me, "received vote request from", args.CandidateId)
	debugPrintln2B(rf.me, "received vote request from", args.CandidateId, "last log term:", args.LastLogTerm, "last log term this server:", rf.log[len(rf.log)-1].Term)
	originTerm := rf.currentTerm
	persist := false
	if args.Term > originTerm {
		changed := rf.changeToFollowerWithoutLock(true)
		rf.updateCurrentTermWithoutLock(args.Term)
		if !persist {
			persist = true
			defer rf.persist()
		}
		if changed {
			debugPrintln2B(rf.me, "changed to follower because of vote request in `RequestVote`",
				"args.Term:", args.Term, "originTerm:", originTerm)
		}
	}
	reply.Term = rf.currentTerm
	reply.LastLogTerm = rf.log[len(rf.log)-1].Term
	if rf.votedFor == NULL || rf.votedFor == args.CandidateId {
		if args.Term < originTerm {
			reply.VoteGranted = false
			return
		} else {
			rfLastLogTerm := rf.log[len(rf.log)-1].Term
			rfLastLogIndex := rf.toLogIndex(len(rf.log) - 1)
			if args.LastLogTerm < rfLastLogTerm || (args.LastLogTerm == rfLastLogTerm && args.LastLogIndex < rfLastLogIndex) {
				reply.VoteGranted = false
				return
			}
		}
		votedForChanged := rf.updateVotedForWithoutLock(args.CandidateId)
		if votedForChanged {
			if !persist {
				persist = true
				defer rf.persist()
			}
		}
		reply.VoteGranted = true
		debugPrintln2A(rf.me, "voted for", args.CandidateId)
	}
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []LogEntry
	LeaderCommit int
	RequestSeq   int
}

type AppendEntriesReply struct {
	Term               int
	Success            bool
	ConflictTerm       int
	FirstConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	argPrevLogActualIndex := rf.toActualIndex(args.PrevLogIndex)
	persist := false
	debugPrintln2D(rf.me, "received append entries from", args.LeaderId, "args:", args, "log:", rf.log, "RequestSeq:", args.RequestSeq)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.ConflictTerm = NULL
		reply.FirstConflictIndex = rf.toLogIndex(0)
		debugPrintln2D(rf.me, "received append entries with outdated term in `AppendEntries`", "RequestSeq:", args.RequestSeq)
	} else if argPrevLogActualIndex < 0 {
		reply.Success = false
		reply.ConflictTerm = NULL
		reply.FirstConflictIndex = rf.toLogIndex(argPrevLogActualIndex)
		debugPrintln2D(rf.me, "received append entries with outdated prev log index in `AppendEntries`", "RequestSeq:", args.RequestSeq)
	} else {
		changed := rf.changeToFollowerWithoutLock(args.Term > rf.currentTerm)
		if changed {
			debugPrintln2B(rf.me, "changed to follower because of append entries in `AppendEntries`",
				"args.Term:", args.Term, "rf.currentTerm:", rf.currentTerm, "RequestSeq:", args.RequestSeq)
		}
		if args.Term > rf.currentTerm {
			if !persist {
				persist = true
				defer rf.persist()
			}
		}
		rf.updateCurrentTermWithoutLock(args.Term)
		rf.resetHeartbeatTimeWithoutLock()
		if !persist {
			persist = true
			defer rf.persist()
		}

		if argPrevLogActualIndex >= len(rf.log) {
			debugPrintln2D(rf.me, "prev log index out of range in `AppendEntries`",
				"RequestSeq:", args.RequestSeq,
				"argPrevLogActualIndex:", argPrevLogActualIndex,
				"len(rf.log):", len(rf.log),
				"args.PrevLogIndex:", args.PrevLogIndex,
				"last included index:", rf.snapshot.LastIncludedIndex,
				"log:", rf.log,
			)
			reply.ConflictTerm = NULL
			reply.FirstConflictIndex = rf.toLogIndex(len(rf.log))
			reply.Success = false
		} else {
			if rf.log[argPrevLogActualIndex].Term != args.PrevLogTerm {
				debugPrintln2D(rf.me, "prev log term mismatch in `AppendEntries`",
					"RequestSeq:", args.RequestSeq,
					"argPrevLogActualIndex:", argPrevLogActualIndex,
					"args.PrevLogTerm:", args.PrevLogTerm,
					"rf.log[argPrevLogActualIndex].Term:", rf.log[argPrevLogActualIndex].Term)
				conflictTerm := rf.log[argPrevLogActualIndex].Term
				firstConflictActualIndex := argPrevLogActualIndex
				for i := argPrevLogActualIndex - 1; i >= 0; i-- {
					if rf.log[i].Term != conflictTerm {
						break
					}
					firstConflictActualIndex = i
				}
				reply.ConflictTerm = conflictTerm
				reply.FirstConflictIndex = rf.toLogIndex(maxInt(firstConflictActualIndex, 1))
				reply.Success = false
			} else {
				for i, logEntry := range args.LogEntries {
					if argPrevLogActualIndex+i+1 >= len(rf.log) || logEntry.Term != rf.log[argPrevLogActualIndex+i+1].Term {
						newLog := append(rf.log[:argPrevLogActualIndex+i+1], args.LogEntries[i:]...)
						if len(newLog) == 0 {
							newLog = append(newLog, rf.log[0])
						}
						rf.log = newLog
						if !persist {
							persist = true
							defer rf.persist()
						}
						break
					}
				}

				reply.Success = true
				rf.commitIndex = maxInt(minInt(args.LeaderCommit, rf.toLogIndex(len(rf.log)-1)), rf.commitIndex)
				rf.newCommitCond.Signal()
				debugPrintln2B(rf.me, "received and accepted log entries from", args.LeaderId, ":", args.LogEntries,
					"RequestSeq:", args.RequestSeq, "current log:", rf.log)
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debugPrintln2B(rf.me, "received command:", command)

	if rf.killed() || rf.role != Leader {
		return NULL, NULL, false
	}

	debugPrintln2B(rf.me, "start agreement on command:", command)

	index := rf.toLogIndex(len(rf.log))
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})
	rf.persist()
	rf.nextIndex[rf.me] = rf.toLogIndex(len(rf.log))
	rf.matchIndex[rf.me] = rf.toLogIndex(len(rf.log)) - 1
	rf.synchronizeEntriesWithoutLock()
	return index, term, true
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.resetHeartbeatTimeWithoutLock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	changed := rf.changeToFollowerWithoutLock(args.Term > rf.currentTerm)
	if changed {
		debugPrintln2B(rf.me, "changed to follower because of install snapshot in `InstallSnapshot`",
			"args.Term:", args.Term, "rf.currentTerm:", rf.currentTerm)
	}
	persist := false
	if args.Term > rf.currentTerm {
		persist = true
		defer rf.persist()
	}
	rf.updateCurrentTermWithoutLock(args.Term)
	debugPrintln2D(rf.me, "received install snapshot from", args.LeaderId,
		"last included index:", args.LastIncludedIndex, "last included term:", args.LastIncludedTerm)
	if args.LastIncludedIndex <= rf.snapshot.LastIncludedIndex {
		reply.Success = true
		return
	}
	rf.saveToSnapshotWithoutLock(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	if !persist {
		persist = true
		defer rf.persist()
	}
	rf.hasNewSnapshot = true
	rf.newCommitCond.Signal()
	reply.Success = true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	rf.newCommitCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) changeToFollowerWithoutLock(resetVote bool) bool {
	changed := false
	if rf.role != Follower {
		debugPrintln2A(rf.me, "changed to follower")
		changed = true
	}
	rf.role = Follower
	if resetVote {
		rf.updateVotedForWithoutLock(NULL)
	}
	return changed
}

func (rf *Raft) changeToLeaderWithoutLock() {
	if rf.role != Leader {
		debugPrintln2A(rf.me, "changed to leader (2A)")
		debugPrintln2B(rf.me, "changed to leader (2B)")
	}
	rf.role = Leader
	rf.resetNextIndexAndMatchIndexWithoutLock()
}

func (rf *Raft) changeToCandidateWithoutLock() {
	rf.role = Candidate
	rf.currentTerm++
	debugPrintln2A(rf.me, "current term:", rf.currentTerm, "changed to candidate")
	rf.updateVotedForWithoutLock(rf.me)
	rf.persist()
	rf.resetHeartbeatTimeWithoutLock()
	rf.votedMe = make([]bool, len(rf.peers))
	rf.votedMe[rf.me] = true
}

func (rf *Raft) resetNextIndexAndMatchIndexWithoutLock() {
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.toLogIndex(len(rf.log))
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = rf.toLogIndex(0)
	}
}

func (rf *Raft) RequestVotesFromPeersWithoutLock() {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.toLogIndex(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				rf.lastAckTime[server] = time.Now()
				if args.Term != rf.currentTerm {
					return
				}
				if reply.Term > rf.currentTerm || (reply.Term == rf.currentTerm && reply.LastLogTerm > rf.log[len(rf.log)-1].Term) {
					rf.updateCurrentTermWithoutLock(reply.Term)
					changed := rf.changeToFollowerWithoutLock(true)
					if changed {
						debugPrintln2B(rf.me,
							"changed to follower because of vote request in `RequestVotesFromPeersWithoutLock`",
							"reply.Term:", reply.Term, "rf.currentTerm:", rf.currentTerm,
							"reply.LastLogTerm:", reply.LastLogTerm, "rf.log[len(rf.log)-1].Term:", rf.log[len(rf.log)-1].Term)
					}
					rf.persist()
					return
				}
				if reply.VoteGranted {
					rf.votedMe[server] = true
					totalVotes := 0
					for i, voted := range rf.votedMe {
						if i == rf.me {
							totalVotes++
						} else if voted {
							totalVotes++
						}
					}
					if totalVotes > len(rf.peers)/2 {
						rf.changeToLeaderWithoutLock()
						rf.sendHeartbeatsWithoutLock()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) enoughActivePeersWithoutLock() bool {
	activePeers := 0
	for i := range rf.lastAckTime {
		if i == rf.me {
			activePeers++
		} else if time.Since(rf.lastAckTime[i]) <= rf.heartbeatTimeout {
			activePeers++
		}
	}
	return activePeers > len(rf.peers)/2
}

func (rf *Raft) sendHeartbeatsWithoutLock() {
	rf.synchronizeEntriesWithoutLock()
}

func (rf *Raft) synchronizeEntriesWithoutLock() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.synchronizeEntriesTo(i)
	}
}

func (rf *Raft) synchronizeEntriesTo(server int) {
	appendArgs, installArgs, isInstall, valid := func() (AppendEntriesArgs, InstallSnapshotArgs, bool, bool) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.role != Leader {
			return AppendEntriesArgs{}, InstallSnapshotArgs{}, false, false
		}

		debugPrintln2D(rf.me, "synchronize entries to", server,
			"nextIndex:", rf.nextIndex[server],
			"lastIncludedIndex:", rf.snapshot.LastIncludedIndex,
			"current log:", rf.log,
		)
		if rf.nextIndex[server] <= rf.snapshot.LastIncludedIndex {
			installArgs := InstallSnapshotArgs{}
			installArgs.Term = rf.currentTerm
			installArgs.LeaderId = rf.me
			installArgs.LastIncludedIndex = rf.snapshot.LastIncludedIndex
			installArgs.LastIncludedTerm = rf.snapshot.LastIncludedTerm
			installArgs.Data = make([]byte, len(rf.snapshot.Data))
			copy(installArgs.Data, rf.snapshot.Data)
			return AppendEntriesArgs{}, installArgs, true, true
		} else {
			nextIndex := rf.nextIndex[server]
			nextActualIndex := rf.toActualIndex(nextIndex)
			if nextActualIndex > len(rf.log) {
				nextActualIndex = len(rf.log)
				nextIndex = rf.toLogIndex(nextActualIndex)
			}
			appendArgs := AppendEntriesArgs{}
			appendArgs.Term = rf.currentTerm
			appendArgs.LeaderId = rf.me
			appendArgs.PrevLogIndex = nextIndex - 1
			appendArgs.PrevLogTerm = rf.log[nextActualIndex-1].Term
			appendArgs.LogEntries = make([]LogEntry, len(rf.log[nextActualIndex:]))
			copy(appendArgs.LogEntries, rf.log[nextActualIndex:])
			appendArgs.LeaderCommit = rf.commitIndex
			appendArgs.RequestSeq = rf.requestSeq
			rf.requestSeq++
			return appendArgs, InstallSnapshotArgs{}, false, true
		}
	}()
	if !valid {
		return
	}

	if isInstall {
		debugPrintln2D(rf.me, "send install snapshot to", server)
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, &installArgs, &reply)
		if ok {
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				rf.lastAckTime[server] = time.Now()
				debugPrintln2D(rf.me, "received reply of install snapshot from", server) // , "at time in millisecond:", rf.lastAckTime[server].UnixMilli())
				if installArgs.Term != rf.currentTerm {
					return
				}
				if !reply.Success {
					if reply.Term > rf.currentTerm {
						originTerm := rf.currentTerm
						rf.updateCurrentTermWithoutLock(reply.Term)
						changed := rf.changeToFollowerWithoutLock(true)
						rf.resetHeartbeatTimeWithoutLock() // TARGET: in case this server will start election immediately
						if changed {
							debugPrintln2D(rf.me, "changed to follower because of install snapshot in `synchronizeEntriesTo`",
								"reply.Term:", reply.Term, "rf.currentTerm:", originTerm)
						}
						rf.persist()
					} else {
						debugPrintln2D(rf.me, "install snapshot failed and this should not happen")
					}
				} else {
					newNextIndex := maxInt(installArgs.LastIncludedIndex+1, rf.nextIndex[server])
					rf.nextIndex[server] = newNextIndex
					rf.matchIndex[server] = newNextIndex - 1
					// QUESTION: broadcast to all peers or just the one that needs to install snapshot?
					go rf.synchronizeEntriesTo(server)
					// rf.synchronizeEntriesWithoutLock()
				}
			}()
		}
	} else {
		debugPrintln2D(appendArgs.LeaderId, "send append entries to", server, "args:", appendArgs, "RequestSeq:", appendArgs.RequestSeq)
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &appendArgs, &reply)
		if ok {
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				rf.lastAckTime[server] = time.Now()
				debugPrintln2B(rf.me, "received reply from", server, "RequestSeq:", appendArgs.RequestSeq) // , "at time in millisecond:", rf.lastAckTime[server].UnixMilli())
				if appendArgs.Term != rf.currentTerm {
					return
				}
				if !reply.Success {
					if reply.Term > rf.currentTerm {
						originTerm := rf.currentTerm
						rf.updateCurrentTermWithoutLock(reply.Term)
						changed := rf.changeToFollowerWithoutLock(true)
						rf.resetHeartbeatTimeWithoutLock() // TARGET: in case this server will start election immediately
						if changed {
							debugPrintln2B(rf.me, "changed to follower because of append entries in `synchronizeEntriesTo`",
								"reply.Term:", reply.Term, "rf.currentTerm:", originTerm)
						}
						rf.persist()
					} else {
						firstConflictActualIndex := rf.toActualIndex(reply.FirstConflictIndex)
						if firstConflictActualIndex >= len(rf.log) {
							firstConflictActualIndex = maxInt(minInt(len(rf.log)-1, appendArgs.PrevLogIndex), 1)
						} else if firstConflictActualIndex <= 0 {
							// Should send snapshot
						} else if rf.log[firstConflictActualIndex].Term == reply.ConflictTerm {
							for i := firstConflictActualIndex - 1; i >= 0; i-- {
								if rf.log[i].Term != reply.ConflictTerm {
									firstConflictActualIndex = i + 1
									break
								}
							}
						}
						rf.nextIndex[server] = rf.toLogIndex(firstConflictActualIndex)
						go rf.synchronizeEntriesTo(server)
					}
				} else {
					newNextIndex := maxInt(appendArgs.PrevLogIndex+1+len(appendArgs.LogEntries), 1)
					newMatchIndex := newNextIndex - 1
					rf.nextIndex[server] = newNextIndex
					rf.matchIndex[server] = newMatchIndex

					matchCount := 0
					for _, matchIndex := range rf.matchIndex {
						if matchIndex >= newMatchIndex {
							matchCount++
						}
					}
					if matchCount > len(rf.peers)/2 && newMatchIndex > rf.commitIndex {
						rf.commitIndex = maxInt(newMatchIndex, rf.commitIndex)
						rf.newCommitCond.Signal()
						rf.synchronizeEntriesWithoutLock()
					}
				}
			}()
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return
	}
	rf.sendHeartbeatsWithoutLock()
}

func (rf *Raft) ticker() {
	debugPrintln2A(rf.me, "started")
	for rf.killed() == false {
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			debugPrintln2B(rf.me, "current term:", rf.currentTerm, "current log:", rf.log)
			switch rf.role {
			case Follower, Candidate:
				debugPrintln2D(rf.me, "as follower or candidate start ticker")
				if time.Since(rf.lastHeartbeatTime) > rf.heartbeatTimeout {
					debugPrintln2D(rf.me, "heartbeat timeout")
					rf.changeToCandidateWithoutLock()
					debugPrintln2A(rf.me, "started election")
					rf.RequestVotesFromPeersWithoutLock()
				}
				debugPrintln2D(rf.me, "as follower or candidate end ticker")
			case Leader:
				debugPrintln2D(rf.me, "as leader start ticker")
				if !rf.enoughActivePeersWithoutLock() {
					debugPrintln2D(rf.me, "not enough active peers")
					changed := rf.changeToFollowerWithoutLock(true)
					if changed {
						debugPrintln2B(rf.me, "changed to follower because of heartbeat timeout in `ticker` at:", time.Now().UnixMilli())
					}
					rf.persist()
					break
				}
				debugPrintln2D(rf.me, "send heartbeats")
				rf.sendHeartbeatsWithoutLock()
				debugPrintln2D(rf.me, "as leader end ticker")
			}
		}()
		ms := BASIC_SLEEP_DURATION + (rand.Int63() % SLEEP_JITTER)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyNewCommitEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		commitActualIndex := rf.toActualIndex(rf.commitIndex)
		lastAppliedActualIndex := rf.toActualIndex(rf.lastApplied)
		endActualIndex := minInt(commitActualIndex, len(rf.log)-1)
		for !rf.killed() && lastAppliedActualIndex >= endActualIndex && !rf.hasNewSnapshot {
			rf.newCommitCond.Wait()
			debugPrintln2B(rf.me, "wake up")
			debugPrintln2B(rf.me, "lastApplied:", rf.lastApplied, "commitIndex:", rf.commitIndex)
			commitActualIndex = rf.toActualIndex(rf.commitIndex)
			lastAppliedActualIndex = rf.toActualIndex(rf.lastApplied)
			endActualIndex = minInt(commitActualIndex, len(rf.log)-1)
		}
		if rf.killed() {
			return
		}
		if rf.hasNewSnapshot {
			debugPrintln2D(rf.me, "apply new snapshot")
			if rf.snapshot.LastIncludedIndex <= rf.lastApplied {
				debugPrintln2D(rf.me, "skip snapshot at index:", rf.snapshot.LastIncludedIndex, "lastApplied:", rf.lastApplied, "commitIndex:", rf.commitIndex)
				rf.hasNewSnapshot = false
			} else {
				applyMsg := ApplyMsg{
					CommandValid:  false,
					SnapshotValid: true,
					Snapshot:      rf.snapshot.Data,
					SnapshotTerm:  rf.snapshot.LastIncludedTerm,
					SnapshotIndex: rf.snapshot.LastIncludedIndex,
				}
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
				rf.hasNewSnapshot = false
				rf.lastApplied = maxInt(rf.lastApplied, applyMsg.SnapshotIndex)
				debugPrintln2D(rf.me, "applied snapshot at index:", rf.snapshot.LastIncludedIndex, "lastApplied:", rf.lastApplied, "commitIndex:", rf.commitIndex)
			}
		} else {
			debugPrintln2B(rf.me, "apply new commit entries")

			endActualIndex = minInt(commitActualIndex, len(rf.log)-1)
			endIndex := rf.toLogIndex(endActualIndex)
			applyMsgs := make([]ApplyMsg, 0)
			for i := maxInt(lastAppliedActualIndex+1, 1); i <= endActualIndex; i++ {
				applyMsgs = append(applyMsgs, ApplyMsg{
					CommandValid:  true,
					Command:       rf.log[i].Command,
					CommandIndex:  rf.toLogIndex(i),
					SnapshotValid: false,
				})
			}
			rf.mu.Unlock()
			for _, applyMsg := range applyMsgs {
				rf.applyCh <- applyMsg
				debugPrintln2B(rf.me, "applied command:", applyMsg.Command, "at index:", applyMsg.CommandIndex)
			}
			rf.mu.Lock()
			rf.lastApplied = maxInt(endIndex, rf.lastApplied)
		}
		debugPrintln2D(rf.me, "set lastApplied to", rf.lastApplied, "current commitIndex:", rf.commitIndex, "in `applyNewCommitEntries`",
			"lastAppliedActualIndex:", lastAppliedActualIndex, "endActualIndex:", endActualIndex)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0
	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{
		Term:    -1,
		Command: nil,
	}
	rf.snapshot = Snapshot{
		LastIncludedIndex: 0,
		LastIncludedTerm:  NULL,
		Data:              nil,
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.resetNextIndexAndMatchIndexWithoutLock()

	rf.resetHeartbeatTimeWithoutLock()
	rf.changeToFollowerWithoutLock(true)
	rf.lastAckTime = make([]time.Time, len(peers))
	for i := range rf.lastAckTime {
		rf.lastAckTime[i] = time.Now()
	}
	rf.votedMe = make([]bool, len(peers))
	rf.applyCh = applyCh
	rf.newCommitCond = sync.NewCond(&rf.mu)
	rf.requestSeq = 0
	// QUESTION: Check rf.snapshot.Data as below will be wrong, but I don't know why
	// rf.hasNewSnapshot = rf.snapshot.LastIncludedTerm != NULL
	rf.hasNewSnapshot = false

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyNewCommitEntries()

	return rf
}
