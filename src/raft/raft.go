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
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const null = -1

const (
	follower = iota
	candidate
	leader
)

type LogEntry struct {
	Command interface{}
	Term    int
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	role                  int
	lastValidRPCTimestamp time.Time
	voteCnt               int
	applyCh               chan ApplyMsg
	applyIndex            int
	cond                  *sync.Cond

	snapshot  []byte
	logOffset int
}

// must hold lock before call
func (rf *Raft) getLogLen() int {
	return len(rf.log) + rf.logOffset
}

// must hold lock before call
func (rf *Raft) getLogEntry(i int) LogEntry {
	i -= rf.logOffset
	return rf.log[i]
}

// must hold lock before call
func (rf *Raft) getLogSuffixCopy(i int) []LogEntry {
	i -= rf.logOffset
	entries := make([]LogEntry, len(rf.log)-i)
	copy(entries, rf.log[i:])
	return entries
}

// must hold lock before call
func (rf *Raft) getLogPrefix(i int) []LogEntry {
	i -= rf.logOffset
	return rf.log[:i]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
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
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.logOffset)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil ||
		d.Decode(&rf.logOffset) != nil {
		log.Fatalf("S%d readPersist error", rf.me)
	}
	rf.snapshot = rf.persister.ReadSnapshot()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = rf.getLogSuffixCopy(index)
	rf.logOffset = index
	rf.snapshot = make([]byte, len(snapshot))
	copy(rf.snapshot, snapshot)
	DPrintf(dSnap, "S%d Snapshot, Log Offset %d", rf.me, rf.logOffset)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == leader {
		DPrintf(dDrop, "S%d Kill, MatchI%v", rf.me, rf.matchIndex)
	} else {
		DPrintf(dDrop, "S%d Kill", rf.me)
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.votedFor = null
	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf(dInfo, "S%d Make, I%d", rf.me, rf.logOffset)

	rf.applyIndex = rf.logOffset
	go rf.apply()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) apply() {
	for !rf.killed() {
		rf.cond.L.Lock()
		for rf.applyIndex == rf.commitIndex {
			rf.cond.Wait()
		}
		for rf.applyIndex+1 <= rf.commitIndex {
			rf.applyIndex++
			DPrintf(dCommit, "S%d Apply CommitIndex, C%d", rf.me, rf.applyIndex)
			msg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.getLogEntry(rf.applyIndex).Command,
				CommandIndex:  rf.applyIndex,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			rf.cond.L.Unlock()
			rf.applyCh <- msg
			rf.cond.L.Lock()
		}
		rf.cond.L.Unlock()
	}
}

