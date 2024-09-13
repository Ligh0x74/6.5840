package raft

import (
	"sort"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int
	XIndex int
	XLen   int
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ok := false
	ok = ok || (rf.currentTerm > args.Term)
	ok = ok || (rf.getLogLen()-1 < args.PrevLogIndex)
	ok = ok || (rf.logOffset <= args.PrevLogIndex && rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm)

	//if len(args.Entries) != 0 {
	//	DPrintf(dLog, "S%d -> S%d AppendEntries, Log Before Append %v -> %v, PrevI%d PrevT%d", args.LeaderId, rf.me, args.Entries, rf.log, args.PrevLogIndex, args.PrevLogTerm)
	//}

	reply.XTerm, reply.XIndex, reply.XLen = null, null, null
	if rf.currentTerm <= args.Term && rf.getLogLen()-1 < args.PrevLogIndex {
		reply.XLen = rf.getLogLen()
	}
	if rf.currentTerm <= args.Term && rf.getLogLen()-1 >= args.PrevLogIndex && rf.logOffset <= args.PrevLogIndex && rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.XTerm = rf.getLogEntry(args.PrevLogIndex).Term
		reply.XIndex = sort.Search(args.PrevLogIndex+1-rf.logOffset, func(i int) bool {
			return rf.log[i].Term >= reply.XTerm
		})
		reply.XIndex += rf.logOffset
	}

	if ok {
		reply.Success = false
	} else {
		rf.lastValidRPCTimestamp = time.Now()
		reply.Success = true

		if rf.logOffset <= args.PrevLogIndex {
			for i := range args.Entries {
				if rf.getLogLen()-1 < args.PrevLogIndex+i+1 {
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				}
				if rf.getLogEntry(args.PrevLogIndex+i+1).Term != args.Entries[i].Term {
					rf.log = rf.getLogPrefix(args.PrevLogIndex + i + 1)
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				}
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
				rf.cond.Broadcast()
			}
		}
	}

	//if len(args.Entries) != 0 {
	//	DPrintf(dLog, "S%d -> S%d AppendEntries, Log After Append %v -> %v, PrevI%d PrevT%d", args.LeaderId, rf.me, args.Entries, rf.log, args.PrevLogIndex, args.PrevLogTerm)
	//}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.role = follower
	}
	if rf.role == candidate && rf.currentTerm == args.Term {
		rf.role = follower
	}
	reply.Term = rf.currentTerm
	rf.persist()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ok := false
	ok = ok || (rf.currentTerm > args.Term)
	ok = ok || (rf.currentTerm == args.Term && rf.votedFor != null && rf.votedFor != args.CandidateId)
	lastIndex := rf.getLogLen() - 1
	ok = ok || (rf.getLogEntry(lastIndex).Term > args.LastLogTerm)
	ok = ok || (rf.getLogEntry(lastIndex).Term == args.LastLogTerm && lastIndex > args.LastLogIndex)

	if ok {
		reply.VoteGranted = false
	} else {
		rf.lastValidRPCTimestamp = time.Now()
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.role = follower
	}
	reply.Term = rf.currentTerm
	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ok := false
	ok = ok || (rf.currentTerm > args.Term)
	ok = ok || (rf.logOffset >= args.LastIncludedIndex)

	if !ok {
		rf.lastValidRPCTimestamp = time.Now()
		snapshot := make([]byte, len(args.Data))
		copy(snapshot, args.Data)
		rf.snapshot = snapshot
		if rf.getLogLen()-1 >= args.LastIncludedIndex && rf.getLogEntry(args.LastIncludedIndex).Term == args.LastIncludedTerm {
			rf.log = rf.getLogSuffixCopy(args.LastIncludedIndex)
		} else {
			dummy := LogEntry{
				Term: args.LastIncludedTerm,
			}
			rf.log = []LogEntry{dummy}
		}
		rf.logOffset = args.LastIncludedIndex
		rf.persist()

		msg := ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  0,
			SnapshotValid: true,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		msg.Snapshot = make([]byte, len(args.Data))
		copy(msg.Snapshot, args.Data)
		DPrintf(dSnap, "S%d Apply Snapshot, LastI%d", rf.me, args.LastIncludedIndex)
		rf.applyIndex = max(rf.applyIndex, rf.logOffset)
		rf.snapshotToChan = true
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.snapshotToChan = false
		rf.cond.Broadcast()
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.role = follower
	}
	if rf.role == candidate && rf.currentTerm == args.Term {
		rf.role = follower
	}
	reply.Term = rf.currentTerm
	rf.persist()
}

