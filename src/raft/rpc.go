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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ok := false
	ok = ok || (rf.currentTerm > args.Term)
	ok = ok || (len(rf.log)-1 < args.PrevLogIndex)
	ok = ok || (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm)

	if len(args.Entries) != 0 {
		DPrintf(dLog, "S%d -> S%d AppendEntries, Log Before Append %v -> %v, PrevI%d PrevT%d", args.LeaderId, rf.me, args.Entries, rf.log, args.PrevLogIndex, args.PrevLogTerm)
	}

	reply.XTerm, reply.XIndex, reply.XLen = null, null, null
	if rf.currentTerm <= args.Term && len(rf.log)-1 < args.PrevLogIndex {
		reply.XLen = len(rf.log)
	}
	if rf.currentTerm <= args.Term && len(rf.log)-1 >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = sort.Search(args.PrevLogIndex+1, func(i int) bool {
			return rf.log[i].Term >= reply.XTerm
		})
	}

	if ok {
		reply.Success = false
	} else {
		rf.lastValidRPCTimestamp = time.Now()
		reply.Success = true
		for i := range args.Entries {
			if len(rf.log)-1 < args.PrevLogIndex+i+1 {
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
			if rf.log[args.PrevLogIndex+i+1].Term != args.Entries[i].Term {
				rf.log = rf.log[:args.PrevLogIndex+i+1]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
			rf.cond.Broadcast()
		}
	}
	if len(args.Entries) != 0 {
		DPrintf(dLog, "S%d -> S%d AppendEntries, Log After Append %v -> %v, PrevI%d PrevT%d", args.LeaderId, rf.me, args.Entries, rf.log, args.PrevLogIndex, args.PrevLogTerm)
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.role = follower
	}
	if rf.role == candidate && rf.currentTerm == args.Term {
		rf.role = follower
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ok := false
	ok = ok || (rf.currentTerm > args.Term)
	ok = ok || (rf.currentTerm == args.Term && rf.votedFor != null && rf.votedFor != args.CandidateId)
	lastIndex := len(rf.log) - 1
	ok = ok || (rf.log[lastIndex].Term > args.LastLogTerm)
	ok = ok || (rf.log[lastIndex].Term == args.LastLogTerm && lastIndex > args.LastLogIndex)

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
}

