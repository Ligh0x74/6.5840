package raft

import (
	"sort"
	"time"
)

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != leader {
			DPrintf(dLeader, "S%d Not Leader Already, Exit Heartbeat, T%d", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		DPrintf(dLeader, "S%d Start Heartbeat, T%d, MatchI%v", rf.me, rf.currentTerm, rf.matchIndex)
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if len(rf.log)-1 < rf.nextIndex[i] {
				args.Entries = []LogEntry{}
			} else {
				args.Entries = []LogEntry{rf.log[rf.nextIndex[i]]}
			}
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].Term
			go rf.appendEntriesWrapper(i, args)
		}
		rf.mu.Unlock()

		ms := 150
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) appendEntriesWrapper(i int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}

	ok := false
	if len(args.Entries) == 0 {
		DPrintf(dLeader, "S%d -> S%d AppendEntries, HeartBeat, T%d", args.LeaderId, i, args.Term)
	} else {
		DPrintf(dLeader, "S%d -> S%d AppendEntries, Log, T%d, NextI%d", args.LeaderId, i, args.Term, args.PrevLogIndex+1)
	}
	ok = rf.sendAppendEntries(i, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.persist()
		rf.role = follower
	}
	if len(args.Entries) == 0 {
		DPrintf(dLeader, "S%d <- S%d AppendEntries, HeartBeat, T%d <- T%d", rf.me, i, rf.currentTerm, reply.Term)
	}

	ok = rf.currentTerm != args.Term
	if ok {
		DPrintf(dLeader, "S%d <- S%d AppendEntries, Log, Assume False, T%d = T%d, PrevI%d = PrevI%d", rf.me, i, rf.currentTerm, args.Term, rf.nextIndex[i]-1, args.PrevLogIndex)
		return
	}

	if reply.Success {
		DPrintf(dLeader, "S%d <- S%d AppendEntries, Log, Reply True, T%d <- T%d, NextI%d", rf.me, i, rf.currentTerm, reply.Term, rf.nextIndex[i])
		curNextIndex := args.PrevLogIndex + 1 + len(args.Entries)
		if rf.nextIndex[i] >= curNextIndex {
			return
		}
		rf.nextIndex[i] = curNextIndex
		rf.matchIndex[i] = rf.nextIndex[i] - 1

		// sort to find kth max, or use quick select
		match := make([]int, len(rf.matchIndex))
		copy(match, rf.matchIndex)
		DPrintf(dLeader, "S%d <- S%d %v", rf.me, i, match)
		sort.Slice(match, func(i, j int) bool {
			return match[i] > match[j]
		})
		n := match[len(rf.peers)/2+1-1]
		if n > rf.commitIndex && rf.log[n].Term == rf.currentTerm {
			rf.commitIndex = n
			rf.cond.Broadcast()
			DPrintf(dCommit, "S%d Advance CommitIndex, C%d", rf.me, rf.commitIndex)
		}
	} else {
		DPrintf(dLeader, "S%d <- S%d AppendEntries, Log, Reply False, T%d <- T%d, NextI%d", rf.me, i, rf.currentTerm, reply.Term, rf.nextIndex[i])
		if reply.XTerm != null {
			if rf.log[reply.XIndex].Term != reply.XTerm {
				rf.nextIndex[i] = reply.XIndex
			} else {
				j := reply.XIndex
				for ; j+1 < len(rf.log) && rf.log[j+1].Term == reply.XTerm; j++ {
				}
				rf.nextIndex[i] = j
			}
		} else {
			rf.nextIndex[i] = reply.XLen
		}

		args.LeaderCommit = rf.commitIndex
		if len(rf.log)-1 >= rf.nextIndex[i] {
			args.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[i]:]))
			copy(args.Entries, rf.log[rf.nextIndex[i]:])
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].Term
			go rf.appendEntriesWrapper(i, args)
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.role == leader
	if isLeader {
		log := LogEntry{
			Command: command,
			Term:    term,
		}
		DPrintf(dLeader, "S%d Start Log %v -> I%d, T%d", rf.me, log, len(rf.log), rf.currentTerm)
		rf.log = append(rf.log, log)
		rf.persist()
		rf.matchIndex[rf.me] = len(rf.log) - 1

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}
		for i := range rf.peers {
			if i == rf.me || (len(rf.log)-1 < rf.nextIndex[i]) {
				continue
			}
			args.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[i]:]))
			copy(args.Entries, rf.log[rf.nextIndex[i]:])
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].Term
			go rf.appendEntriesWrapper(i, args)
		}
	}
	return index, term, isLeader
}

