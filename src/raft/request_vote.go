package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) ticker() {
	for !rf.killed() {
		// election timeout, larger than [150, 300]
		electionTimeout := 850 + (rand.Int63() % 150)
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		rf.mu.Lock()
		isTimeout := time.Now().Sub(rf.lastValidRPCTimestamp).Milliseconds() > electionTimeout
		if (rf.role == follower && isTimeout) || rf.role == candidate {
			rf.role = candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCnt = 1
			DPrintf(dVote, "S%d Start Leader Election, T%d", rf.me, rf.currentTerm)
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.requestVoteWrapper(i, args)
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) requestVoteWrapper(i int, args RequestVoteArgs) {
	rf.mu.Lock()
	reply := RequestVoteReply{}
	rf.mu.Unlock()

	ok := false
	for !ok {
		DPrintf(dVote, "S%d -> S%d RequestVote, T%d", args.CandidateId, i, args.Term)
		ok = rf.sendRequestVote(i, &args, &reply)
		if rf.killed() {
			return
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.role = follower
	}

	ok = rf.currentTerm != args.Term
	ok = ok || (len(rf.log)-1 != args.LastLogIndex)
	ok = ok || (rf.log[len(rf.log)-1].Term != args.LastLogTerm)
	if ok {
		DPrintf(dVote, "S%d <- S%d RequestVote, Assume False, T%d <- T%d", rf.me, i, rf.currentTerm, reply.Term)
		return
	}

	if reply.VoteGranted {
		DPrintf(dVote, "S%d <- S%d RequestVote, Reply True, T%d <- T%d", rf.me, i, rf.currentTerm, reply.Term)
		if rf.voteCnt++; rf.voteCnt == len(rf.peers)/2+1 {
			rf.role = leader
			for i := range rf.peers {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			go rf.heartbeat()
		}
	} else {
		DPrintf(dVote, "S%d <- S%d RequestVote, Reply False, T%d <- T%d", rf.me, i, rf.currentTerm, reply.Term)
	}
}

