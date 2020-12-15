package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []Log

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.NextIndex = rf.getLastLogIndex() + 1

	rf.toFollowerIfTermLowerThan(args.Term)

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.heartbeatCh <- true

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		return
	}

	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.log[i].Term != rf.log[i-1].Term {
				reply.Success = false
				reply.NextIndex = i
				return
			}
		}
	}

	reply.Success = true
	if len(args.Entries) > 0 {
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		rf.persist()
		reply.NextIndex = rf.getLastLogIndex() + 1
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.getLastLogIndex() {
			rf.commitIndex = rf.getLastLogIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		go rf.apply()
	}
}

const (
	Follower int = iota
	Candidate
	Leader
)

const VotedForNull = -1

type Log struct {
	Index   int
	Term    int
	Command interface{}
}

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	currentTerm int
	votedFor    int
	log         []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state     int
	voteCount int

	votedCh     chan bool
	toLeaderCh  chan bool
	heartbeatCh chan bool
	applyCh     chan ApplyMsg
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	_ = d.Decode(&rf.currentTerm)
	_ = d.Decode(&rf.votedFor)
	_ = d.Decode(&rf.log)
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

func (rf *Raft) toFollowerIfTermLowerThan(term int) {
	if term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = term
		rf.votedFor = VotedForNull
		rf.persist()
	}
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	rf.toFollowerIfTermLowerThan(args.Term)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false

	} else if rf.votedFor == VotedForNull || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.votedCh <- true
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && args.Term == rf.currentTerm && rf.state == Candidate {

		rf.toFollowerIfTermLowerThan(reply.Term)

		if reply.VoteGranted {
			rf.voteCount += 1

			if rf.voteCount > len(rf.peers)/2 {
				rf.state = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for peer := range rf.peers {
					rf.nextIndex[peer] = rf.getLastLogIndex() + 1
					rf.matchIndex[peer] = 0
				}
				rf.toLeaderCh <- true
			}
		}
	}
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := false
	if rf.state == Leader {
		term = rf.currentTerm
		rf.log = append(rf.log, Log{Term: rf.currentTerm, Command: command})
		rf.persist()
		isLeader = true
		index = rf.getLastLogIndex()
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) FollowerAction() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
}

func (rf *Raft) CandidateAction() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()

	for peer := range rf.peers {
		if peer != rf.me {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm:  rf.getLastLogTerm(),
			}
			go rf.sendRequestVote(peer, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			Index:   i,
			Command: rf.log[i].Command,
		}
		rf.lastApplied = i
	}
}

func (rf *Raft) LeaderAction() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.log[N].Term == rf.currentTerm; N-- {
		cnt := 1
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= N {
				cnt += 1
				if cnt > len(rf.peers)/2 {
					rf.commitIndex = N
					go rf.apply()
					break
				}
			}
		}
	}

	for peer := range rf.peers {
		if rf.me != peer {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[peer] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
				Entries:      rf.log[rf.nextIndex[peer]:],
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendAppendEntries(peer, args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && args.Term == rf.currentTerm && rf.state == Leader {

		rf.toFollowerIfTermLowerThan(reply.Term)

		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = reply.NextIndex - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

func timeAfter(t1 int, t2 int) <-chan time.Time {
	t := t1 + rand.Intn(t2-t1+1)
	return time.After(time.Duration(t) * time.Millisecond)
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.votedFor = VotedForNull
	rf.log = []Log{{Term: 0}}
	rf.heartbeatCh = make(chan bool)
	rf.votedCh = make(chan bool)
	rf.toLeaderCh = make(chan bool)
	rf.applyCh = applyCh

	rf.readPersist(persister.ReadRaftState())
	go func() {
		for {
			switch rf.state {
			case Follower:
				go rf.FollowerAction()
				select {
				case <-rf.votedCh:
				case <-rf.heartbeatCh:
				case <-timeAfter(150, 300):
					rf.state = Candidate
				}
			case Candidate:
				go rf.CandidateAction()
				select {
				case <-rf.votedCh:
				case <-rf.heartbeatCh:
					rf.state = Follower
				case <-rf.toLeaderCh:
				case <-timeAfter(150, 300):
				}
			case Leader:
				go rf.LeaderAction()
				select {
				case <-rf.votedCh:
				case <-rf.heartbeatCh:
				case <-timeAfter(50, 50):
				}
			}
		}
	}()

	return rf
}
