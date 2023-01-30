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
	//	"bytes"

	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type ServerStateEnum int

const (
	Leader ServerStateEnum = iota
	Candidate
	Follower
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// common state
	state       ProcessableState
	currentTerm int
	votedFor    int
	log         []LogItem
	lastLogTerm int

	// violatile state
	commitIndex int
	lastApplied int

	// leader state

	// follow state
	// electionTimeout time.Duration

	//state singleton
	followerState  *FollowerState
	candidateState *CandidateState
	leaderState    *LeaderState

	appendEntriesReqCh chan localAppendEntriesArgs
	requestVoteReqCh   chan localRequestVoteArgs
}

type ServerState struct {
	State          ServerStateEnum
	rf             *Raft
	killedCh       chan int8
	stateChangedCh chan int8
}

func (s *ServerState) isStateChanged() bool {
	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()
	return reflect.ValueOf(s.rf.state).Elem().FieldByName("State").Interface().(ServerStateEnum) != s.State
}

type ProcessableState interface {
	processEvents()
}

type FollowerState struct {
	*ServerState
	lastHeartBeatTime time.Time
}

type CandidateState struct {
	*ServerState
	voteResult chan *RequestVoteReply
}

type LeaderState struct {
	*ServerState
	nextIndex    []int
	matchIndex   []int
	appendResult chan *AppendEntriesReply
}

type LogItem struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = reflect.ValueOf(rf.state).Elem().FieldByName("State").Interface().(ServerStateEnum) == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term    int
	Granted bool
	// Your data here (2A).
}

type localRequestVoteArgs struct {
	RequestVoteArgs
	replyCh chan RequestVoteReply
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	replyCh := make(chan RequestVoteReply, 1)
	req := localRequestVoteArgs{RequestVoteArgs: *args, replyCh: replyCh}
	fmt.Printf("%v handling RequestVote %v\n", rf.me, *args)
	// fmt.Printf("%v handling RequestVote %v, spawning goroutine\n", rf.me, *args)

	go func() {
		rf.requestVoteReqCh <- req
		fmt.Printf("%v add to reqChan %v, chan %v\n", rf.me, req, rf.requestVoteReqCh)
	}()

	localReply := <-replyCh
	fmt.Printf("%v receives from local reply chan res %v\n", rf.me, localReply)

	reply.Granted = localReply.Granted
	reply.Term = localReply.Term
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogItem
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type localAppendEntriesArgs struct {
	AppendEntriesArgs
	replyCh chan AppendEntriesReply
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	replyCh := make(chan AppendEntriesReply, 1)
	req := localAppendEntriesArgs{AppendEntriesArgs: *args, replyCh: replyCh}
	rf.appendEntriesReqCh <- req
	localReply := <-replyCh
	reply.Success = localReply.Success
	reply.Term = localReply.Term
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Printf("%v requests vote from %v for term %v\n", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	fmt.Printf("%v receives vote response OK=%v from %v, %v for term %v\n", rf.me, ok, server, *reply, args.Term)

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// now := time.Now()
		// if d := now.Sub(rf.lastPingTime).Milliseconds(); d > rf.electionTimeout {
		// 	rf.changeState(Candidate)
		// }
		rf.state.processEvents()
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

func (rf *Raft) toCandidateState(term int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	rf.currentTerm = term
	rf.votedFor = rf.me
	rf.candidateState.killedCh = make(chan int8, 1)
	rf.candidateState.stateChangedCh = make(chan int8, 1)
	rf.state = rf.candidateState
	// rf.candidateState.startElection()
}

func (rf *Raft) toFollowerState(term int) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	rf.currentTerm = term
	rf.followerState.lastHeartBeatTime = time.Now()
	rf.votedFor = -1
	rf.state = rf.followerState

}

func (rf *Raft) toLeaderState() {
	rf.leaderState.matchIndex = make([]int, len(rf.peers))

	rf.leaderState.nextIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.leaderState.nextIndex[i] = len(rf.log) + 1
	}
	rf.leaderState.appendResult = make(chan *AppendEntriesReply)
	rf.leaderState.killedCh = make(chan int8, 1)
	rf.leaderState.stateChangedCh = make(chan int8, 1)
	rf.state = rf.leaderState
}

func (f *FollowerState) processEvents() {
	rf := f.rf
	electionTimeout := time.NewTimer(rf.getElectionTimeout())
	defer electionTimeout.Stop()
	// for _ = range electionTimeout.C {
	// 	rf.toCandidateState()
	// }
	fmt.Printf("%v is in follower state, process Events, for term %v\n", rf.me, rf.currentTerm)

	for {
		select {
		case <-electionTimeout.C:
			rf.toCandidateState(rf.currentTerm + 1)
			return
			// TODO case AppendLogs
		case voteReq := <-rf.requestVoteReqCh:
			fmt.Printf("%v is a follower, process voteReq %v\n", rf.me, voteReq)
			if voteReq.Term > rf.currentTerm {
				fmt.Printf("%v term is lagging, currently at %v, inc to %v\n", rf.me, rf.currentTerm, voteReq.Term)
				rf.currentTerm = voteReq.Term
				rf.votedFor = -1

			}
			if rf.canVoteFor(voteReq) {
				fmt.Printf("%v vote for %v at term %v\n", rf.me, voteReq.CandidateId, rf.currentTerm)

				voteReq.replyCh <- RequestVoteReply{Term: rf.currentTerm, Granted: true}
				rf.votedFor = voteReq.CandidateId
				electionTimeout.Reset(rf.getElectionTimeout())
			} else {
				fmt.Printf("%v REFUSE to vote for %v for term %v, bc voted for %v at term %v\n", rf.me, voteReq.CandidateId, voteReq.Term, rf.votedFor, rf.currentTerm)

				voteReq.replyCh <- RequestVoteReply{Term: rf.currentTerm, Granted: false}
			}
		case appendEntriesReq := <-rf.appendEntriesReqCh:
			if appendEntriesReq.Term < rf.currentTerm { // leader outdated
				appendEntriesReq.replyCh <- AppendEntriesReply{Success: false, Term: rf.currentTerm}
			} else {
				if appendEntriesReq.Term > rf.currentTerm {
					rf.currentTerm = appendEntriesReq.Term
					rf.votedFor = -1
				} else if !rf.canAppendEntries(appendEntriesReq) {
					appendEntriesReq.replyCh <- AppendEntriesReply{Success: false, Term: rf.currentTerm}
				} else {
					// TODO do append entries

				}
				electionTimeout.Reset(rf.getElectionTimeout())
			}
		}
	}
}

func (c *CandidateState) startElection() {
	rf := c.rf
	req := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.log), LastLogTerm: rf.lastLogTerm}

	for i := range rf.peers {
		if i != rf.me {
			res := &RequestVoteReply{} //TODO
			go func(i int) {
				if rf.sendRequestVote(i, req, res) && !rf.killed() {
					c.voteResult <- res
				}
			}(i)
		}
	}
	// go c.processElectionResult()
	go c.watchKilledAndState()
}

func (c *CandidateState) processEvents() {
	rf := c.rf
	voteCnt := 1
	fmt.Printf("%v is in Candidate state, process Events, for term %v\n", rf.me, rf.currentTerm)
	rf.candidateState.startElection()

	electionTimeout := time.NewTimer(rf.getElectionTimeout())
	defer electionTimeout.Stop()

	for {
		select {
		case <-c.killedCh:
			return
		// case <-c.stateChangedCh:
		// 	go func() {
		// 		for {
		// 			select {
		// 			case <-c.voteResult:
		// 			default:
		// 				return
		// 			}
		// 		}
		// 	}()
		// 	return
		case vote := <-c.voteResult:
			fmt.Printf("%v receives vote response %v\n", rf.me, vote)
			if vote.Term > rf.currentTerm {
				// rf.mu.Lock()
				// defer rf.mu.Unlock()
				fmt.Printf("%v is at term %v, but now is term %v, converting to follower from Candidate\n", rf.me, rf.currentTerm, vote.Term)

				rf.toFollowerState(vote.Term)
				return
			} else if vote.Term == rf.currentTerm && vote.Granted {
				voteCnt += 1
				if voteCnt >= len(rf.peers)/2+1 {

					rf.toLeaderState()
					fmt.Printf("%v to leader for term %v\n", rf.me, rf.currentTerm)

					return
				}
			}
		case <-electionTimeout.C:
			fmt.Printf("Candidate %v elec timeout for term %v, moving to term %v\n", rf.me, rf.currentTerm, rf.currentTerm+1)
			rf.toCandidateState(rf.currentTerm + 1)

			return
		case voteReq := <-rf.requestVoteReqCh:
			fmt.Printf("%v is a Candidate, process voteReq %v\n", rf.me, voteReq)

			if voteReq.Term > rf.currentTerm {
				rf.toFollowerState(voteReq.Term)
				fmt.Printf("%v is a Candidate, process voteReq %v before send to channel\n", rf.me, voteReq)
				go func() {
					rf.requestVoteReqCh <- voteReq
				}()
				fmt.Printf("%v is a Candidate, process voteReq %v after send to channel\n", rf.me, voteReq)

				return
			} else {
				voteReq.replyCh <- RequestVoteReply{Term: rf.currentTerm, Granted: false}
			}
		case appendEntriesReq := <-rf.appendEntriesReqCh:
			if appendEntriesReq.Term >= rf.currentTerm {
				rf.toFollowerState(appendEntriesReq.Term)

				go func() {
					rf.appendEntriesReqCh <- appendEntriesReq
				}()
				return
			} else {
				appendEntriesReq.replyCh <- AppendEntriesReply{Success: false, Term: rf.currentTerm}
			}
		}
	}

}

func (c *ServerState) watchKilledAndState() {
	rf := c.rf
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for _ = range ticker.C {

		if rf.killed() {
			c.killedCh <- 1
			return
		}
		if c.isStateChanged() {
			c.stateChangedCh <- 1
			return
		}

	}
}

func (l *LeaderState) heartBeat() {
	rf := l.rf
	pulse := time.NewTicker(time.Millisecond * 100)
	defer pulse.Stop()
	for {
		select {
		case <-pulse.C:
			for i := range rf.peers {
				req := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: l.nextIndex[i] - 1, PrevLogTerm: rf.lastLogTerm, LeaderCommit: rf.commitIndex} // TODO
				res := &AppendEntriesReply{}
				if i != rf.me {
					go func(i int) {
						if rf.sendAppendEntries(i, req, res) && !rf.killed() {
							l.appendResult <- res
						}
					}(i)
				}
			}
		case killed := <-l.killedCh:
			l.killedCh <- killed
			return
		case changed := <-l.stateChangedCh:
			l.stateChangedCh <- changed
			return
		}
	}
}

func (l *LeaderState) processEvents() {
	go l.heartBeat()
	rf := l.rf
	fmt.Printf("%v is in Leader state, process Events, for term %v\n", rf.me, rf.currentTerm)

	for {
		select {
		case killed := <-l.killedCh:
			l.killedCh <- killed
			return
		case changed := <-l.stateChangedCh:
			l.stateChangedCh <- changed
			return
		case res := <-l.appendResult:
			if res.Term > rf.currentTerm {
				rf.toFollowerState(res.Term)
				return
			}
			// TODO decrease nextIndex
		case voteReq := <-rf.requestVoteReqCh:
			fmt.Printf("%v is leader for term %v, but now asked to vote %v for term %v", rf.me, rf.currentTerm, voteReq.CandidateId, voteReq.Term)

			if voteReq.Term > rf.currentTerm {
				fmt.Printf("%v is leader for term %v, but now asked to vote %v for term %v, converting to follower", rf.me, rf.currentTerm, voteReq.CandidateId, voteReq.Term)
				rf.toFollowerState(voteReq.Term)
				fmt.Printf("%v was a leader, process voteReq %v before send to channel\n", rf.me, voteReq)

				go func() {
					rf.requestVoteReqCh <- voteReq
					fmt.Printf("%v was a Candidate, process voteReq %v after send to channel\n", rf.me, voteReq)
				}()
				return
			} else {
				voteReq.replyCh <- RequestVoteReply{Term: rf.currentTerm, Granted: false}
			}
		case appendEntriesReq := <-rf.appendEntriesReqCh:
			if appendEntriesReq.Term > rf.currentTerm {
				rf.toFollowerState(appendEntriesReq.Term)
				go func() {
					rf.appendEntriesReqCh <- appendEntriesReq
				}()
				return
			} else {
				appendEntriesReq.replyCh <- AppendEntriesReply{Success: false, Term: rf.currentTerm}
			}
		}
	}
}

func (rf *Raft) canVoteFor(req localRequestVoteArgs) bool {
	termCheck := rf.currentTerm == req.Term
	notVotedCheck := rf.votedFor == -1 || rf.votedFor == req.CandidateId
	upToDateCheck := rf.lastLogTerm < req.LastLogTerm || (rf.lastLogTerm == req.LastLogTerm && len(rf.log) <= req.LastLogIndex)
	return termCheck && notVotedCheck && upToDateCheck
}

func (rf *Raft) canAppendEntries(req localAppendEntriesArgs) bool {
	termCheck := req.Term == rf.currentTerm
	var logCheck bool
	if req.PrevLogIndex == 0 {
		logCheck = len(rf.log) == 0
	} else {
		logCheck = len(rf.log) >= req.PrevLogIndex && rf.log[req.PrevLogIndex-1].Term == req.PrevLogTerm
	}
	return termCheck && logCheck
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

	// Your initialization code here (2A, 2B, 2C).
	rf.initServerState()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (r *Raft) initServerState() {
	r.currentTerm = 0
	r.lastLogTerm = 0
	r.votedFor = -1
	r.commitIndex = 0
	r.lastApplied = 0
	r.leaderState = &LeaderState{ServerState: &ServerState{rf: r, State: Leader}}
	r.candidateState = &CandidateState{ServerState: &ServerState{rf: r, State: Candidate}, voteResult: make(chan *RequestVoteReply)}
	r.followerState = &FollowerState{ServerState: &ServerState{State: Follower, rf: r}, lastHeartBeatTime: time.Now()}
	// r.state = r.followerState
	r.requestVoteReqCh = make(chan localRequestVoteArgs)
	r.appendEntriesReqCh = make(chan localAppendEntriesArgs)
	r.toFollowerState(0)

}

func (r *Raft) getElectionTimeout() time.Duration {
	ms := 500 + rand.Intn(500)
	return time.Millisecond * time.Duration(ms)
}

// func (r *Raft) changeState(s ServerStateEnum) {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()
// 	r.state = s
// }
