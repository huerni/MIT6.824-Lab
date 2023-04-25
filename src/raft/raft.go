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
	"log"
	"math/rand"

	//	"bytes"

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
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

const (
	Leader    string = "leader"
	Follower  string = "follower"
	Candidate string = "candidate"
)

// A Go object implementing a single Raft peer.
type LogEntrie struct {
	Term    int
	Index   int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state        string
	currentTerm  int
	votedFor     int
	changeChan   chan int
	electionTime *time.Timer
	leaderTime   *time.Timer
	applyCh      chan ApplyMsg

	applyCond *sync.Cond

	logEntries  []LogEntrie
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	timestamp int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)

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
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)

	// fmt.Printf("persist:%v %v, %v, %v\n", rf.me, rf.currentTerm, rf.votedFor, len(rf.logEntries)-1)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	var currentTerm int
	var votedFor int
	var logEntries []LogEntrie
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logEntries) != nil {
		//   error...
		log.Fatal("error")
	} else {
		rf.mu.Lock()
		// fmt.Printf("readPersist:%v %v, %v, %v\n", rf.me, rf.currentTerm, rf.votedFor, len(rf.logEntries)-1)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logEntries = logEntries
		rf.mu.Unlock()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 任期
	CandidateId  int // 候选人ID
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
// 完成RequestVote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 当候选人记录更新时，才投票给它
		// 比较最后一个条目的索引和任期号，来决定哪个新。如果两个日志的任期号不同，任期号大的更新；如果任期号相同，更长的日志更新。
		loglen := len(rf.logEntries)
		if loglen == 1 || args.LastLogTerm > rf.logEntries[loglen-1].Term {
			rf.state = Follower
			reply.VoteGranted = true
			// 本轮已投票，不可再投给其他人
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.electionTime.Reset(time.Duration((rand.Int()%150)+300) * time.Millisecond)
		} else if args.LastLogTerm == rf.logEntries[loglen-1].Term && args.LastLogIndex >= loglen-1 {
			reply.VoteGranted = true
			rf.state = Follower
			// 本轮已投票，不可再投给其他人
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.electionTime.Reset(time.Duration((rand.Int()%150)+300) * time.Millisecond)
		}
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
	Entries      []LogEntrie
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

// FIXME:TestConcurrentStarts2B  TestUnreliableAgree2C
func (rf *Raft) ReceiveEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//curr := rf.timestamp
	rf.timestamp++
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.state = Follower
	rf.electionTime.Reset(time.Duration((rand.Int()%150)+300) * time.Millisecond)

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.persist()
	}

	// fmt.Printf("term:%v，%v接收心跳\n", rf.currentTerm, rf.me)
	if len(rf.logEntries) <= args.PrevLogIndex {
		reply.XIndex = -1
		reply.XTerm = -1
		reply.XLen = len(rf.logEntries)
		return
	}
	/**
		XTerm:  term in the conflicting entry (if any)
	    XIndex: index of first entry with that term (if any)
	    XLen:   log length
	**/
	if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XLen = -1
		reply.XTerm = rf.logEntries[args.PrevLogIndex].Term
		for index, val := range rf.logEntries {
			if val.Term == reply.XTerm {
				reply.XIndex = index
				break
			}
		}
		return
	}

	// Q: 为什么出现这种情况？可以快速复制吗？

	reply.Success = true
	reply.XLen = 0
	var lastlogIndex int
	if len(args.Entries) > 0 {
		lastlogIndex = args.Entries[len(args.Entries)-1].Index
	}

	if rf.lastApplied > args.PrevLogIndex || len(args.Entries) == 0 || (len(rf.logEntries) > lastlogIndex && rf.logEntries[lastlogIndex].Index == lastlogIndex && rf.logEntries[lastlogIndex].Term == args.Entries[len(args.Entries)-1].Term) {
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > len(rf.logEntries)-1 {
				rf.commitIndex = len(rf.logEntries) - 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			rf.persist()
			rf.applyCond.Broadcast()
		}
		return
	}

	// 删除不匹配的现有条目
	// // FIXME 最后一个测试 同步日志需从第一个不匹配日志开始，在这里时前面的日志都已经匹配
	// FIXME日志还会倒退？？ 为什么会重复接收同样的Entries  网络不稳定后发先至出错
	//fmt.Printf("timestamp[%v] %v: len:%v, next:%v ", curr, rf.me, len(args.Entries), args.Entries[0].Index)
	//for _, val := range args.Entries {
	//	fmt.Printf("[%v, %v], ", val.Index, val.Term)
	//}
	//fmt.Printf("\n")
	//fmt.Printf("timestamp[%v] before:%v: ", curr, rf.me)
	//for index, val := range rf.logEntries {
	//	fmt.Printf("[%v, %v], ", index, val.Term)
	//}
	//fmt.Printf("\n")
	logIndex := args.PrevLogIndex + 1
	insertIndex := 0
	for {
		if logIndex >= len(rf.logEntries) || insertIndex >= len(args.Entries) {
			break
		}
		if rf.logEntries[logIndex].Term != args.Entries[insertIndex].Term {
			rf.logEntries = rf.logEntries[:logIndex]
			break
		}
		logIndex++
		insertIndex++
	}
	reply.XLen = len(args.Entries) - insertIndex
	if reply.XLen > 0 {
		rf.logEntries = append(rf.logEntries, args.Entries[insertIndex:]...)
		rf.persist()
		//fmt.Printf("timestamp[%v] %v: len:%v, next:%v ", curr, rf.me, reply.XLen, args.Entries[0].Index)
		//for index, val := range rf.logEntries {
		//	fmt.Printf("[%v, %v], ", index, val.Term)
		//}
		//fmt.Printf("\n")
	} else {
		//fmt.Printf("timestamp[%v] %v:len is 0\n", curr, rf.me)
	}

	// fmt.Printf("%v 日志添加到 %v\n", rf.me, len(rf.logEntries)-1)

	// 将提交后还没有使用的日志应用于状态机
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logEntries)-1 {
			rf.commitIndex = len(rf.logEntries) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.persist()
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) appendEntries() {
	rf.mu.Lock()
	//curr := rf.timestamp
	rf.timestamp++
	rf.mu.Unlock()
	// 修改成异步发送
	for index, _ := range rf.peers {
		if index != rf.me {
			go func(id int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				nextIndex := rf.nextIndex[id]
				args.PrevLogIndex = rf.nextIndex[id] - 1
				args.PrevLogTerm = rf.logEntries[args.PrevLogIndex].Term
				// 需要同步的logEntries  切片应该是深拷贝
				if nextIndex < len(rf.logEntries) {
					args.Entries = append([]LogEntrie{}, rf.logEntries[nextIndex:]...)
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.peers[id].Call("Raft.ReceiveEntries", &args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
						rf.persist()
						return
					}

					if rf.currentTerm != args.Term || rf.state != Leader {
						return
					}

					if reply.Success == true {
						// Leader通过检查matchIndex中的信息从前到后统计哪个记录已经保存在大多数服务器上了，找到后将此记录前面还没提交的记录全部提交。但在这里要增加一个限制条件，Leader只能提交自己Term里面添加的记录(为了防止论文Figure 8的问题)。
						// 更新nextIndex和matchIndex
						//rf.nextIndex[id] += reply.XLen
						//fmt.Printf("timestamp[%v] reply %v next:%v\n", curr, id, rf.nextIndex[id])
						//rf.matchIndex[id] = rf.nextIndex[id] - 1
						rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[id] = rf.matchIndex[id] + 1
						mapcount := make(map[int]int)
						for index, val := range rf.matchIndex {
							if index == rf.me {
								continue
							}
							mapcount[val]++
							// 不包括自己的matchIndex
							if val > rf.commitIndex && mapcount[val] >= len(rf.peers)/2 && rf.logEntries[val].Term == rf.currentTerm {
								rf.commitIndex = val
								rf.applyCond.Broadcast()
								break
							}
						}
					} else {
						/**
						  Case 1: leader doesn't have XTerm:
						    nextIndex = XIndex
						  Case 2: leader has XTerm:
						    nextIndex = leader's last entry for XTerm ??
						  Case 3: follower's log is too short:
						    nextIndex = XLen
						**/
						if reply.XLen != -1 {
							rf.nextIndex[id] = reply.XLen
						} else if reply.XTerm != -1 {
							hasTerm := false
							for i := len(rf.logEntries) - 1; i >= 0; i-- {
								if rf.logEntries[i].Term == reply.XTerm {
									rf.nextIndex[id] = i + 1
									hasTerm = true
									break
								} else if rf.logEntries[i].Term < reply.XTerm {
									break
								}
							}
							if hasTerm == false {
								rf.nextIndex[id] = reply.XIndex
							}
						}

						if rf.nextIndex[id] < 1 {
							rf.nextIndex[id] = 1
						}
					}
				}
			}(index)
		}
	}
}

func (rf *Raft) processMsg() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		msgs := []ApplyMsg{}
		for k := rf.lastApplied + 1; k <= rf.commitIndex; k++ {
			msg := ApplyMsg{CommandValid: true, Command: rf.logEntries[k].Command, CommandIndex: rf.logEntries[k].Index}
			msgs = append(msgs, msg)
		}
		// log.Printf("%v 日志commit到 %v", rf.me, len(rf.logEntries)-1)
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
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
// first code
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.killed() {
		return -1, -1, isLeader
	}

	index = len(rf.logEntries)
	term = rf.currentTerm
	entrie := LogEntrie{
		term,
		index,
		command,
	}
	rf.logEntries = append(rf.logEntries, entrie)
	rf.persist()
	//fmt.Printf("timestamp[%v] term: %v, %v 添加日志 %v\n", rf.timestamp, rf.currentTerm, rf.me, len(rf.logEntries)-1)

	// AppendEntrie发送给其他服务器
	go rf.appendEntries()

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 什么时候更新voteFor为-1？  term增加的时候
func (rf *Raft) startElection() {
	rf.mu.Lock()

	// 开始选举
	if rf.state == Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}

	rf.state = Candidate
	rf.currentTerm++
	// fmt.Printf("term:%v, %v 开始选举\n", rf.currentTerm, rf.me)
	rf.votedFor = rf.me
	rf.persist()
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.logEntries) - 1
	args.LastLogTerm = rf.logEntries[args.LastLogIndex].Term
	var voteCount int32 = 0
	atomic.AddInt32(&voteCount, 1)
	rf.mu.Unlock()
	// 如果收到多数服务器的投票：成为领先者
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(id int, args *RequestVoteArgs) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(id, args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = Follower
					rf.persist()
					return
				}

				if rf.currentTerm != args.Term || rf.state != Candidate {
					return
				}

				if reply.VoteGranted {
					atomic.AddInt32(&voteCount, 1)
					if int(atomic.LoadInt32(&voteCount)) > len(rf.peers)/2 {
						// 成为领导者，立马发送心跳
						// fmt.Printf("term:%v, %v 成为领导者\n", rf.currentTerm, rf.me)
						rf.state = Leader
						// 初始化nextIndex 和 matchIndex
						// nextIndex初始化为leader最后一个index+1
						for i := 0; i < len(rf.nextIndex); i++ {
							rf.nextIndex[i] = args.LastLogIndex + 1
						}
						for i := 0; i < len(rf.matchIndex); i++ {
							rf.matchIndex[i] = 0
						}
						go rf.appendEntries()
					}
				}
			}
		}(index, &args)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTime.C:
			rf.mu.Lock()
			rf.electionTime.Reset(time.Duration((rand.Int()%150)+300) * time.Millisecond)
			rf.mu.Unlock()
			go rf.startElection()
		case <-rf.leaderTime.C:
			_, isLeader := rf.GetState()
			rf.mu.Lock()
			rf.leaderTime.Reset(time.Duration(50 * time.Millisecond))
			rf.mu.Unlock()
			if isLeader {
				go rf.appendEntries()
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(10 * time.Millisecond)
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastApplied = 0
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.electionTime = time.NewTimer(time.Duration((rand.Int()%150)+300) * time.Millisecond)
	rf.leaderTime = time.NewTimer(50 * time.Millisecond)
	// initialization log[] 先添加0，减少判断条件
	entrie := LogEntrie{
		Term:  0,
		Index: 0,
	}
	rf.logEntries = append(rf.logEntries, entrie)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.timestamp = 0
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.processMsg()

	return rf
}
