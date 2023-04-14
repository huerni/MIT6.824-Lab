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
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	state       string
	currentTerm int
	votedFor    int
	changeChan  chan int
	applyCh     chan ApplyMsg

	logEntries  []LogEntrie
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 当候选人记录更新时，才投票给它
		// 比较最后一个条目的索引和任期号，来决定哪个新。如果两个日志的任期号不同，任期号大的更新；如果任期号相同，更长的日志更新。
		loglen := len(rf.logEntries)
		if loglen == 1 || args.LastLogTerm > rf.logEntries[loglen-1].Term {
			reply.VoteGranted = true
			// 本轮已投票，不可再投给其他人
			// fmt.Printf("term %v, %v 投给 %v 一票\n", args.Term, rf.me, args.CandidateId)
			rf.votedFor = args.CandidateId
			rf.changeChan <- 1
		} else if args.LastLogTerm == rf.logEntries[loglen-1].Term && args.LastLogIndex >= rf.logEntries[loglen-1].Index {
			reply.VoteGranted = true
			// 本轮已投票，不可再投给其他人
			// fmt.Printf("term %v, %v 投给 %v 一票\n", args.Term, rf.me, args.CandidateId)
			rf.votedFor = args.CandidateId
			rf.changeChan <- 1
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
}

func (rf *Raft) ReceiveEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.state = Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.changeChan <- 1

	if len(rf.logEntries) <= args.PrevLogIndex || rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Success = true
	// 删除不匹配的现有条目
	if len(args.Entries) > 0 {
		for _, val := range args.Entries {
			if val.Index >= len(rf.logEntries) {
				break
			}
			// index相等，但Term不相等，截断
			if val.Term != rf.logEntries[val.Index].Term {
				rf.logEntries = rf.logEntries[:val.Index]
				break
			}
		}

		rf.logEntries = append(rf.logEntries, args.Entries...)
		fmt.Printf("%v 添加日志到 %v\n", rf.me, rf.logEntries[len(rf.logEntries)-1].Index)
	}

	// 将提交后还没有使用的日志应用于状态机
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.logEntries[len(rf.logEntries)-1].Index {
			rf.commitIndex = rf.logEntries[len(rf.logEntries)-1].Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		go rf.processMsg()
	}
}

func (rf *Raft) appendEntries() {
	// 修改成异步发送
	for index, _ := range rf.peers {
		if index != rf.me {
			go func(id int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				// 不断尝试，直到成功
				for {
					rf.mu.Lock()
					// FIXME: 为什么会出现等于0的情况？？
					if rf.nextIndex[id] > 0 {
						args.PrevLogIndex = rf.nextIndex[id] - 1
						args.PrevLogTerm = rf.logEntries[args.PrevLogIndex].Term
						// 需要同步的logEntries
						args.Entries = rf.logEntries[rf.nextIndex[id]:]
					}
					rf.mu.Unlock()
					ok := rf.peers[id].Call("Raft.ReceiveEntries", &args, &reply)
					if ok {
						if reply.Success == false {
							rf.mu.Lock()
							// 新leader存在，转为Follower
							if rf.currentTerm < reply.Term {
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								rf.state = Follower
								rf.changeChan <- 1
								rf.mu.Unlock()
								return
							}
							rf.nextIndex[id]--
							rf.mu.Unlock()
						} else {
							// Leader通过检查matchIndex中的信息从前到后统计哪个记录已经保存在大多数服务器上了，找到后将此记录前面还没提交的记录全部提交。但在这里要增加一个限制条件，Leader只能提交自己Term里面添加的记录(为了防止论文Figure 8的问题)。
							rf.mu.Lock()
							// 更新nextIndex和matchIndex
							if len(args.Entries) > 0 {
								rf.nextIndex[id] = args.Entries[len(args.Entries)-1].Index + 1
							}
							rf.matchIndex[id] = rf.nextIndex[id] - 1
							mapcount := make(map[int]int)
							// fmt.Printf("检测commit\n")
							for _, val := range rf.matchIndex {
								if mapcount[val] != 0 {
									mapcount[val]++
								} else {
									mapcount[val] = 1
								}
								// 不包括自己的matchIndex
								if val > rf.commitIndex && mapcount[val] >= len(rf.peers)/2 && rf.logEntries[val].Term == rf.currentTerm {
									rf.commitIndex = val
								}
							}
							go rf.processMsg()
							rf.mu.Unlock()
						}
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
			}(index)
		}
	}
}

func (rf *Raft) processMsg() {
	rf.mu.Lock()
	if rf.commitIndex > rf.lastApplied {
		fmt.Printf("%v 开始commit:  ", rf.me)
	}
	for k := rf.lastApplied + 1; k <= rf.commitIndex; k++ {
		fmt.Printf("%v, ", k)
		msg := ApplyMsg{CommandValid: true, Command: rf.logEntries[k].Command, CommandIndex: rf.logEntries[k].Index}
		rf.applyCh <- msg
	}
	if rf.commitIndex > rf.lastApplied {
		fmt.Printf("\n")
	}
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()

	time.Sleep(10 * time.Millisecond)
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
	term, isLeader = rf.GetState()
	rf.mu.Lock()
	index = len(rf.logEntries)
	if isLeader {
		// start the agreement
		entrie := LogEntrie{
			term,
			index,
			command,
		}
		rf.logEntries = append(rf.logEntries, entrie)
		fmt.Printf("%v 添加日志 %v\n", rf.me, index)
		// AppendEntrie发送给其他服务器
		rf.changeChan <- 1
	}
	rf.mu.Unlock()
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
	fmt.Printf("term:%v, %v 开始拉投票\n", rf.currentTerm, rf.me)
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.logEntries[len(rf.logEntries)-1].Index
	args.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
	rf.mu.Unlock()
	// 如果收到多数服务器的投票：成为领先者
	var voteCount int32 = 0
	atomic.AddInt32(&voteCount, 1)
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(id int, args *RequestVoteArgs) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(id, args, &reply)
			if !ok {
				return
			}
			if reply.VoteGranted {
				atomic.AddInt32(&voteCount, 1)
				rf.mu.Lock()
				if rf.state == Candidate && int(atomic.LoadInt32(&voteCount)) > len(rf.peers)/2 {
					fmt.Printf("term:%v, %v成为leader, 拉到%v张票\n", rf.currentTerm, rf.me, voteCount)
					// 成为领导者，立马发送心跳
					rf.state = Leader
					// 初始化nextIndex 和 matchIndex
					// nextIndex初始化为leader最后一个index+1
					for i := 0; i < len(rf.nextIndex); i++ {
						rf.nextIndex[i] = args.LastLogIndex + 1
					}
					for i := 0; i < len(rf.matchIndex); i++ {
						rf.matchIndex[i] = 0
					}
					rf.changeChan <- 1
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = Follower
					rf.changeChan <- 1
				}
				rf.mu.Unlock()
			}
		}(index, &args)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		switch rf.state {
		case Follower:
			select {
			case <-rf.changeChan:
			case <-time.After(time.Duration(150+rand.Int31n(150)) * time.Millisecond):
				{
					rf.mu.Lock()
					rf.state = Candidate
					rf.mu.Unlock()
				}
			}
		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.mu.Unlock()
			go rf.startElection()
			select {
			case <-rf.changeChan:
			case <-time.After(time.Duration(150+rand.Int31n(150)) * time.Millisecond):
			}
		case Leader:
			go rf.appendEntries()
			select {
			case <-rf.changeChan:
			case <-time.After(100 * time.Millisecond):
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
	rf.changeChan = make(chan int, 30)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastApplied = 0
	// initialization log[] 先添加0，减少判断条件
	rf.logEntries = append(rf.logEntries, LogEntrie{Term: 0, Index: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	fmt.Printf("%v 启动\n", rf.me)
	go rf.ticker()

	return rf
}
