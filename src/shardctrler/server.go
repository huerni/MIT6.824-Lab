package shardctrler

import (
	"fmt"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastSequentId map[int64]int
	notifyCh      map[int]chan Op
	configs       []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Operator   string
	ClientId   int64
	SequenceId int

	Shard int
	GID   int

	Servers map[int][]string

	GIDs []int
}

// TODO: 打log
func (sc *ShardCtrler) applyMsg() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			sc.mu.Lock()
			lastid, ok := sc.lastSequentId[op.ClientId]
			if !ok || op.SequenceId > lastid {
				sc.lastSequentId[op.ClientId] = op.SequenceId
				switch op.Operator {
				case "Join":
					sc.joinConfig(op.Servers)
				case "Leave":
					sc.leaveConfig(op.GIDs)
				case "Move":
					sc.moveConfig(op.Shard, op.GID)
				}
			}
			val, ok := sc.notifyCh[msg.CommandIndex]
			sc.mu.Unlock()
			if ok {
				val <- op
			}
		}
	}
}

func (sc *ShardCtrler) joinConfig(Servers map[int][]string) {
	config := Config{
		Num: len(sc.configs),
	}
	config.Groups = make(map[int][]string)
	for key, val := range Servers {
		config.Groups[key] = val
	}
	for key, val := range sc.configs[len(sc.configs)-1].Groups {
		config.Groups[key] = val
	}
	// NShards个shards平均分配给gids
	shardsGidNum := NShards / len(config.Groups)
	id := 0
	keys := make(map[int]bool)
	for key := range config.Groups {
		_, ok := keys[key]
		if !ok {
			keys[key] = true
			right := id + shardsGidNum
			for ; id < NShards && id < right; id++ {
				config.Shards[id] = key
			}
		}
	}
	if id < NShards {
		for key := range keys {
			if id >= NShards {
				break
			}
			config.Shards[id] = key
			id++
		}
	}
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// 1. 判断是否是leader
	reply.WrongLeader = true
	sc.mu.Lock()
	op := Op{Operator: "Join", ClientId: args.ClientId, SequenceId: args.SequenceId}
	op.Servers = make(map[int][]string)
	for key, val := range args.Servers {
		op.Servers[key] = val
	}
	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		sc.mu.Unlock()
		return
	}

	//fmt.Printf("[me:%v]Join cid: %v, sid: %v\n", sc.me, args.ClientId, args.SequenceId)
	// 2. 添加日志，等待日志成功提交，超时重传
	sc.notifyCh[index] = make(chan Op, 1)
	sc.mu.Unlock()
	select {
	case val := <-sc.notifyCh[index]:
		if val.ClientId == args.ClientId && val.SequenceId == args.SequenceId {
			reply.WrongLeader = false
		}

	case <-time.After(350 * time.Millisecond):

	}

	sc.mu.Lock()
	delete(sc.notifyCh, index)
	sc.mu.Unlock()
}

// FIXME: shard 5 -> invalid group 1
func (sc *ShardCtrler) leaveConfig(GIDs []int) {
	// 移除gids，重新分配分片
	config := Config{
		Num:    len(sc.configs),
		Shards: sc.configs[len(sc.configs)-1].Shards,
	}
	config.Groups = make(map[int][]string)
	for key, val := range sc.configs[len(sc.configs)-1].Groups {
		config.Groups[key] = val
	}
	for _, gid := range GIDs {
		delete(config.Groups, gid)
	}
	// 剩下均匀分配
	remainGids := make([]int, len(config.Groups))
	index := 0
	keys := make(map[int]bool)
	for key := range config.Groups {
		_, ok := keys[key]
		if !ok {
			remainGids[index] = key
			index++
			keys[key] = true
		}
	}
	index = 0
	for _, val := range config.Shards {
		_, ok := config.Groups[val]
		if !ok {
			val = remainGids[index]
			index = (index + 1) % len(remainGids)
		}
	}
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader = true
	sc.mu.Lock()

	index, _, isLeader := sc.rf.Start(Op{
		Operator:   "Leave",
		ClientId:   args.ClientId,
		SequenceId: args.SequenceId,
		GIDs:       args.GIDs,
	})

	if !isLeader {
		sc.mu.Unlock()
		return
	}
	fmt.Printf("[me:%v]Leave cid: %v, sid: %v\n", sc.me, args.ClientId, args.SequenceId)
	sc.notifyCh[index] = make(chan Op, 1)
	sc.mu.Unlock()

	select {
	case val := <-sc.notifyCh[index]:
		if val.ClientId == args.ClientId && val.SequenceId == args.SequenceId {
			reply.WrongLeader = false
		}
	case <-time.After(350 * time.Millisecond):
	}

	sc.mu.Lock()
	delete(sc.notifyCh, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) moveConfig(Shard int, GID int) {
	config := Config{
		Num:    len(sc.configs),
		Shards: sc.configs[len(sc.configs)-1].Shards,
	}
	for key, val := range sc.configs[len(sc.configs)-1].Groups {
		config.Groups[key] = val
	}
	config.Shards[Shard] = GID
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader = true
	sc.mu.Lock()

	index, _, isLeader := sc.rf.Start(Op{
		Operator:   "Move",
		ClientId:   args.ClientId,
		SequenceId: args.SequenceId,
		Shard:      args.Shard,
		GID:        args.GID,
	})

	if !isLeader {
		sc.mu.Unlock()
		return
	}
	//fmt.Printf("[me:%v]move cid: %v, sid: %v\n", sc.me, args.ClientId, args.SequenceId)
	sc.notifyCh[index] = make(chan Op, 1)
	sc.mu.Unlock()

	select {
	case val := <-sc.notifyCh[index]:
		if val.ClientId == args.ClientId && val.SequenceId == args.SequenceId {
			reply.WrongLeader = false
		}
	case <-time.After(350 * time.Millisecond):
	}

	sc.mu.Lock()
	delete(sc.notifyCh, index)
	sc.mu.Unlock()
}

// 返回指定配置或最新配置，其中在此查询之前的join等操作必须apply才能返回
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// 1. 判断是否是leader
	reply.WrongLeader = true
	sc.mu.Lock()
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		sc.mu.Unlock()
		return
	}
	// 2. 查看配置号是否是-1或高于最大配置，否直接返回对应配置
	if args.Num > -1 && args.Num < len(sc.configs) {
		reply.Config = sc.configs[args.Num]
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}
	// 3. 是的话确保此前所有操作apply后，返回配置
	index, _, isLeader := sc.rf.Start(Op{
		Operator:   "Query",
		ClientId:   args.ClientId,
		SequenceId: args.SequenceId,
	})
	if !isLeader {
		sc.mu.Unlock()
		return
	}
	//fmt.Printf("[me:%v]Query cid: %v, sid: %v\n", sc.me, args.ClientId, args.SequenceId)
	sc.notifyCh[index] = make(chan Op, 1)
	sc.mu.Unlock()

	select {
	case val := <-sc.notifyCh[index]:
		sc.mu.Lock()
		if val.ClientId == args.ClientId && val.SequenceId == args.SequenceId {
			if args.Num == -1 || args.Num >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[args.Num]
			}
			reply.WrongLeader = false
		}
		sc.mu.Unlock()
	case <-time.After(350 * time.Millisecond):

	}

	sc.mu.Lock()
	delete(sc.notifyCh, index)
	sc.mu.Unlock()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	// 初始为空，配置编号为0
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastSequentId = make(map[int64]int)
	sc.notifyCh = make(map[int]chan Op)
	go sc.applyMsg()
	return sc
}
