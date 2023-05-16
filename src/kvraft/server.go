package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Op应该是对应操作，每次用来修改日志中的值
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Opera    string
	ClientId int64
	SerialId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdata       map[string]string
	notifyCh     map[int]chan Op
	lastSerialId map[int64]int
}

// 直接从map中查询？？ 会查询到过时的数据  需要判断数据是否过期
// 频繁更换Leader，导致get到过期数据  此时没有commit？？
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() == false {
		reply.Value = ""
		reply.Err = ErrWrongLeader
		op := Op{Key: args.Key, Opera: "Get", ClientId: args.ClientId, SerialId: args.SerialId}

		kv.mu.Lock()
		// 需要判断数据是否过期后，再从日志中找
		index, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			kv.mu.Unlock()
			return
		}

		// 当为小分区leader时，get不应该返回数据，需要判断是否为小分区leader
		// Leader不断变换时，刚换leader就get，有的状态机apply完，导致数据是过时的
		if kv.rf.SendGetBeforeHeartBeat() == false {
			kv.mu.Unlock()
			return
		}

		reply.LeaderId = kv.me

		kv.notifyCh[index] = make(chan Op, 1)
		kv.mu.Unlock()
		select {
		case val := <-kv.notifyCh[index]:
			kv.mu.Lock()
			delete(kv.notifyCh, index)
			if val.ClientId == args.ClientId && val.SerialId == args.SerialId {
				reply.Err = OK
			}

			kv.mu.Unlock()
		case <-time.After(400 * time.Millisecond):
		}

		kv.mu.Lock()
		if val, ok := kv.kvdata[args.Key]; ok {
			reply.Err = OK
			reply.Value = val
			//fmt.Printf("[id:%v, term:%v] get key:%v OK\n", kv.me, kv.currentTerm, args.Key)
		} else {
			//fmt.Printf("[id:%v, term:%v] get key:%v noKey\n", kv.me, kv.currentTerm, args.Key)
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	}
}

// 应该将所有commit的日志全部apply
func (kv *KVServer) applyMsg() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.mu.Lock()
			// 判断是否重复提交
			lastid, ok := kv.lastSerialId[op.ClientId]
			if !ok || op.SerialId > lastid {
				kv.lastSerialId[op.ClientId] = op.SerialId
				if op.Opera == "Put" {
					kv.kvdata[op.Key] = op.Value
					//fmt.Printf("[id:%v, term:%v] put key:%v, index:%v, last value:%v\n", kv.me, kv.currentTerm, op.Key, msg.CommandIndex, op.Value)
				} else if op.Opera == "Append" {
					kv.kvdata[op.Key] = kv.kvdata[op.Key] + op.Value
					//fmt.Printf("[id:%v, term:%v] append key:%v, index: %v, last value:%v\n", kv.me, kv.currentTerm, op.Key, msg.CommandIndex, op.Value)
				}
			}
			// 通知
			val, ok := kv.notifyCh[msg.CommandIndex]
			kv.mu.Unlock()
			if ok {
				val <- op
			}
		}
	}
}

// 每次调用 Clerk.Put() 或 Clerk.Append() 应该只有一次执行，因此你需要确保重新发送不会导致服务器执行请求两次。
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() == false {
		kv.mu.Lock()

		op := Op{Key: args.Key, Value: args.Value, Opera: args.Op, ClientId: args.ClientId, SerialId: args.SerialId}

		reply.Err = ErrWrongLeader

		// 在commit之后，没有存入map之前，可能多次重复提交
		// 重发机制
		index, _, isLeader := kv.rf.Start(op)

		if !isLeader {
			kv.mu.Unlock()
			return
		}
		reply.LeaderId = kv.me

		kv.notifyCh[index] = make(chan Op, 1)
		kv.mu.Unlock()

		// 换leader时，新leader里面的msg没有读取出来，应该在成为leader之后，将chan里面的信息读取出来到non-op  只在切换Leader时，可用term判断是否切换了leader
		//fmt.Printf("before [id:%v, term:%v] put key:%v, index:%v, last value:%v\n", kv.me, kv.currentTerm, op.Key, index, op.Value)
		select {
		case val := <-kv.notifyCh[index]:
			kv.mu.Lock()
			delete(kv.notifyCh, index)
			kv.mu.Unlock()
			if val.ClientId == args.ClientId && val.SerialId == args.SerialId {
				reply.Err = OK
			}
		case <-time.After(400 * time.Millisecond):
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kvdata = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.notifyCh = make(map[int]chan Op)
	kv.lastSerialId = make(map[int64]int)
	go kv.applyMsg()

	return kv
}
