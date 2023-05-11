package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

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
	Key       string
	Value     string
	Opera     string
	SerialNum string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdata      map[string]string
	serialCache map[string]string
	currentTerm int
}

// 直接从map中查询？？ 会查询到过时的数据  需要判断数据是否过期
// FIXME:频繁更换Leader，导致get到过期数据
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() == false {
		reply.Value = ""
		reply.Err = ErrWrongLeader

		kv.mu.Lock()
		defer kv.mu.Unlock()

		if val, ok := kv.serialCache[args.SerialNum]; ok {
			reply.Value = val
			reply.Err = OK
			return
		}

		// 需要判断数据是否过期后，再从日志中找
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			return
		}

		// 当为小分区leader时，get不应该返回数据，需要判断是否为小分区leader
		// Leader不断变换时，刚换leader就get，有的状态机apply完，导致数据是过时的
		if kv.rf.SendGetBeforeHeartBeat() == false {
			return
		}
		// applyMsg
		//kv.applyMsg()
		reply.LeaderId = kv.me

		if val, ok := kv.kvdata[args.Key]; ok {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
		}
		kv.serialCache[args.SerialNum] = reply.Value
	}
}

func (kv *KVServer) applyMsg() {
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				kv.serialCache[op.SerialNum] = op.Value
				if op.Opera == "Put" {
					kv.kvdata[op.Key] = op.Value
					fmt.Printf("[]put key:%v, value:%v\n", op.Key, kv.kvdata[op.Key])
				} else if op.Opera == "Append" {
					kv.kvdata[op.Key] = kv.kvdata[op.Key] + op.Value
					fmt.Printf("[]append %v\n", kv.kvdata[op.Key])
				}
			}
		default:
			return
		}
	}
}

// 每次调用 Clerk.Put() 或 Clerk.Append() 应该只有一次执行，因此你需要确保重新发送不会导致服务器执行请求两次。
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() == false {
		kv.mu.Lock()

		op := Op{Key: args.Key, Value: args.Value, Opera: args.Op, SerialNum: args.SerialNum}
		reply.Err = ErrWrongLeader

		if _, ok := kv.serialCache[args.SerialNum]; ok {
			reply.Err = OK
			reply.LeaderId = kv.me
			kv.mu.Unlock()
			return
		}
		currentTerm := kv.currentTerm
		kv.mu.Unlock()

		// 判断leader不在小分区的办法
		if kv.rf.SendGetBeforeHeartBeat() == false {
			return
		}

		index, term, isLeader := kv.rf.Start(op)

		if !isLeader {
			return
		}
		reply.LeaderId = kv.me

		// 换leader时，新leader里面的msg没有读取出来，应该在成为leader之后，将chan里面的信息读取出来到non-op  只在切换Leader时，可用term判断是否切换了leader
		fmt.Printf("term:%v, currentTerm:%v\n", term, currentTerm)
		if term > currentTerm {
			kv.mu.Lock()
			kv.currentTerm = term
			kv.mu.Unlock()
			for msg := range kv.applyCh {
				//fmt.Printf("commandIndex:%v, index:%v\n", msg.CommandIndex, index)
				if msg.CommandValid {
					op = msg.Command.(Op)
					kv.mu.Lock()
					kv.serialCache[op.SerialNum] = op.Value
					if op.Opera == "Put" {
						kv.kvdata[op.Key] = op.Value
						//fmt.Printf("put key:%v, value:%v\n", op.Key, kv.kvdata[op.Key])
					} else if op.Opera == "Append" {
						kv.kvdata[op.Key] = kv.kvdata[op.Key] + op.Value
						//fmt.Printf("append %v\n", kv.kvdata[op.Key])
					}
					kv.mu.Unlock()
					if msg.CommandIndex == index {
						reply.Err = OK
						break
					}
				}
			}
		} else {
			// FIXME: chan中没数据，卡死
			msg := <-kv.applyCh
			// fmt.Printf("commandIndex:%v, index:%v\n", msg.CommandIndex, index)
			// msg.CommandIndex == index  多客户端并行发送，状态顺序错了或者缺少最后一个apply，提交时确保提交本次操作的日志
			if msg.CommandValid && msg.CommandIndex == index {
				op = msg.Command.(Op)
				kv.mu.Lock()
				kv.serialCache[op.SerialNum] = op.Value
				if op.Opera == "Put" {
					kv.kvdata[op.Key] = op.Value
					//fmt.Printf("[]put key:%v, value:%v\n", op.Key, kv.kvdata[op.Key])
				} else if op.Opera == "Append" {
					kv.kvdata[op.Key] = kv.kvdata[op.Key] + op.Value
					//fmt.Printf("[]append %v\n", kv.kvdata[op.Key])
				}
				reply.Err = OK
				kv.mu.Unlock()
			}
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
	kv.currentTerm = 0

	// You may need initialization code here.
	kv.kvdata = make(map[string]string)
	kv.serialCache = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
