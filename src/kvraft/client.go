package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	preLeaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.preLeaderId = int(nrand()) % len(ck.servers)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{}
	args.Key = key
	args.SerialNum = fmt.Sprintf("%d%d", time.Now().UnixNano()/int64(time.Millisecond), nrand())
	reply := GetReply{}
	fmt.Printf("[Get key: %v] \n", key)
	id := ck.preLeaderId
	for {
		//fmt.Printf("%v, ", id)
		ok := ck.servers[id].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK || reply.Err == ErrNoKey {
				ck.preLeaderId = reply.LeaderId
				break
			}
		}
		id = int(nrand()) % len(ck.servers)
	}
	//fmt.Printf("result %v\n", reply.Value)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.SerialNum = fmt.Sprintf("%d%d", time.Now().UnixNano()/int64(time.Millisecond), nrand())
	reply := PutAppendReply{}
	//fmt.Printf("[%v key: %v, value: %v]\n", op, key, value)
	id := ck.preLeaderId
	for {
		// fmt.Printf("%v, ", id)
		ok := ck.servers[id].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.preLeaderId = reply.LeaderId
				break
			}
		}
		id = int(nrand()) % len(ck.servers)
	}
	// fmt.Printf("\n")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
