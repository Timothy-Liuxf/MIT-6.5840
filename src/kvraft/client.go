package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.5840/labrpc"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	clerkId    int64
	opSeq      int64
	lastLeader int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) allocNewOpSeq() int64 {
	return atomic.AddInt64(&ck.opSeq, 1)
}

func (ck *Clerk) getLastLeader() int64 {
	return atomic.LoadInt64(&ck.lastLeader)
}

func (ck *Clerk) setLastLeader(lastLeader int64) {
	atomic.StoreInt64(&ck.lastLeader, lastLeader)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = nrand()
	ck.opSeq = 0
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
	args := GetArgs{
		Key:     key,
		ClerkId: ck.clerkId,
		OpSeq:   ck.allocNewOpSeq(),
	}
	for {
		lastLeader := int(ck.getLastLeader())
		for i := 0; i < len(ck.servers); i++ {
			leader := (i + lastLeader) % len(ck.servers)
			reply := GetReply{}
			if ck.servers[leader].Call("KVServer.Get", &args, &reply) && reply.Err == OK {
				ck.setLastLeader(int64(leader))
				return reply.Value
			}
		}
	}
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
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		ClerkId: ck.clerkId,
		OpSeq:   ck.allocNewOpSeq(),
	}
	for {
		lastLeader := int(ck.getLastLeader())
		for i := 0; i < len(ck.servers); i++ {
			leader := (i + lastLeader) % len(ck.servers)
			reply := PutAppendReply{}
			if ck.servers[leader].Call("KVServer.PutAppend", &args, &reply) && reply.Err == OK {
				ck.setLastLeader(int64(leader))
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
