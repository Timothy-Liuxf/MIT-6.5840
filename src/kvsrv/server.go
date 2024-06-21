package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

type KVServer struct {
	mu        sync.Mutex
	kvStore   map[string]string
	lastSeq   map[int64]int64
	lastReply map[int64]string
}

func (kv *KVServer) newClerkWithoutLock(ClerkId int64) {
	_, ok := kv.lastSeq[ClerkId]
	if !ok {
		kv.lastSeq[ClerkId] = 0
		kv.lastReply[ClerkId] = ""
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.kvStore[args.Key]
	if !ok {
		reply.Value = ""
	} else {
		reply.Value = value
	}

	if _, ok := kv.lastReply[args.ClerkId]; ok {
		delete(kv.lastReply, args.ClerkId)
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.PutAppend(args, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.PutAppend(args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clerkId := args.ClerkId
	kv.newClerkWithoutLock(clerkId)
	if args.OpSeq <= kv.lastSeq[clerkId] {
		reply.Value = kv.lastReply[clerkId]
		return
	}

	if args.Op == "Put" {
		kv.kvStore[args.Key] = args.Value
		reply.Value = ""
	} else {
		value, ok := kv.kvStore[args.Key]
		if !ok {
			value = ""
		}
		kv.kvStore[args.Key] = value + args.Value
		reply.Value = value
	}

	kv.lastSeq[clerkId] = args.OpSeq
	kv.lastReply[clerkId] = reply.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.mu = sync.Mutex{}
	kv.kvStore = make(map[string]string)
	kv.lastSeq = make(map[int64]int64)
	kv.lastReply = make(map[int64]string)
	return kv
}
