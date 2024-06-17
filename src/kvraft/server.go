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

const TIMEOUT = 500

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug {
		log.Println(a...)
	}
	return
}

type Op struct {
	ClerkId int64
	OpSeq   int64
	Key     string
	Value   string
	Op      string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore        map[string]string
	maxAppliedSeqs map[int64]int64
	timeOut        map[int64]map[int64]bool
	waitCond       map[int64]*sync.Cond
	killCh         chan int
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (kv *KVServer) NewClerkWithoutLock(ClerkId int64) {
	_, ok := kv.maxAppliedSeqs[ClerkId]
	if !ok {
		kv.maxAppliedSeqs[ClerkId] = 0
		kv.timeOut[ClerkId] = make(map[int64]bool)
		kv.waitCond[ClerkId] = sync.NewCond(&kv.mu)
	}
}

func (kv *KVServer) ExecuteOpWithoutLock(op Op) Err {
	kv.NewClerkWithoutLock(op.ClerkId)
	if op.OpSeq > kv.maxAppliedSeqs[op.ClerkId] {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			return ErrWrongLeader
		}

		kv.timeOut[op.ClerkId][op.OpSeq] = false
		defer delete(kv.timeOut[op.ClerkId], op.OpSeq)
		_, _, success := kv.rf.Start(op)
		if !success {
			return ErrWrongLeader
		}

		go func() {
			<-time.After(time.Millisecond * TIMEOUT)
			kv.mu.Lock()
			defer kv.mu.Unlock()
			kv.timeOut[op.ClerkId][op.OpSeq] = true
			kv.waitCond[op.ClerkId].Broadcast()
		}()

		for op.OpSeq > kv.maxAppliedSeqs[op.ClerkId] && !kv.timeOut[op.ClerkId][op.OpSeq] {
			kv.waitCond[op.ClerkId].Wait()
		}

		if kv.timeOut[op.ClerkId][op.OpSeq] {
			return ErrTimeout
		}
	}
	return OK
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	err := kv.ExecuteOpWithoutLock(Op{
		ClerkId: args.ClerkId,
		OpSeq:   args.OpSeq,
		Key:     args.Key,
		Value:   "",
		Op:      "Get",
	})
	if err != OK {
		reply.Err = err
		return
	}

	value, ok := kv.kvStore[args.Key]
	if !ok {
		value = ""
	}
	reply.Err = OK
	reply.Value = value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	err := kv.ExecuteOpWithoutLock(Op{
		ClerkId: args.ClerkId,
		OpSeq:   args.OpSeq,
		Key:     args.Key,
		Value:   args.Value,
		Op:      args.Op,
	})
	if err != OK {
		reply.Err = err
		return
	}
	reply.Err = OK
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
	kv.killCh <- 1
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ReceiveRaftApply() {
	for {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()

					DPrintln(kv.me, "Apply", op.ClerkId, op.OpSeq, op)
					clerkId := op.ClerkId
					kv.NewClerkWithoutLock(clerkId)
					if op.OpSeq > kv.maxAppliedSeqs[clerkId] {
						if op.Op == "Put" || op.Op == "Append" {
							value, ok := kv.kvStore[op.Key]
							if !ok {
								value = ""
							}
							if op.Op == "Put" {
								kv.kvStore[op.Key] = op.Value
							} else {
								kv.kvStore[op.Key] = value + op.Value
							}
						}
						kv.maxAppliedSeqs[clerkId] = op.OpSeq
					}
					kv.waitCond[clerkId].Broadcast() // QUESTION: this broadcast might be moved into the if block
				}()
			}
		}
	}
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
	kv.mu = sync.Mutex{}
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dead = 0

	kv.kvStore = make(map[string]string)
	kv.maxAppliedSeqs = make(map[int64]int64)
	kv.timeOut = make(map[int64]map[int64]bool)
	kv.waitCond = make(map[int64]*sync.Cond)
	kv.killCh = make(chan int, 1)

	go kv.ReceiveRaftApply()

	return kv
}
