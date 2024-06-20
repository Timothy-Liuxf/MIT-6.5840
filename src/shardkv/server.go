package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

func DPrintln(a ...interface{}) {
	// log.Println(a...)
}

func WPrintln(a ...interface{}) {
	// fmt.Println(a...)
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

const NShards = shardctrler.NShards
const INVALID_GID = shardctrler.INVALID_GID
const TIMEOUT = 500
const CHECK_NEW_CONFIG_INTERVAL = 80 // QUESTION: The interval is 100ms in the tutorial
const CHECK_DATA_TRANSFER_INTERVAL = CHECK_NEW_CONFIG_INTERVAL

type ShardState int

const (
	Active ShardState = iota
	Sending
	Receiving
	Inactive
)

const (
	GetOp           = "Get"
	PutOp           = "Put"
	AppendOp        = "Append"
	ReconfigOp      = "Reconfig"
	HasSentOp       = "HasSent"
	ReceivedShardOp = "ReceivedShard"
)

type Op struct {
	ClerkId          int64
	PrevOpSeq        int64
	OpSeq            int64
	SubOpSeq         int64
	Key              string
	Value            string
	Op               string
	NewConfig        shardctrler.Config
	Shards           []int
	ReconfigTo       int
	KVStore          [NShards]map[string]string
	Gid              int
	MaxAppliedSeqs   map[int64]int64
	UnreliableFailes map[int64]map[int64]int
}

func isUserOp(op string) bool {
	return op == GetOp || op == PutOp || op == AppendOp
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mck          *shardctrler.Clerk

	kvStore          [NShards]map[string]string
	shardState       [NShards]ShardState
	currentConfig    shardctrler.Config
	previousConfig   shardctrler.Config
	dead             int32
	killCh           chan int
	maxAppliedSeqs   map[int64]int64
	timeOut          map[int64]map[int64]bool
	waitCond         map[int64]*sync.Cond
	persister        *raft.Persister
	reconfigToNum    int
	gidWaitCond      map[int]*sync.Cond
	unreliableFailes map[int64]map[int64]int // clerkId -> opSeq -> shard
}

func (kv *ShardKV) newClerkWithoutLock(ClerkId int64) {
	_, ok := kv.maxAppliedSeqs[ClerkId]
	if !ok {
		kv.maxAppliedSeqs[ClerkId] = 0
	}

	_, ok = kv.timeOut[ClerkId]
	if !ok {
		kv.timeOut[ClerkId] = make(map[int64]bool)
	}

	_, ok = kv.waitCond[ClerkId]
	if !ok {
		kv.waitCond[ClerkId] = sync.NewCond(&kv.mu)
	}

	_, ok = kv.unreliableFailes[ClerkId]
	if !ok {
		kv.unreliableFailes[ClerkId] = make(map[int64]int)
	}
}

func (kv *ShardKV) executeOpWithoutLock(op Op) Err {
	kv.newClerkWithoutLock(op.ClerkId)
	if op.OpSeq > kv.maxAppliedSeqs[op.ClerkId] {
		shard := key2shard(op.Key)
		if kv.shardState[shard] != Active {
			return ErrWrongGroup
		}

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

		if op.OpSeq > kv.maxAppliedSeqs[op.ClerkId] { // Timeout
			return ErrTimeout
		}
	}
	if _, ok := kv.unreliableFailes[op.ClerkId]; ok {
		if _, ok := kv.unreliableFailes[op.ClerkId][op.OpSeq]; ok {
			return ErrUnreliable
		}
	}
	return OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	err := kv.executeOpWithoutLock(Op{
		ClerkId:   args.ClerkId,
		OpSeq:     args.OpSeq,
		PrevOpSeq: args.PrevOpSeq,
		Key:       args.Key,
		Value:     "",
		Op:        GetOp,
	})
	if err != OK {
		reply.Err = err
		return
	}

	shard := key2shard(args.Key)
	value, ok := kv.kvStore[shard][args.Key]
	if !ok {
		reply.Value = ""
		reply.Err = ErrNoKey
	} else {
		reply.Value = value
		reply.Err = OK
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	err := kv.executeOpWithoutLock(Op{
		ClerkId:   args.ClerkId,
		OpSeq:     args.OpSeq,
		PrevOpSeq: args.PrevOpSeq,
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
	})
	if err != OK {
		reply.Err = err
		return
	}
	reply.Err = OK
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.killCh <- 1
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) receiveRaftApply() {
	for {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			if msg.SnapshotValid {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					kv.readSnapshotWithoutLock(msg.Snapshot)
					for _, cond := range kv.waitCond {
						cond.Broadcast()
					}
				}()
			}
			if msg.CommandValid {
				// msg.Command == nil means this log has been included in the snapshot and it is commited at rf.log[0]
				if msg.Command != nil {
					op := msg.Command.(Op)
					func() {
						kv.mu.Lock()
						defer kv.mu.Unlock()

						if isUserOp(op.Op) {
							clerkId := op.ClerkId
							kv.newClerkWithoutLock(clerkId)
							_, ok := kv.unreliableFailes[clerkId][op.PrevOpSeq]
							if ok {
								delete(kv.unreliableFailes[clerkId], op.PrevOpSeq)
								if len(kv.unreliableFailes[clerkId]) == 0 {
									delete(kv.unreliableFailes, clerkId)
								}
							}
							if op.OpSeq > kv.maxAppliedSeqs[clerkId] {
								shard := key2shard(op.Key)
								if kv.shardState[shard] != Active {
									// This should be impossible.
									// Because the leader should not accept the request if the shard is not active
									// And the user request and the reconfig request are linearized
									// But in unreliable network, the leader might accept the request
									// WPrintln("Warning: The shard is not active when applying, which is impossible.")
									kv.unreliableFailes[clerkId][op.OpSeq] = shard
								}

								if op.Op == "Put" {
									kv.kvStore[shard][op.Key] = op.Value
									DPrintln("Server", kv.gid, kv.me, "apply Put", op.Key, op.Value)
								} else if op.Op == "Append" {
									value, ok := kv.kvStore[shard][op.Key]
									if !ok {
										value = ""
									}
									kv.kvStore[shard][op.Key] = value + op.Value
									if kv.shardState[shard] != Active {
										DPrintln("Server", kv.gid, kv.me, "apply Append", op.Key, op.Value, "but the shard is not active", "result:", value+op.Value)
									}
									DPrintln("Server", kv.gid, kv.me, "apply Append", op.Key, op.Value, "result:", value+op.Value)
								}
								kv.maxAppliedSeqs[clerkId] = op.OpSeq
							}
							kv.waitCond[clerkId].Broadcast() // QUESTION: this broadcast might be moved into the if block
						} else {
							transferedShardData := false
							switch op.Op {
							case ReconfigOp:
								if kv.reconfigToNum == -1 && op.NewConfig.Num == kv.currentConfig.Num+1 {
									// QUESTION: Should here check whether all shards are not in the sending or receiving state?
									// - I don't think so.

									hasDataTransfer := false
									for i := 0; i < NShards; i++ {
										currentGid := kv.currentConfig.Shards[i]
										newGid := op.NewConfig.Shards[i]
										if newGid == INVALID_GID {
											// Should not happen
											// QUESTION: But if it happens, should we delete the data in the shard?
											WPrintln("Warning: The new config seems invalid that it assigns a shard to an invalid group")
											kv.shardState[i] = Inactive
										} else if currentGid == INVALID_GID && newGid == kv.gid {
											kv.shardState[i] = Active
										} else if newGid == kv.gid && currentGid != kv.gid {
											kv.shardState[i] = Receiving
											hasDataTransfer = true
										} else if newGid != kv.gid && currentGid == kv.gid {
											kv.shardState[i] = Sending
											hasDataTransfer = true
										}
									}
									kv.previousConfig = kv.currentConfig
									kv.currentConfig = op.NewConfig
									if hasDataTransfer {
										kv.reconfigToNum = op.NewConfig.Num
									}
								}
							case HasSentOp:
								if kv.reconfigToNum == op.ReconfigTo && kv.reconfigToNum == kv.currentConfig.Num {
									for _, shard := range op.Shards {
										if kv.shardState[shard] == Sending {
											kv.shardState[shard] = Inactive
											kv.kvStore[shard] = make(map[string]string)
											transferedShardData = true
										} else {
											WPrintln("Warning: The shard state is not sending when applying HasSentOp, which is impossible.")
										}
									}
									for clerkId, opSeqToShards := range kv.unreliableFailes {
										for opSeq, shard := range opSeqToShards {
											if kv.shardState[shard] == Inactive {
												delete(kv.unreliableFailes[clerkId], opSeq)
											}
										}
										if len(opSeqToShards) == 0 {
											delete(kv.unreliableFailes, clerkId)
										}
									}
								}
							case ReceivedShardOp:
								DPrintln("Server", kv.gid, kv.me, "received shard data", op.Gid, op.KVStore, op.MaxAppliedSeqs)
								if kv.reconfigToNum == op.ReconfigTo && kv.reconfigToNum == kv.currentConfig.Num {
									transferedShardData = true
									gid := op.Gid
									for i := 0; i < NShards; i++ {
										if kv.previousConfig.Shards[i] == gid && kv.shardState[i] == Receiving {
											kv.shardState[i] = Active
											kv.kvStore[i] = make(map[string]string)
											for k, v := range op.KVStore[i] {
												kv.kvStore[i][k] = v
											}
										}
									}
									for k, v := range op.MaxAppliedSeqs {
										kv.maxAppliedSeqs[k] = maxInt64(kv.maxAppliedSeqs[k], v)
									}
									for clerkId, opSeqToShards := range kv.unreliableFailes {
										if _, ok := kv.unreliableFailes[clerkId]; !ok {
											kv.unreliableFailes[clerkId] = make(map[int64]int)
										}
										for opSeq, shard := range opSeqToShards {
											if kv.shardState[shard] == Active {
												kv.unreliableFailes[clerkId][opSeq] = shard
											}
										}
										if len(kv.unreliableFailes[clerkId]) == 0 {
											delete(kv.unreliableFailes, clerkId)
										}
									}
								}
								cond, ok := kv.gidWaitCond[op.Gid]
								if ok {
									cond.Broadcast()
								}
							}
							if transferedShardData == true {
								noDataTransfer := true
								for i := 0; i < NShards; i++ {
									if kv.shardState[i] == Sending || kv.shardState[i] == Receiving {
										noDataTransfer = false
									}
								}
								if noDataTransfer {
									kv.reconfigToNum = -1
								}
							}
						}

						if kv.maxraftstate != -1 && float64(kv.persister.RaftStateSize()) >= 0.8*float64(kv.maxraftstate) {
							kv.saveToSnapshotWithoutLock(msg.CommandIndex)
						}
					}()
				}
			}
		}
	}
}

func (kv *ShardKV) saveToSnapshotWithoutLock(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(kv.kvStore); err != nil {
		panic("Failed to encode kvStore: " + err.Error())
	}
	if err := e.Encode(kv.shardState); err != nil {
		panic("Failed to encode shardState: " + err.Error())
	}
	if err := e.Encode(kv.currentConfig); err != nil {
		panic("Failed to encode currentConfig: " + err.Error())
	}
	if err := e.Encode(kv.previousConfig); err != nil {
		panic("Failed to encode previousConfig: " + err.Error())
	}
	if err := e.Encode(kv.maxAppliedSeqs); err != nil {
		panic("Failed to encode maxAppliedSeqs: " + err.Error())
	}
	if err := e.Encode(kv.reconfigToNum); err != nil {
		panic("Failed to encode reconfigToNum: " + err.Error())
	}
	if err := e.Encode(kv.unreliableFailes); err != nil {
		panic("Failed to encode unreliableFailes: " + err.Error())
	}

	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) readSnapshotWithoutLock(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvStore [NShards]map[string]string
	var shardState [NShards]ShardState
	var currentConfig shardctrler.Config
	var previousConfig shardctrler.Config
	var maxAppliedSeqs map[int64]int64
	var reconfigToNum int
	var unreliableFailes map[int64]map[int64]int
	if err := d.Decode(&kvStore); err != nil {
		panic("Failed to decode kvStore: " + err.Error())
	}
	if err := d.Decode(&shardState); err != nil {
		panic("Failed to decode shardState: " + err.Error())
	}
	if err := d.Decode(&currentConfig); err != nil {
		panic("Failed to decode currentConfig: " + err.Error())
	}
	if err := d.Decode(&previousConfig); err != nil {
		panic("Failed to decode previousConfig: " + err.Error())
	}
	if err := d.Decode(&maxAppliedSeqs); err != nil {
		panic("Failed to decode maxAppliedSeqs: " + err.Error())
	}
	if err := d.Decode(&reconfigToNum); err != nil {
		panic("Failed to decode reconfigToNum: " + err.Error())
	}
	if err := d.Decode(&unreliableFailes); err != nil {
		panic("Failed to decode unreliableFailes: " + err.Error())
	}

	kv.kvStore = kvStore
	kv.shardState = shardState
	kv.currentConfig = currentConfig
	kv.previousConfig = previousConfig
	kv.maxAppliedSeqs = maxAppliedSeqs
	kv.reconfigToNum = reconfigToNum
	kv.unreliableFailes = unreliableFailes
}

func (kv *ShardKV) checkNewConfig() {
	for !kv.killed() {
		nextConfigNum, isLeader := func() (int, bool) {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			_, isLeaderInternal := kv.rf.GetState()
			return kv.currentConfig.Num + 1, isLeaderInternal
		}()
		if isLeader {
			newConfig := kv.mck.Query(nextConfigNum)
			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()

				if newConfig.Num != kv.currentConfig.Num+1 || kv.reconfigToNum != -1 {
					return
				}

				kv.rf.Start(Op{
					Op:        ReconfigOp,
					NewConfig: newConfig,
				})
				DPrintln("Server", kv.gid, kv.me, "config:", kv.currentConfig, "shard state:", kv.shardState, "new config:", newConfig)
			}()
		}
		DPrintln("Server", kv.gid, kv.me, "config:", kv.currentConfig, "shard state:", kv.shardState)
		time.Sleep(time.Millisecond * CHECK_NEW_CONFIG_INTERVAL)
	}
}

type SendShardDataArgs struct {
	ReconfigToNum    int
	Gid              int
	KVStore          [NShards]map[string]string
	MaxAppliedSeqs   map[int64]int64
	UnreliableFailes map[int64]map[int64]int
}

type SendShardDataReply struct {
	Success bool
}

func (kv *ShardKV) SendShardData(args *SendShardDataArgs, reply *SendShardDataReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Success = false
		return
	}

	DPrintln("Server", kv.gid, kv.me, "received send shard data request", "args.ReconfigToNum:", args.ReconfigToNum, "kv.reconfigToNum:", kv.reconfigToNum)

	if args.ReconfigToNum != kv.reconfigToNum {
		if args.ReconfigToNum < kv.currentConfig.Num {
			reply.Success = true
			return
		}
		if args.ReconfigToNum > kv.currentConfig.Num {
			reply.Success = false
			return
		}
	}

	checkFinished := func() bool {
		if kv.currentConfig.Num != args.ReconfigToNum {
			return true
		}
		for i := 0; i < NShards; i++ {
			if kv.previousConfig.Shards[i] == args.Gid && kv.shardState[i] == Receiving {
				return false
			}
		}
		return true
	}

	if checkFinished() {
		reply.Success = true
		return
	}

	DPrintln("Server", kv.gid, kv.me, "start to receive shard data", "args.ReconfigToNum:", args.ReconfigToNum, "kv.reconfigToNum:", kv.reconfigToNum)

	_, _, ok := kv.rf.Start(Op{
		Op:               ReceivedShardOp,
		ReconfigTo:       args.ReconfigToNum,
		KVStore:          args.KVStore,
		Gid:              args.Gid,
		MaxAppliedSeqs:   args.MaxAppliedSeqs,
		UnreliableFailes: args.UnreliableFailes,
	})
	if !ok {
		reply.Success = false
		return
	}

	cond, ok := kv.gidWaitCond[args.Gid]
	if !ok {
		cond = sync.NewCond(&kv.mu)
		kv.gidWaitCond[args.Gid] = cond
	}

	var timeOut int32 = 0
	go func() {
		<-time.After(time.Millisecond * TIMEOUT)
		atomic.StoreInt32(&timeOut, 1)
	}()

	finished := false
	for {
		if checkFinished() {
			finished = true
			break
		}
		if atomic.LoadInt32(&timeOut) == 1 {
			break
		}
		cond.Wait()
	}

	if finished {
		reply.Success = true
		delete(kv.gidWaitCond, args.Gid)
	} else {
		reply.Success = false
	}
}

func (kv *ShardKV) sendSendShardData(server string, args *SendShardDataArgs, reply *SendShardDataReply) bool {
	ok := kv.make_end(server).Call("ShardKV.SendShardData", args, reply)
	return ok
}

func (kv *ShardKV) dataTransfer() {
	for !kv.killed() {
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()

			if kv.reconfigToNum == -1 {
				return
			}

			// TODO: Test what if the data transfer is only done by the leader

			noDataTransfer := true
			args := make(map[int]*SendShardDataArgs, 0)
			maxAppliedSeqs := make(map[int64]int64)
			for k, v := range kv.maxAppliedSeqs {
				maxAppliedSeqs[k] = v
			}
			for i := 0; i < NShards; i++ {
				if kv.shardState[i] == Sending {
					noDataTransfer = false
					gid := kv.currentConfig.Shards[i]
					newArgs, ok := args[gid]
					if !ok {
						newArgs = &SendShardDataArgs{}
						newArgs.ReconfigToNum = kv.reconfigToNum
						newArgs.Gid = kv.gid
						newArgs.KVStore = [NShards]map[string]string{} // Here I didn't make each maps, so nil visit will occur when the receiver visit the map by mistake
						newArgs.MaxAppliedSeqs = maxAppliedSeqs
						newArgs.UnreliableFailes = make(map[int64]map[int64]int)
					}
					newArgs.KVStore[i] = make(map[string]string)
					for k, v := range kv.kvStore[i] {
						newArgs.KVStore[i][k] = v
					}
					for clerkId, opSeqToShards := range kv.unreliableFailes {
						for opSeq, shard := range opSeqToShards {
							if shard == i {
								_, ok := newArgs.UnreliableFailes[clerkId]
								if !ok {
									newArgs.UnreliableFailes[clerkId] = make(map[int64]int)
								}
								newArgs.UnreliableFailes[clerkId][opSeq] = shard
							}
						}
					}
					args[gid] = newArgs
				} else if kv.shardState[i] == Receiving {
					noDataTransfer = false
				}
			}

			go func() {
				for gid, thisArgs := range args {
					for _, server := range kv.currentConfig.Groups[gid] {
						reply := SendShardDataReply{}
						ok := kv.sendSendShardData(server, thisArgs, &reply)
						if ok && reply.Success {
							func() {
								kv.mu.Lock()
								defer kv.mu.Unlock()

								_, isLeader := kv.rf.GetState()
								if !isLeader {
									return
								}

								if kv.reconfigToNum == thisArgs.ReconfigToNum {
									shards := make([]int, 0)
									for i := 0; i < NShards; i++ {
										if kv.currentConfig.Shards[i] == gid && kv.shardState[i] == Sending {
											shards = append(shards, i)
										}
									}
									kv.rf.Start(Op{
										Op:         HasSentOp,
										Shards:     shards,
										ReconfigTo: thisArgs.ReconfigToNum,
									})
								}
							}()
							break
						}
					}
				}
			}()

			if noDataTransfer {
				kv.reconfigToNum = -1
			}
		}()
		time.Sleep(time.Millisecond * CHECK_DATA_TRANSFER_INTERVAL)
		// TODO: can I use select <- has new config channel and time.After channel to optimize?
		// Enlarge the time interval is not OK? Timeout is essential if all failed
		// But the current implementation has too much send shard data requests
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.mu = sync.Mutex{}
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.kvStore = [NShards]map[string]string{}
	for i := 0; i < NShards; i++ {
		kv.kvStore[i] = make(map[string]string)
	}
	kv.shardState = [NShards]ShardState{}
	for i := 0; i < NShards; i++ {
		kv.shardState[i] = Inactive
	}
	kv.currentConfig = shardctrler.DefaultConfig()
	kv.previousConfig = shardctrler.DefaultConfig()
	kv.dead = 0
	kv.killCh = make(chan int, 1)
	kv.maxAppliedSeqs = make(map[int64]int64)
	kv.timeOut = make(map[int64]map[int64]bool)
	kv.waitCond = make(map[int64]*sync.Cond)
	kv.persister = persister
	kv.reconfigToNum = -1
	kv.gidWaitCond = make(map[int]*sync.Cond)
	kv.unreliableFailes = make(map[int64]map[int64]int)

	if maxraftstate != -1 && persister.SnapshotSize() > 0 {
		kv.readSnapshotWithoutLock(persister.ReadSnapshot())
	}

	go kv.receiveRaftApply()
	go kv.checkNewConfig()
	go kv.dataTransfer()

	return kv
}
