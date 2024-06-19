package shardctrler

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const TIMEOUT = 500
const INVALID_GID = 0

func DPrintln(a ...interface{}) {
	// log.Println(a...)
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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	dead           int32 // set by Kill()
	maxAppliedSeqs map[int64]int64
	timeOut        map[int64]map[int64]bool
	waitCond       map[int64]*sync.Cond
	killCh         chan int

	configs []Config // indexed by config num
}

const (
	JoinOp  = "Join"
	LeaveOp = "Leave"
	MoveOp  = "Move"
	QueryOp = "Query"
)

type Op struct {
	ClerkId int64
	OpSeq   int64
	Servers map[int][]string
	GIDs    []int
	Shard   int
	GID     int
	Num     int
	Op      string
}

func (sc *ShardCtrler) NewClerkWithoutLock(ClerkId int64) {
	_, ok := sc.maxAppliedSeqs[ClerkId]
	if !ok {
		sc.maxAppliedSeqs[ClerkId] = 0
	}

	_, ok = sc.timeOut[ClerkId]
	if !ok {
		sc.timeOut[ClerkId] = make(map[int64]bool)
	}

	_, ok = sc.waitCond[ClerkId]
	if !ok {
		sc.waitCond[ClerkId] = sync.NewCond(&sc.mu)
	}
}

func (sc *ShardCtrler) ExecuteOpWithoutLock(op Op) (bool, Err) {
	sc.NewClerkWithoutLock(op.ClerkId)
	if op.OpSeq > sc.maxAppliedSeqs[op.ClerkId] {
		_, isLeader := sc.rf.GetState()
		if !isLeader {
			return true, OK
		}

		sc.timeOut[op.ClerkId][op.OpSeq] = false
		defer delete(sc.timeOut[op.ClerkId], op.OpSeq)
		_, _, success := sc.rf.Start(op)
		if !success {
			return true, OK
		}

		go func() {
			<-time.After(time.Millisecond * TIMEOUT)
			sc.mu.Lock()
			defer sc.mu.Unlock()
			sc.timeOut[op.ClerkId][op.OpSeq] = true
			sc.waitCond[op.ClerkId].Broadcast()
		}()

		for op.OpSeq > sc.maxAppliedSeqs[op.ClerkId] && !sc.timeOut[op.ClerkId][op.OpSeq] {
			sc.waitCond[op.ClerkId].Wait()
		}

		if op.OpSeq > sc.maxAppliedSeqs[op.ClerkId] { // Timeout
			return false, ErrTimeout
		}
	}
	return false, OK
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	wrongLeader, err := sc.ExecuteOpWithoutLock(Op{
		ClerkId: args.ClerkId,
		OpSeq:   args.OpSeq,
		Servers: args.Servers,
		Op:      JoinOp,
	})
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	wrongLeader, err := sc.ExecuteOpWithoutLock(Op{
		ClerkId: args.ClerkId,
		OpSeq:   args.OpSeq,
		GIDs:    args.GIDs,
		Op:      LeaveOp,
	})
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	wrongLeader, err := sc.ExecuteOpWithoutLock(Op{
		ClerkId: args.ClerkId,
		OpSeq:   args.OpSeq,
		Shard:   args.Shard,
		GID:     args.GID,
		Op:      MoveOp,
	})
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	wrongLeader, err := sc.ExecuteOpWithoutLock(Op{
		ClerkId: args.ClerkId,
		OpSeq:   args.OpSeq,
		Num:     args.Num,
		Op:      QueryOp,
	})
	reply.WrongLeader = wrongLeader
	reply.Err = err
	if !wrongLeader && err == OK {
		num := args.Num
		if num == -1 || num >= len(sc.configs) {
			num = len(sc.configs) - 1
		}
		reply.Config = sc.copyConfigWithoutLock(num)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	sc.killCh <- 1
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) copyConfigWithoutLock(index int) Config {
	config := Config{}
	config.Num = sc.configs[index].Num
	config.Shards = [NShards]int{}
	copy(config.Shards[:], sc.configs[index].Shards[:])
	config.Groups = make(map[int][]string)
	for gid, servers := range sc.configs[index].Groups {
		config.Groups[gid] = make([]string, len(servers))
		copy(config.Groups[gid], servers)
	}
	return config
}

func (sc *ShardCtrler) allocNextConfigWithoutLock() Config {
	newConfig := sc.copyConfigWithoutLock(len(sc.configs) - 1)
	newConfig.Num++
	return newConfig
}

func reBalance(config *Config) {
	if len(config.Groups) == 0 {
		for i := 0; i < len(config.Shards); i++ {
			config.Shards[i] = INVALID_GID
		}
		return
	}

	shards := make(map[int][]int)
	for i, gid := range config.Shards {
		shards[gid] = append(shards[gid], i)
	}

	for gid := range config.Groups {
		if _, ok := shards[gid]; !ok {
			shards[gid] = make([]int, 0)
		}
	}

	type GidAndShardCountPair struct {
		gid        int
		shardCount int
	}
	gidAndShardCountPairs := make([]GidAndShardCountPair, 0)
	for gid, shard := range shards {
		gidAndShardCountPairs = append(gidAndShardCountPairs, GidAndShardCountPair{gid, len(shard)})
	}
	sort.SliceStable(gidAndShardCountPairs, func(i, j int) bool {
		if gidAndShardCountPairs[i].shardCount == gidAndShardCountPairs[j].shardCount {
			return gidAndShardCountPairs[i].gid < gidAndShardCountPairs[j].gid
		}
		return gidAndShardCountPairs[i].shardCount > gidAndShardCountPairs[j].shardCount
	})
	DPrintln("Config:", "config.Group:", config.Groups, "config.Shards:", config.Shards)
	DPrintln("After sort:", gidAndShardCountPairs)

	aliveGidsCount := len(config.Groups)
	averageShards := len(config.Shards) / aliveGidsCount
	additionalShards := len(config.Shards) % aliveGidsCount
	availableShards := make([]int, 0)
	resultShardCounts := make([]int, len(gidAndShardCountPairs))
	for i := 0; i < len(gidAndShardCountPairs); i++ {
		gidAndShardCountPair := gidAndShardCountPairs[i]
		gid := gidAndShardCountPair.gid
		shardCount := gidAndShardCountPair.shardCount

		resultShardCount := averageShards
		if _, ok := config.Groups[gid]; !ok {
			resultShardCount = 0
		} else if additionalShards > 0 {
			resultShardCount++
			additionalShards--
		}
		resultShardCounts[i] = resultShardCount

		if shardCount > resultShardCount {
			originalShards := shards[gid]
			availableShards = append(availableShards, originalShards[resultShardCount:]...)
			shards[gid] = originalShards[:resultShardCount]
		}
	}
	for i := 0; i < len(gidAndShardCountPairs); i++ {
		gidAndShardCountPair := gidAndShardCountPairs[i]
		gid := gidAndShardCountPair.gid
		shardCount := gidAndShardCountPair.shardCount
		resultShardCount := resultShardCounts[i]

		if shardCount < resultShardCount {
			needShards := resultShardCount - shardCount
			if needShards > len(availableShards) {
				needShards = len(availableShards)
				fmt.Println("Warning: Not enough shards to re-balance, which is impossible")
			}
			moveShards := availableShards[:needShards]
			shards[gid] = append(shards[gid], moveShards...)
			availableShards = availableShards[needShards:]

			for _, moveShard := range moveShards {
				config.Shards[moveShard] = gid
			}
		}
	}
	if len(availableShards) > 0 {
		fmt.Println("Warning: There are some shards left, which is impossible")
	}
}

func (sc *ShardCtrler) ReceiveRaftApply() {
	for {
		select {
		case <-sc.killCh:
			return
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				// msg.Command == nil means this log has been included in the snapshot and it is commited at rf.log[0]
				if msg.Command != nil {
					op := msg.Command.(Op)
					func() {
						sc.mu.Lock()
						defer sc.mu.Unlock()

						clerkId := op.ClerkId
						sc.NewClerkWithoutLock(clerkId)
						if op.OpSeq > sc.maxAppliedSeqs[clerkId] {
							switch op.Op {
							case JoinOp:
								newConfig := sc.allocNextConfigWithoutLock()
								for gid, servers := range op.Servers {
									if _, ok := newConfig.Groups[gid]; !ok {
										newConfig.Groups[gid] = make([]string, len(servers))
										copy(newConfig.Groups[gid], servers)
									}
								}
								reBalance(&newConfig)
								sc.configs = append(sc.configs, newConfig)
							case LeaveOp:
								newConfig := sc.allocNextConfigWithoutLock()
								for _, gid := range op.GIDs {
									delete(newConfig.Groups, gid)
								}
								reBalance(&newConfig)
								sc.configs = append(sc.configs, newConfig)
							case MoveOp:
								if op.Shard >= 0 && op.Shard < NShards {
									newConfig := sc.allocNextConfigWithoutLock()
									if _, ok := newConfig.Groups[op.GID]; ok {
										newConfig.Shards[op.Shard] = op.GID
										reBalance(&newConfig)
									} else {
										fmt.Println("Warning: The target group does not exist")
									}
									sc.configs = append(sc.configs, newConfig)
								}
							}
							sc.maxAppliedSeqs[clerkId] = op.OpSeq
						}
						sc.waitCond[clerkId].Broadcast() // QUESTION: this broadcast might be moved into the if block
					}()
				}
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.mu = sync.Mutex{}
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Num = 0
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = INVALID_GID
	}
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.dead = 0
	sc.maxAppliedSeqs = make(map[int64]int64)
	sc.timeOut = make(map[int64]map[int64]bool)
	sc.waitCond = make(map[int64]*sync.Cond)
	sc.killCh = make(chan int, 1)

	go sc.ReceiveRaftApply()

	return sc
}
