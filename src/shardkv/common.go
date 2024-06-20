package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrUnreliable  = "ErrUnreliable"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClerkId   int64
	OpSeq     int64
	PrevOpSeq int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClerkId   int64
	OpSeq     int64
	PrevOpSeq int64
}

type GetReply struct {
	Err   Err
	Value string
}
