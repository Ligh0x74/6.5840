package shardkv

import "fmt"

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
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId   int64
	RequestId int64
}

func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("ClerkId %v RequestId %v Key %v Value %v",
		args.ClerkId, args.RequestId, args.Key, args.Value)
}

type PutAppendReply struct {
	Err Err
}

func (args *PutAppendReply) String() string {
	return fmt.Sprintf("Err %v", args.Err)
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId   int64
	RequestId int64
}

func (args *GetArgs) String() string {
	return fmt.Sprintf("ClerkId %v RequestId %v Key %v",
		args.ClerkId, args.RequestId, args.Key)
}

type GetReply struct {
	Err   Err
	Value string
}

func (args *GetReply) String() string {
	return fmt.Sprintf("Err %v Value %v", args.Err, args.Value)
}

