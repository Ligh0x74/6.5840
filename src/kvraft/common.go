package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
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

