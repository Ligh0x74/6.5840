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

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data   map[string]string
	buffer map[int64]int64
	old    map[int64]string
}

func (kv *KVServer) testAndSet(clerkId int64, requestId int64) bool {
	DPrintf("Server ClerkId %19v RequestId %19v TestAndSet", clerkId, requestId)
	if kv.buffer[clerkId] == requestId {
		return false
	}
	kv.buffer[clerkId] = requestId
	return true
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.testAndSet(args.ClerkId, args.RequestId) {
		kv.data[args.Key] = args.Value
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.testAndSet(args.ClerkId, args.RequestId) {
		old := kv.data[args.Key]
		kv.old[args.ClerkId] = old
		kv.data[args.Key] = old + args.Value
	}
	reply.Value = kv.old[args.ClerkId]
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.buffer = make(map[int64]int64)
	kv.old = make(map[int64]string)
	return kv
}

