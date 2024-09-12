package kvraft

import (

	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GET = iota
	PUT
	APPEND
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  int
	Key   string
	Value string

	ClerkId   int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data           map[string]string
	requestBuffer  map[int64]int64
	lastApplyIndex int
	term           int
	cond           *sync.Cond
}

func (kv *KVServer) apply() {
	var msg raft.ApplyMsg
	for !kv.killed() {
		msg = <-kv.applyCh
		if msg.CommandValid {
			kv.applyToState(&msg)
		}
	}
}

func (kv *KVServer) applyToState(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := msg.Command.(Op)
	if !kv.testAndSetL(op.ClerkId, op.RequestId) {
		return
	}
	if op.Type == PUT {
		kv.data[op.Key] = op.Value
	} else if op.Type == APPEND {
		kv.data[op.Key] += op.Value
	}
	kv.lastApplyIndex = msg.CommandIndex
	kv.cond.Broadcast()
}

func (kv *KVServer) testAndSetL(clerkId int64, requestId int64) bool {
	if kv.requestBuffer[clerkId] == requestId {
		return false
	}
	kv.requestBuffer[clerkId] = requestId
	return true
}

func (kv *KVServer) waitL(clerkId int64, requestId int64, index int, term int) bool {
	for kv.requestBuffer[clerkId] != requestId && kv.lastApplyIndex < index && kv.term == term {
		kv.cond.Wait()
	}
	return kv.requestBuffer[clerkId] == requestId
}

func (kv *KVServer) ticker() {
	for !kv.killed() {
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
		term, _ := kv.rf.GetState()
		if kv.term < term {
			kv.term = term
			kv.cond.Broadcast()
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Type:      GET,
		Key:       args.Key,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, isLeader := kv.rf.Start(op)
	ok := isLeader
	ok = ok && kv.waitL(args.ClerkId, args.RequestId, index, term)
	if ok {
		reply.Err = OK
		reply.Value = kv.data[args.Key]
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Type:      PUT,
		Key:       args.Key,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, isLeader := kv.rf.Start(op)
	ok := isLeader
	ok = ok && kv.waitL(args.ClerkId, args.RequestId, index, term)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Type:      APPEND,
		Key:       args.Key,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, isLeader := kv.rf.Start(op)
	ok := isLeader
	ok = ok && kv.waitL(args.ClerkId, args.RequestId, index, term)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
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
	// Your code here, if desired.
	DPrintf("S%v Kill\n", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.requestBuffer = make(map[int64]int64)
	kv.cond = sync.NewCond(&kv.mu)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()
	go kv.ticker()

	return kv
}

