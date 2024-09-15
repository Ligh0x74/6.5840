package shardkv

import (
	"bytes"
	"log"
	"sync/atomic"
	"time"
)

import "6.5840/shardctrler"
import "6.5840/labrpc"
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	// Your definitions here.
	data           map[string]string
	requestBuffer  map[int64]int64
	lastApplyIndex int
	term           int
	cond           *sync.Cond

	persister *raft.Persister

	shardctrlerClerk *shardctrler.Clerk
	conf             shardctrler.Config
}

func (kv *ShardKV) apply() {
	var msg raft.ApplyMsg
	for !kv.killed() {
		msg = <-kv.applyCh
		kv.mu.Lock()
		//DPrintf("S%v ApplyMsg CI %v LI %v\n", kv.me, msg.CommandIndex, kv.lastApplyIndex)
		if msg.CommandValid && msg.CommandIndex > kv.lastApplyIndex {
			kv.applyToStateL(&msg)
		}
		if msg.SnapshotValid {
			kv.readSnapshotL(msg.Snapshot)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyToStateL(msg *raft.ApplyMsg) {
	op := msg.Command.(Op)
	if !kv.testAndSetL(op.ClerkId, op.RequestId) {
		return
	}
	DPrintf("S%v ApplyMsg %v, KeyValue %v\n", kv.me, msg, kv.data[op.Key])
	if op.Type == PUT {
		kv.data[op.Key] = op.Value
	} else if op.Type == APPEND {
		kv.data[op.Key] += op.Value
	}
	kv.lastApplyIndex = msg.CommandIndex
	kv.cond.Broadcast()
}

func (kv *ShardKV) testAndSetL(clerkId int64, requestId int64) bool {
	if kv.requestBuffer[clerkId] == requestId {
		return false
	}
	kv.requestBuffer[clerkId] = requestId
	return true
}

func (kv *ShardKV) waitL(clerkId int64, requestId int64, index int, term int) bool {
	for kv.requestBuffer[clerkId] != requestId && kv.lastApplyIndex < index && kv.term == term {
		kv.cond.Wait()
	}
	return kv.requestBuffer[clerkId] == requestId
}

func (kv *ShardKV) ticker() {
	for !kv.killed() {
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
		term, _ := kv.rf.GetState()
		if kv.term < term {
			kv.term = term
			kv.cond.Broadcast()
		}
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.writeSnapshotL()
		}
		kv.conf = kv.shardctrlerClerk.Query(-1)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.conf.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}

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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.conf.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}

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

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf("S%v Kill\n", kv.me)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.persister = persister
	kv.data = make(map[string]string)
	kv.requestBuffer = make(map[int64]int64)
	kv.cond = sync.NewCond(&kv.mu)
	kv.readSnapshotL(kv.persister.ReadSnapshot())

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.shardctrlerClerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.conf = kv.shardctrlerClerk.Query(-1)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.apply()
	go kv.ticker()

	return kv
}

func (kv *ShardKV) writeSnapshotL() {
	DPrintf("S%v Write Snapshot, LastI%v\n", kv.me, kv.lastApplyIndex)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.requestBuffer)
	e.Encode(kv.lastApplyIndex)
	e.Encode(kv.term)
	snapshot := w.Bytes()
	kv.rf.Snapshot(kv.lastApplyIndex, snapshot)
}

func (kv *ShardKV) readSnapshotL(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var requestBuffer map[int64]int64
	var lastApplyIndex int
	var term int
	if d.Decode(&data) != nil ||
		d.Decode(&requestBuffer) != nil ||
		d.Decode(&lastApplyIndex) != nil ||
		d.Decode(&term) != nil {
		log.Fatalf("S%d readSnapshot error", kv.me)
	}
	if kv.lastApplyIndex < lastApplyIndex {
		kv.data = data
		kv.requestBuffer = requestBuffer
		kv.lastApplyIndex = lastApplyIndex
		kv.term = term
	}
	DPrintf("S%v Read Snapshot, LastI %v < %v\n", kv.me, kv.lastApplyIndex, lastApplyIndex)
}

