package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

import "6.5840/shardctrler"
import "6.5840/labrpc"
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	Get          = "Get"
	Put          = "Put"
	Append       = "Append"
	UpdateConfig = "UpdateConfig"
	PutShard     = "PutShard"
	DeleteShard  = "DeleteShard"
	Empty        = "Empty"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string

	ClerkId   int64
	RequestId int64

	Conf        shardctrler.Config
	GidToShards map[int][]int
	WaitShards  map[int]bool

	Gid           int
	Num           int
	Data          map[int]map[string]string
	RequestBuffer map[int]map[int64]int64

	DeleteShards []int
}

func (op *Op) String() string {
	return fmt.Sprintf("ClerkId %v RequestId %v Type %v Key %v Value %v Data %v",
		op.ClerkId, op.RequestId, op.Type, op.Key, op.Value, op.Data)
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
	data           map[int]map[string]string
	requestBuffer  map[int]map[int64]int64
	lastApplyIndex int
	term           int
	cond           *sync.Cond

	persister *raft.Persister

	shardctrlerClerk *shardctrler.Clerk
	conf             shardctrler.Config
	gidToShards      map[int][]int
	waitShards       map[int]bool
	putShardBuffer   map[int]int
	configNum        int

	restart bool
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
	if op.Type == Empty {
		return
	}
	if !kv.testAndSetL(&op) {
		kv.cond.Broadcast()
		return
	}
	shard := key2shard(op.Key)
	_, ok := kv.data[shard]
	if !ok {
		kv.data[shard] = make(map[string]string)
	}
	_, ok = kv.requestBuffer[shard]
	if !ok {
		kv.requestBuffer[shard] = make(map[int64]int64)
	}
	DPrintf("G%v S%v ApplyMsg %v\n", kv.gid, kv.me, msg)
	if op.Type == Put {
		kv.data[shard][op.Key] = op.Value
	} else if op.Type == Append {
		kv.data[shard][op.Key] += op.Value
	} else if op.Type == UpdateConfig {
		if kv.conf.Num < op.Conf.Num {
			kv.conf = op.Conf
			// copy!!race, raft log encode gob
			kv.gidToShards = make(map[int][]int)
			for i, s := range op.GidToShards {
				for _, v := range s {
					kv.gidToShards[i] = append(kv.gidToShards[i], v)
					DPrintf("G%v S%v Append GidToShards %v\n", kv.gid, kv.me, kv.gidToShards)
				}
			}
			for i, v := range op.WaitShards {
				kv.waitShards[i] = v
			}
		}
	} else if op.Type == PutShard {
		if kv.conf.Num == op.Num {
			for s, v := range op.Data {
				if _, ok = kv.waitShards[s]; ok {
					kv.data[s] = kv.copyMapL(v)
					delete(kv.waitShards, s)
					DPrintf("G%v S%v Delete WaitShard %v, WaitShards %v\n", kv.gid, kv.me, s, kv.waitShards)
				}
			}
			for s := range op.RequestBuffer {
				kv.requestBuffer[s] = kv.copyMappL(op.RequestBuffer[s])
			}
		}
	} else if op.Type == DeleteShard {
		if kv.conf.Num == op.Num {
			for _, s := range op.DeleteShards {
				delete(kv.data, s)
			}
			delete(kv.gidToShards, op.Gid)
		}
	}
	kv.lastApplyIndex = msg.CommandIndex
	kv.cond.Broadcast()
}

func (kv *ShardKV) testAndSetL(op *Op) bool {
	if op.Type == Get || op.Type == Put || op.Type == Append {
		shard := key2shard(op.Key)
		// !
		if kv.conf.Shards[shard] != kv.gid {
			return false
		}
		clerkId := op.ClerkId
		requestId := op.RequestId
		_, ok := kv.requestBuffer[shard]
		if !ok {
			kv.requestBuffer[shard] = make(map[int64]int64)
		}
		if kv.requestBuffer[shard][clerkId] == requestId {
			return false
		}
		kv.requestBuffer[shard][clerkId] = requestId
	} else if op.Type == PutShard {
		gid := op.Gid
		num := op.Num
		if kv.putShardBuffer[gid] >= num {
			return false
		}
		kv.putShardBuffer[gid] = num
	}
	return true
}

func (kv *ShardKV) waitL(clerkId int64, requestId int64, index int, term int, shard int) bool {
	_, ok := kv.requestBuffer[shard]
	if !ok {
		kv.requestBuffer[shard] = make(map[int64]int64)
	}
	for !kv.killed() && kv.requestBuffer[shard][clerkId] != requestId && kv.lastApplyIndex < index && kv.term == term && kv.conf.Shards[shard] == kv.gid {
		kv.cond.Wait()
	}
	return kv.requestBuffer[shard][clerkId] == requestId
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
		//DPrintf("G%v S%v GidToShards %v", kv.gid, kv.me, kv.gidToShards)
		//DPrintf("G%v S%v WaitShards %v", kv.gid, kv.me, kv.waitShards)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) checkConfig() {
	for !kv.killed() {
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			continue
		}

		for !kv.killed() && (len(kv.gidToShards) > 0 || len(kv.waitShards) > 0) {
			DPrintf("G%v S%v Wait Config finish %v < %v, GidToShards %v WaitShards %v\n", kv.gid, kv.me, kv.conf.Num, kv.configNum, kv.gidToShards, kv.waitShards)
			kv.cond.Wait()
		}
		if kv.killed() {
			return
		}

		conf := kv.shardctrlerClerk.Query(kv.conf.Num + 1)
		DPrintf("G%v S%v Num %v < %v", kv.gid, kv.me, kv.conf.Num, conf.Num)
		kv.configNum = conf.Num
		gidToShards := make(map[int][]int)
		waitShards := make(map[int]bool)
		if conf.Num != 1 {
			for i, v := range kv.conf.Shards {
				// leave
				if v == kv.gid && conf.Shards[i] != kv.gid {
					gidToShards[conf.Shards[i]] = append(gidToShards[conf.Shards[i]], i)
				}
				// join
				if v != kv.gid && conf.Shards[i] == kv.gid {
					waitShards[i] = true
				}
			}
		}
		if kv.conf.Num < conf.Num {
			DPrintf("G%v S%v New Config, GidToShards %v WaitShards %v, Config %v -> %v, Wait for %v %v must nil\n", kv.gid, kv.me, gidToShards, waitShards, kv.conf, conf, kv.gidToShards, kv.waitShards)
			op := Op{
				Type:        UpdateConfig,
				Conf:        conf,
				GidToShards: gidToShards,
				WaitShards:  waitShards,
			}
			kv.rf.Start(op)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) copyMapL(data map[string]string) map[string]string {
	res := make(map[string]string)
	for k, v := range data {
		res[k] = v
	}
	return res
}

func (kv *ShardKV) copyMappL(data map[int64]int64) map[int64]int64 {
	res := make(map[int64]int64)
	for k, v := range data {
		res[k] = v
	}
	return res
}

func (kv *ShardKV) putShard() {
	for !kv.killed() {
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			kv.mu.Unlock()
			continue
		}

		// advance commitIndex when restart
		if kv.restart {
			kv.restart = false
			op := Op{Type: Empty}
			kv.rf.Start(op)
		}

		for gid, slice := range kv.gidToShards {
			args := PutShardArgs{}
			args.Data = make(map[int]map[string]string)
			args.RequestBuffer = make(map[int]map[int64]int64)
			args.GID = kv.gid
			args.Num = kv.conf.Num
			for _, shard := range slice {
				args.Data[shard] = kv.copyMapL(kv.data[shard])
				args.RequestBuffer[shard] = kv.copyMappL(kv.requestBuffer[shard])
			}
			go kv.putShardProcessor(gid, &args)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) putShardProcessor(gid int, args *PutShardArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if servers, ok := kv.conf.Groups[gid]; ok {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply PutShardReply
			kv.mu.Unlock()
			DPrintf("G%v S%v -> G%v S%v PutShard RPC, Args %v Reply %v\n", kv.gid, kv.me, gid, si, args, &reply)
			ok := srv.Call("ShardKV.PutShard", args, &reply)
			kv.mu.Lock()
			if ok && reply.Err == OK {
				DPrintf("G%v S%v <- G%v S%v PutShard RPC, Args %v Reply %v\n", kv.gid, kv.me, gid, si, args, &reply)
				// check assume
				if kv.conf.Num == args.Num {
					kv.deleteShardsL(kv.gidToShards[gid], gid)
					kv.cond.Broadcast()
				}
				return
			}
		}
	}
}

func (kv *ShardKV) deleteShardsL(shards []int, gid int) {
	op := Op{}
	op.Type = DeleteShard
	op.DeleteShards = make([]int, len(shards))
	copy(op.DeleteShards, shards)
	op.Gid = gid
	op.Num = kv.conf.Num
	kv.rf.Start(op)
}

func (kv *ShardKV) waitApplyL(Gid int, Num int, index int, term int) bool {
	for kv.killed() || kv.putShardBuffer[Gid] < Num && kv.lastApplyIndex < index && kv.term == term {
		kv.cond.Wait()
	}
	return kv.putShardBuffer[Gid] >= Num
}

func (kv *ShardKV) PutShard(args *PutShardArgs, reply *PutShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.conf.Num > args.Num {
		//TODO ?
		reply.Err = OK
		return
	}
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		if kv.conf.Num < args.Num {
			DPrintf("G%v S%v PutShard Wait Update Config, CurrNum %v < ArgsNum %v\n", kv.gid, kv.me, kv.conf.Num, args.Num)
			kv.cond.Wait()
		} else {
			break
		}
	}
	if kv.killed() {
		return
	}

	op := Op{
		Type:          PutShard,
		Data:          args.Data,
		RequestBuffer: args.RequestBuffer,
		Gid:           args.GID,
		Num:           args.Num,
	}
	index, term, isLeader := kv.rf.Start(op)
	ok := isLeader
	ok = ok && kv.waitApplyL(args.GID, args.Num, index, term)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(args.Key)
	if kv.conf.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		_, ok := kv.waitShards[shard]
		if ok {
			DPrintf("G%v S%v Wait Shard %v WaitShards %v Data %v\n", kv.gid, kv.me, shard, kv.waitShards, kv.data)
			kv.cond.Wait()
		} else {
			break
		}
	}
	if kv.killed() {
		return
	}

	op := Op{
		Type:      Get,
		Key:       args.Key,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, isLeader := kv.rf.Start(op)
	ok := isLeader
	ok = ok && kv.waitL(args.ClerkId, args.RequestId, index, term, shard)
	if ok {
		reply.Err = OK
		reply.Value = kv.data[shard][args.Key]
		DPrintf("G%v S%v Data %v\n", kv.gid, kv.me, kv.data)
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(args.Key)
	if kv.conf.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}

	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		if _, ok := kv.waitShards[shard]; ok {
			DPrintf("G%v S%v Wait Shard %v WaitShards %v Data %v\n", kv.gid, kv.me, shard, kv.waitShards, kv.data)
			kv.cond.Wait()
		} else {
			break
		}
	}
	if kv.killed() {
		return
	}

	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, isLeader := kv.rf.Start(op)
	ok := isLeader
	ok = ok && kv.waitL(args.ClerkId, args.RequestId, index, term, shard)
	if ok {
		reply.Err = OK
		DPrintf("G%v S%v Data %v\n", kv.gid, kv.me, kv.data)
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
	DPrintf("G%v S%v Kill\n", kv.gid, kv.me)
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
	DPrintf("G%v S%v Make", kv.gid, kv.me)
	kv.persister = persister
	kv.data = make(map[int]map[string]string)
	kv.requestBuffer = make(map[int]map[int64]int64)
	kv.cond = sync.NewCond(&kv.mu)
	kv.restart = true

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.shardctrlerClerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.gidToShards = make(map[int][]int)
	kv.waitShards = make(map[int]bool)
	kv.putShardBuffer = make(map[int]int)

	// ... after init
	kv.readSnapshotL(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.apply()
	go kv.ticker()
	go kv.checkConfig()
	go kv.putShard()

	return kv
}

func (kv *ShardKV) writeSnapshotL() {
	DPrintf("G%v S%v Write Snapshot, LastI%v\n", kv.gid, kv.me, kv.lastApplyIndex)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.requestBuffer)
	e.Encode(kv.lastApplyIndex)
	e.Encode(kv.term)
	e.Encode(kv.conf)
	e.Encode(kv.gidToShards)
	e.Encode(kv.waitShards)
	e.Encode(kv.putShardBuffer)
	snapshot := w.Bytes()
	kv.rf.Snapshot(kv.lastApplyIndex, snapshot)
}

func (kv *ShardKV) readSnapshotL(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[int]map[string]string
	var requestBuffer map[int]map[int64]int64
	var lastApplyIndex int
	var term int
	var conf shardctrler.Config
	var gidToShards map[int][]int
	var waitShards map[int]bool
	var putShardBuffer map[int]int
	if d.Decode(&data) != nil ||
		d.Decode(&requestBuffer) != nil ||
		d.Decode(&lastApplyIndex) != nil ||
		d.Decode(&term) != nil ||
		d.Decode(&conf) != nil ||
		d.Decode(&gidToShards) != nil ||
		d.Decode(&waitShards) != nil ||
		d.Decode(&putShardBuffer) != nil {
		log.Fatalf("G%v S%v readSnapshot error", kv.gid, kv.me)
	}
	if kv.lastApplyIndex < lastApplyIndex {
		kv.data = data
		kv.requestBuffer = requestBuffer
		kv.lastApplyIndex = lastApplyIndex
		kv.term = term
		kv.conf = conf
		kv.gidToShards = gidToShards
		kv.waitShards = waitShards
		kv.putShardBuffer = putShardBuffer
	}
	DPrintf("G%v S%v Read Snapshot, LastI %v < %v, Wait for %v %v\n", kv.gid, kv.me, kv.lastApplyIndex, lastApplyIndex, kv.gidToShards, kv.waitShards)
}

