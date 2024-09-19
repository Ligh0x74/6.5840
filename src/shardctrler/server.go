package shardctrler

import (
	"log"
	"sort"
	"time"
)

import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	requestBuffer  map[int64]int64
	lastApplyIndex int
	term           int
	cond           *sync.Cond

	configs []Config // indexed by config num
}

const (
	Join = iota
	Leave
	Move
	Query
)

type Op struct {
	// Your data here.
	Type int

	Servers map[int][]string // new GID -> servers mappings

	GIDs []int

	Shard int
	GID   int

	Num int // desired config number

	ClerkId   int64
	RequestId int64
}

func (sc *ShardCtrler) apply() {
	var msg raft.ApplyMsg
	for {
		msg = <-sc.applyCh
		sc.mu.Lock()
		if msg.CommandValid && msg.CommandIndex > sc.lastApplyIndex {
			DPrintf("S%v ApplyMsg, LastI %v Cmd %v\n", sc.me, sc.lastApplyIndex, msg.Command)
			sc.applyToStateL(&msg)
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) applyToStateL(msg *raft.ApplyMsg) {
	op := msg.Command.(Op)
	if !sc.testAndSetL(op.ClerkId, op.RequestId) {
		return
	}
	if op.Type == Join {
		sc.joinL(&op)
	} else if op.Type == Leave {
		sc.leaveL(&op)
	} else if op.Type == Move {
		sc.moveL(&op)
	}
	sc.lastApplyIndex = msg.CommandIndex
	sc.cond.Broadcast()
}

func (sc *ShardCtrler) joinL(op *Op) {
	prev := sc.configs[len(sc.configs)-1]
	curr := Config{}
	curr.Num = len(sc.configs)
	curr.Groups = make(map[int][]string)
	for i, v := range prev.Groups {
		curr.Groups[i] = v
	}
	for i, v := range op.Servers {
		curr.Groups[i] = v
	}
	curr.Shards = [NShards]int{}
	for i, v := range prev.Shards {
		curr.Shards[i] = v
	}
	sc.balanceL(&curr)
	sc.configs = append(sc.configs, curr)
}

func (sc *ShardCtrler) leaveL(op *Op) {
	prev := sc.configs[len(sc.configs)-1]
	curr := Config{}
	curr.Num = len(sc.configs)
	curr.Groups = make(map[int][]string)
	for i, v := range prev.Groups {
		curr.Groups[i] = v
	}
	for _, v := range op.GIDs {
		delete(curr.Groups, v)
	}
	curr.Shards = [NShards]int{}
	for i, v := range prev.Shards {
		curr.Shards[i] = v
	}
	sc.balanceL(&curr)
	sc.configs = append(sc.configs, curr)
}

func (sc *ShardCtrler) moveL(op *Op) {
	prev := sc.configs[len(sc.configs)-1]
	curr := Config{}
	curr.Num = len(sc.configs)
	curr.Groups = make(map[int][]string)
	for i, v := range prev.Groups {
		curr.Groups[i] = v
	}
	curr.Shards = [NShards]int{}
	for i, v := range prev.Shards {
		curr.Shards[i] = v
	}
	curr.Shards[op.Shard] = op.GID
	sc.configs = append(sc.configs, curr)
}

func (sc *ShardCtrler) balanceL(conf *Config) {
	if len(conf.Groups) == 0 {
		for i := range conf.Shards {
			conf.Shards[i] = 0
		}
		return
	}
	DPrintf("S%v Balance Before Config %v\n", sc.me, conf)
	avg := NShards / len(conf.Groups)
	rem := NShards % len(conf.Groups)
	cnt := make(map[int]int)
	queue := make([]int, 0)
	for i, gid := range conf.Shards {
		_, ok := conf.Groups[gid]
		if !ok || (cnt[gid] == avg && rem == 0) || cnt[gid] > avg {
			queue = append(queue, i)
			continue
		}
		if cnt[gid] < avg {
			cnt[gid]++
			continue
		}
		if cnt[gid] == avg {
			cnt[gid]++
			rem--
		}
	}
	tmp := make([]int, len(conf.Groups))
	for gid := range conf.Groups {
		tmp = append(tmp, gid)
	}
	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i] < tmp[j]
	})
	for _, gid := range tmp {
		if gid == 0 {
			continue
		}
		for cnt[gid] < avg {
			if len(queue) == 0 {
				log.Fatalf("Error Queue Length, cnt[gid] < avg")
			}
			conf.Shards[queue[0]] = gid
			queue = queue[1:]
			cnt[gid]++
		}
		if cnt[gid] == avg && rem > 0 {
			if len(queue) == 0 {
				log.Fatalf("Error Queue Length, cnt[gid] == avg && rem > 0")
			}
			conf.Shards[queue[0]] = gid
			queue = queue[1:]
			cnt[gid]++
			rem--
		}
	}
	DPrintf("S%v Balance After Config %v\n", sc.me, conf)
}

func (sc *ShardCtrler) testAndSetL(clerkId int64, requestId int64) bool {
	if sc.requestBuffer[clerkId] == requestId {
		return false
	}
	sc.requestBuffer[clerkId] = requestId
	return true
}

func (sc *ShardCtrler) waitL(clerkId int64, requestId int64, index int, term int) bool {
	for sc.requestBuffer[clerkId] != requestId && sc.lastApplyIndex < index && sc.term == term {
		sc.cond.Wait()
	}
	return sc.requestBuffer[clerkId] == requestId
}

func (sc *ShardCtrler) ticker() {
	for {
		time.Sleep(50 * time.Millisecond)
		sc.mu.Lock()
		term, _ := sc.rf.GetState()
		if sc.term < term {
			sc.term = term
			sc.cond.Broadcast()
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{
		Type:      Join,
		Servers:   args.Servers,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, isLeader := sc.rf.Start(op)
	ok := isLeader
	ok = ok && sc.waitL(args.ClerkId, args.RequestId, index, term)
	if !ok {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{
		Type:      Leave,
		GIDs:      args.GIDs,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, isLeader := sc.rf.Start(op)
	ok := isLeader
	ok = ok && sc.waitL(args.ClerkId, args.RequestId, index, term)
	if !ok {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{
		Type:      Move,
		Shard:     args.Shard,
		GID:       args.GID,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, isLeader := sc.rf.Start(op)
	ok := isLeader
	ok = ok && sc.waitL(args.ClerkId, args.RequestId, index, term)
	if !ok {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := Op{
		Type:      Query,
		Num:       args.Num,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, isLeader := sc.rf.Start(op)
	ok := isLeader
	ok = ok && sc.waitL(args.ClerkId, args.RequestId, index, term)
	if ok {
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
	} else {
		reply.WrongLeader = true
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.requestBuffer = make(map[int64]int64)
	sc.cond = sync.NewCond(&sc.mu)
	go sc.apply()
	go sc.ticker()

	return sc
}

