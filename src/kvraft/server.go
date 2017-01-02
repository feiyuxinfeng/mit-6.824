package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
)

type OpType int

const (
	GET = OpType(iota)
	PUT
	APPEND
)

func (ty OpType) String() string {
	switch ty {
	case GET:
		return "GET"
	case PUT:
		return "PUT"
	case APPEND:
		return "APPEND"
	default:
		return "Unkown"
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Key   string
	Value string
	Xid   int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state   map[string]string
	chanMap map[int64]chan Op
	xidMap  map[int64]Op
}

func (kv *RaftKV) processApplyLog() {
	for m := range kv.applyCh {
		kv.mu.Lock()
		if op, ok := (m.Command).(Op); ok {
			switch op.Type {
			case GET:
				op.Value = kv.state[op.Key]
			case PUT:
				kv.state[op.Key] = op.Value
			case APPEND:
				if _, exists := kv.state[op.Key]; exists {
					kv.state[op.Key] = kv.state[op.Key] + op.Value
				} else {
					kv.state[op.Key] = op.Value
				}
			}

			if ch, ok := kv.chanMap[op.Xid]; ok {
				delete(kv.chanMap, op.Xid)

				DPrintf("Found Chan: server %v receive apply log %v, xid: %v, op: %v", kv.me, m.Index, op.Xid, op.Type.String())
				go func() {
					ch <- op
				}()
			} else {
				DPrintf("No Chan: server %v receive apply log %v, xid: %v, op: %v", kv.me, m.Index, op.Xid, op.Type.String())
			}
		}

		kv.mu.Unlock()
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type: GET,
		Xid:  args.Xid,
		Key:  args.Key,
	}

	kv.mu.Lock()
	resultCh := make(chan Op, 1)

	if _, exists := kv.chanMap[args.Xid]; !exists {
		kv.chanMap[args.Xid] = resultCh
	} else {
		log.Fatalf("chan Op for Xid %v exists", args.Xid)
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(op)

	DPrintf("server receive %v Get RPC xid: %v, Index: %v, isLeader: %v", kv.me, args.Xid, index, isLeader)

	reply.WrongLeader = !isLeader

	defer func() {
		if reply.Err != "" {
			kv.mu.Lock()
			delete(kv.chanMap, args.Xid)
			kv.mu.Unlock()
		}
	}()

	if isLeader == false {
		reply.Err = Err(fmt.Sprintf("server %v is not a leader", kv.me))
		return
	}

	resultOp := <-resultCh

	if resultOp.Xid != args.Xid {
		reply.Err = "conflit xid"
		reply.Value = ""
		return
	}
	reply.Err = ""
	reply.Value = resultOp.Value
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Xid:   args.Xid,
	}
	if args.Op == "Put" {
		op.Type = PUT
	} else if args.Op == "Append" {
		op.Type = APPEND
	} else {
		log.Fatalf("Unkown rpc type %v", args.Op)
		return
	}

	kv.mu.Lock()

	resultCh := make(chan Op, 1)

	if _, exists := kv.chanMap[args.Xid]; !exists {
		kv.chanMap[args.Xid] = resultCh
	} else {
		log.Fatalf("chan Op for Xid %v exists", args.Xid)
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(op)

	DPrintf("server %v receive %v RPC xid: %v, Index: %v, isLeader: %v", args.Op, args.Xid, index, isLeader)
	reply.WrongLeader = !isLeader

	defer func() {
		if reply.Err != "" {
			kv.mu.Lock()
			delete(kv.chanMap, args.Xid)
			kv.mu.Unlock()
		}
	}()

	if isLeader == false {
		reply.Err = Err(fmt.Sprintf("server %v is not a leader", kv.me))
		return
	}

	resultOp := <-resultCh
	if resultOp.Xid != args.Xid {
		reply.Err = "conflit xid"
		return
	}

	reply.Err = ""
	return
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.state = make(map[string]string)
	kv.chanMap = make(map[int64]chan Op)
	kv.xidMap = make(map[int64]Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.processApplyLog()

	return kv
}
