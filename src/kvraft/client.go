package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	mrand "math/rand"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader  int
	seenXid int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	return ck
}

func (ck *Clerk) chooseLeader(prev int) int {
	if prev < 0 {
		return ck.leader
	}
	leader := prev
	for leader == prev {
		leader = mrand.Intn(len(ck.servers))
	}
	return leader
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply, timeoutMs int64) bool {
	if timeoutMs == 0 {
		ok := ck.servers[server].Call("RaftKV.Get", args, reply)
		return ok
	} else {
		retChan := make(chan bool, 1)
		go func(reply *GetReply, retChan chan bool) {
			ok := ck.servers[server].Call("RaftKV.Get", args, reply)
			retChan <- ok
		}(reply, retChan)

		select {
		case ok := <-retChan:
			return ok
		case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
			return false
		}
	}
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply, timeoutMs int64) bool {
	if timeoutMs == 0 {
		ok := ck.servers[server].Call("RaftKV.PutAppend", args, reply)
		return ok
	} else {
		retChan := make(chan bool, 1)
		go func(reply *PutAppendReply, retChan chan bool) {
			ok := ck.servers[server].Call("RaftKV.PutAppend", args, reply)
			retChan <- ok
		}(reply, retChan)

		select {
		case ok := <-retChan:
			return ok
		case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
			return false
		}
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:     key,
		Xid:     nrand(),
		SeenXid: ck.seenXid,
	}
	leader := -1
	for {
		leader = ck.chooseLeader(leader)
		reply := GetReply{}
		ret := ck.sendGet(leader, &args, &reply, 0)
		if ret == true {
			if reply.WrongLeader {
				DPrintf("Get(Xid: %v): %v", args.Xid, reply.Err)
				continue
			}
			if reply.Err != "" {
				DPrintf("Get(Xid: %v): %v", args.Xid, reply.Err)
				continue
			}
			DPrintf("Get(Xid: %v) success.", args.Xid)
			ck.seenXid = args.Xid
			return reply.Value
		} else {
			DPrintf("Get(xid: %v) RPC fails", args.Xid)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		Xid:     nrand(),
		SeenXid: ck.seenXid,
	}
	leader := -1
	for {
		leader = ck.chooseLeader(leader)
		reply := PutAppendReply{}
		ret := ck.sendPutAppend(leader, &args, &reply, 0)
		if ret == true {
			if reply.WrongLeader {
				// DPrintf("%v(Xid: %v): %v", op, args.Xid, reply.Err)
				continue
			}
			if reply.Err != "" {
				DPrintf("%v(Xid: %v): %v", op, args.Xid, reply.Err)
				continue
			}
			ck.leader = leader
			DPrintf("%v(Xid: %v) success.", op, args.Xid)
			ck.seenXid = args.Xid
			return
		} else {
			DPrintf("%v(xid: %v) RPC fails", op, args.Xid)
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
