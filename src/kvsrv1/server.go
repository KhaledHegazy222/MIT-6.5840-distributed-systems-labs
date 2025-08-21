package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

// TODO:: Remove Debug Whenever you finish the development
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVEntry struct {
	Version rpc.Tversion
	Value   string
}

type KVServer struct {
	mu    sync.RWMutex
	store map[string]KVEntry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		store: map[string]KVEntry{},
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	key := args.Key
	data, found := kv.store[key]
	if !found {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Err = rpc.OK
	reply.Value = data.Value
	reply.Version = data.Version
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	data, found := kv.store[args.Key]
	expectedVersion := rpc.Tversion(0)
	if found {
		expectedVersion = data.Version
	}
	// Update not create
	if args.Version != 0 && !found {
		// log.Printf("Version is not 0 and not found")
		reply.Err = rpc.ErrNoKey
		return
	}
	// create or update
	if expectedVersion != args.Version {
		// log.Printf("Version doesn't Match, Expected: %d, found: %d", expectedVersion, args.Version)

		reply.Err = rpc.ErrVersion
		return
	}
	newVersion := args.Version + 1
	kv.store[args.Key] = KVEntry{
		Value:   args.Value,
		Version: newVersion,
	}
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
