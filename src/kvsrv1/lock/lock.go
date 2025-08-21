package lock

import (
	"log"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck          kvtest.IKVClerk
	lockKey     string
	identifier  string
	lastVersion rpc.Tversion
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, identifier: kvtest.RandValue(8), lockKey: l}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey {
			// No one have acquired the lock before, trying ...
			err = lk.ck.Put(lk.lockKey, lk.identifier, 0)
		} else if value == "0" {
			// Lock is released trying to acquire it
			err = lk.ck.Put(lk.lockKey, lk.identifier, version)
		} else if value != lk.identifier {
			// lock is already acquired, retry again (POLLING)
			continue
		}
		if err == rpc.OK {
			break
		}
	}
}

func (lk *Lock) Release() {
	value, version, err := lk.ck.Get(lk.lockKey)
	if err == rpc.ErrNoKey {
		log.Fatalf("Key Not Found, exiting...")
	}
	if value == lk.identifier {
		for {
			err = lk.ck.Put(lk.lockKey, "0", version)
			if err == rpc.ErrMaybe {
				_, latestVersion, _ := lk.ck.Get(lk.lockKey)
				if latestVersion > version {
					// put has been done successfully
					break
				}
			} else if err == rpc.OK {
				break
			}

		}
	} else {
		log.Printf("The lock is locked by another client, skipping...")
	}
}
