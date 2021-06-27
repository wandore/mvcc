package mvcc

import (
	"github.com/wandore/set"
	"sync/atomic"
)

// kv
type KV struct {
	collection  map[string]*record // 记录集合
	nextXid     int64              // 全局事务id
	activeXids  *set.Set           // 活跃事务集合，活跃意指同时执行，set内部有读写锁保证并发安全
	lockManager *lockManager       // 锁管理器
}

// 构造kv实例
func NewKV() *KV {
	return &KV{
		collection:  make(map[string]*record, 0),
		nextXid:     0,
		activeXids:  set.New(),
		lockManager: newLockManager(),
	}
}

// 构造事务实例
func (kv *KV) NewTransaction() *Transaction {
	tr := &Transaction{
		kv:       kv,
		xid:      atomic.AddInt64(&kv.nextXid, 1),
		rollback: make([]*action, 0),
	}

	kv.activeXids.Add(tr.xid)

	return tr
}
