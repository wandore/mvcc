package mvcc

import "github.com/wandore/set"

// 锁管理器
type lockManager struct {
	locks *set.Set // 锁集合
}

// 锁
type lock struct {
	xid int64  // 事务id
	key string // 键
}

// 构造锁管理器实例
func newLockManager() *lockManager {
	return &lockManager{locks: set.New()}
}
