package mvcc

import "log"

// 事务
type Transaction struct {
	kv       *KV       // kv
	xid      int64     // 事务id
	rollback []*action // 回滚操作列表
}

// 记录
type record struct {
	key        string // 键
	value      []byte // 值
	createdXid int64  // 创建事务id
	expiredXid int64  // 删除事务id
}

// 操作
type action struct {
	method string // 操作
	key    string // 键
}

// 记录是否可见
func (t *Transaction) isVisable(r *record) bool {
	// 由其它活跃事务创建的记录不可见
	if t.kv.activeXids.Contain(r.createdXid) && r.createdXid != t.xid {
		return false
	}

	// 被非活跃事务或者当前事务删除的记录不可见
	if r.expiredXid != 0 && (!t.kv.activeXids.Contain(r.expiredXid) || r.expiredXid == t.xid) {
		return false
	}

	// 添加记录锁
	t.kv.lockManager.locks.Add(lock{
		xid: t.xid,
		key: r.key,
	})

	// 其余事务可见
	return true
}

// 记录是否被锁
func (t *Transaction) isLocked(re *record) bool {

}

// 添加记录
func (t *Transaction) add(key string, value []byte) {
	r := &record{
		key:        key,
		value:      value,
		createdXid: t.xid,
		expiredXid: 0,
	}

	a := &action{
		method: "delete",
		key:    key,
	}

	t.rollback = append(t.rollback, a) // 添加回滚操作

	t.kv.collection[key] = r
}

// 删除记录
func (t *Transaction) Delete(key string) {
	r, ok := t.kv.collection[key]
	if !ok {
		log.Println("key:", key, "missing")
	}

	if t.isVisable(r) {
		if t.isLocked(r) {
			log.Println("key:", key, "locked")
		} else {
			r.expiredXid = t.xid
			
			a := &action{
				method: "add",
				key:    key,
			}
			
			t.rollback = append(t.rollback, a)
		}
	}
}

// 更新记录
func (t *Transaction) Set(key string, value []byte) {
	t.Delete(key)
	t.add(key, value)
}

// 查询记录
func (t *Transaction) Get(key string) {

}

// 提交
func (t *Transaction) Commit() {
	t.kv.activeXids.Delete(t.xid)
}

// 回滚
func (t *Transaction) Rollback() {
	for i := len(t.rollback) - 1; i >= 0; i-- {
		action := t.rollback[i]
		if action.method == "add" {
			t.kv.collection[action.key].expiredXid = 0
		}
		if action.method == "delete" {
			t.kv.collection[action.key].expiredXid = t.xid
		}
	}

	t.kv.activeXids.Delete(t.xid)
}