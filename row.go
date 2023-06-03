package memdb

import "sync"

type Tx struct {
	tx *Tx
}

type Row struct {
	Item
	cas uint64
	rw  sync.RWMutex
	mx  sync.Mutex
	tx  *Tx
}

func (r *Row) acquire(t *Tx) bool {
	r.mx.Lock()
	defer r.mx.Unlock()
	for x := r.tx; x != nil; x = x.tx {
		if t == x {
			return false
		}
	}
	t.tx, r.tx = r.tx, t
	return true
}

func (r *Row) release(t *Tx) {
	r.mx.Lock()
	defer r.mx.Unlock()
	for x := &r.tx; *x != nil; x = &(*x).tx {
		if t == *x {
			*x, t.tx = t.tx, nil
			return
		}
	}
	panic(t)
}

func (r *Row) lock(t *Tx) {
	if !r.acquire(t) {
		panic(t)
	}
	r.rw.Lock()
}

func (r *Row) unlock(t *Tx) {
	r.release(t)
	r.rw.Unlock()
}

func (r *Row) read(t *Tx) bool {
	ok := r.acquire(t)
	if ok {
		r.rw.RLock()
	}
	return ok
}

func (r *Row) unread(t *Tx) {
	r.release(t)
	r.rw.RUnlock()
}

func (r *Row) committed(tx *Tx) bool {
	if r.read(tx) {
		defer r.unread(tx)
		return r.cas > 0
	}
	return true
}

func (r *Row) get(tx *Tx) (Item, uint64, bool) {
	if r.read(tx) {
		defer r.unread(tx)
		if r.Item != nil && r.cas > 0 {
			return r.Item, r.cas, true
		}
	}
	return nil, 0, false
}

func (r *Row) delete(tx *Tx) {
	r.lock(tx)
	defer r.unlock(tx)
	r.Item, r.cas = nil, 0
}
