package memdb

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
)

type Mapper interface {
	Load(key interface{}) (value interface{}, ok bool)
	Store(key, value interface{})
	LoadOrStore(key, value interface{}) (actual interface{}, loaded bool)
	LoadAndDelete(key interface{}) (value interface{}, loaded bool)
	Delete(key interface{})
	Range(f func(key, value interface{}) bool)
}

type Indexer func(...interface{}) string

type Locker interface {
	Lock(key string)
	TryUnlock(key string) error
	Unlock(key string)
}

type Index struct {
	Indexer
	Mapper
	Field []string
	Locker
}

func (i Index) Put(key string, row *Row) (*Row, string, bool) {
	v, ok := i.LoadOrStore(key, row)
	return v.(*Row), key, ok
}

func (i Index) Key(item Item) string {
	var b bytes.Buffer
	for _, f := range i.Field {
		_, _ = fmt.Fprintf(&b, "::%v", item.Field(f))
	}
	return i.Indexer(b.String())
}

type Collection struct {
	Indexes []Index
}

type Item interface {
	Field(string) interface{}
	Copy(Item) (Item, bool)
}

type Row struct {
	Item
	cas uint64
	sync.RWMutex
}

var x int32

func (r *Row) Lock() {
	println(fmt.Sprintf("%*sLock %p", atomic.AddInt32(&x, 1)-1, "", r))
	r.RWMutex.Lock()
}

func (r *Row) Unlock() {
	println(fmt.Sprintf("%*sUnlock %p", atomic.AddInt32(&x, -1), "", r))
	r.RWMutex.Unlock()
}

func (r *Row) RLock() {
	println(fmt.Sprintf("%*sRLock %p", atomic.AddInt32(&x, 1)-1, "", r))
	r.RWMutex.RLock()
}

func (r *Row) RUnlock() {
	println(fmt.Sprintf("%*sRUnlock %p", atomic.AddInt32(&x, -1), "", r))
	r.RWMutex.RUnlock()
}

func (r *Row) committed() bool {
	r.RLock()
	defer r.RUnlock()
	return r.cas > 0
}

type Rollback struct {
	key   string
	index Index
}

// Put
//
//  insert row1= code:1 name:1
//  insert row2= code:2 name:2
//
//  update row1= code:2 name:1                  |
//  row1.committed ? true                       *
//    c.update(row1)                            |
//      row1.Lock() <---------------------------+
//        put index code:2 -> return row2       |
//        row2.committed ? true                -*
//                                              |
//                                              |  update row2= code:1 name:2
//                                              *- row2.committed ? true
//                                              |    c.update(row2)
//          SLEEP!!!                            +----> row2.Lock()
//                                              |        put index code:1 -> return row1
//                                              *-       row1.committed ? +
//                                              |                         |
//                                              |                         |
//                                              |                         |
//                                              |                         |
//          rollback                            |                         |
//      row1.Unlock() X-------------------------+                         + true
//                                              |          rollback
//                                              +----X row2.Unlock()
//                                              |
//                                              |
//  update row1= code:2 name:1                  |  update row2= code:1 name:2
//  row1.committed ? true                      -*- row2.committed ? true
//    c.update(row1)                            |    c.update(row2)
//      row1.Lock() <---------------------------+----> row2.Lock()
//        put index code:2 -> return row2       |        put index code:1 -> return row1
//        row2.committed ???                DEAD*LOCK    row1.committed ???
//                                              |
//
func (c Collection) Put(item Item, cas uint64) (uint64, bool) {
	one := &Row{}
	one.Lock()
	defer one.Unlock()
index:
	row, key, ok := c.Indexes[0].Put(c.Indexes[0].Key(item), one)
	if ok {
		if row.committed() {
			return c.update(row, item, cas)
		}
		goto index
	}
	return c.insert(row, item, cas, Rollback{index: c.Indexes[0], key: key})
}

func (c Collection) update(row *Row, item Item, cas uint64) (uint64, bool) {
	row.Lock()
	defer row.Unlock()
	var rollbacks, unleashes []Rollback
	for _, index := range c.Indexes[1:] {
	index:
		one, key, ok := index.Put(index.Key(item), row)
		if ok {
			if one != row {
				if one.committed() {
					return c.rollback(rollbacks...)
				}
				goto index
			}
			continue
		}
		rollbacks = append(rollbacks, Rollback{index: index, key: key})
		unleashes = append(unleashes, Rollback{index: index, key: index.Key(row)})
	}
	return c.end(rollbacks, row, item, cas, unleashes...)
}

func (c Collection) insert(row *Row, item Item, cas uint64, rollbacks ...Rollback) (uint64, bool) {
	for _, index := range c.Indexes[1:] {
	index:
		one, key, ok := index.Put(index.Key(item), row)
		if ok {
			if one.committed() {
				return c.rollback(rollbacks...)
			}
			goto index
		}
		rollbacks = append(rollbacks, Rollback{key: key, index: index})
	}
	return c.end(rollbacks, row, item, cas)
}

func (c Collection) rollback(rollbacks ...Rollback) (uint64, bool) {
	for _, r := range rollbacks {
		r.index.Delete(r.key)
	}
	return 0, false
}

func (c Collection) commit(row *Row, item Item, cas uint64, rollbacks ...Rollback) (uint64, bool) {
	switch {
	case cas == 0:
		cas = row.cas + 1
	case cas <= row.cas:
		return 0, false
	}
	c.rollback(rollbacks...)
	item, ok := item.Copy(row.Item)
	if !ok {
		return 0, false
	}
	row.Item = item
	row.cas = cas
	return cas, true
}

func (c Collection) end(rollbacks []Rollback, row *Row, item Item, cas uint64, unleashes ...Rollback) (uint64, bool) {
	cas, ok := c.commit(row, item, cas, unleashes...)
	if !ok {
		return c.rollback(rollbacks...)
	}
	return cas, ok
}
