package memdb

import (
	"bytes"
	"fmt"
	"sync"
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

type Index struct {
	Indexer
	Mapper
	Field []string
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
}

type Row struct {
	Item
	cas uint64
	sync.RWMutex
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

func (c Collection) Put(item Item) (uint64, bool) {
	one := &Row{}
	one.Lock()
	defer one.Unlock()
index:
	row, key, ok := c.Indexes[0].Put(c.Indexes[0].Key(item), one)
	if ok {
		if row.committed() {
			return c.update(row, item)
		}
		goto index
	}
	return c.insert(row, item, Rollback{index: c.Indexes[0], key: key})
}

func (c Collection) update(row *Row, item Item) (uint64, bool) {
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
	return c.commit(row, item, unleashes...)
}

func (c Collection) insert(row *Row, item Item, rollbacks ...Rollback) (uint64, bool) {
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
	return c.commit(row, item)
}

func (c Collection) rollback(rollbacks ...Rollback) (uint64, bool) {
	for _, r := range rollbacks {
		r.index.Delete(r.key)
	}
	return 0, false
}

func (c Collection) commit(row *Row, item Item, rollbacks ...Rollback) (uint64, bool) {
	c.rollback(rollbacks...)
	row.Item = item
	row.cas++
	return row.cas, true
}
