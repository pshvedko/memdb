package memdb

import (
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
	return i.Indexer(item.Field(i.Field...)...)
}

type Indexer func(...interface{}) string

type Collection struct {
	Indexes []Index
}

type Item interface {
	Field(...string) []interface{}
}

type Row struct {
	Item
	cas uint64
	sync.RWMutex
}

type Rollback struct {
	key   string
	index Index
}

func (c *Collection) Put(item Item) (uint64, bool) {
	one := &Row{}
	one.Lock()
	defer one.Unlock()
	row, key, ok := c.Indexes[0].Put(c.Indexes[0].Key(item), one)
	if ok {
		return c.update(row, item)
	}
	return c.insert(row, item, Rollback{index: c.Indexes[0], key: key})
}

func (c *Collection) update(row *Row, item Item) (uint64, bool) {
	row.Lock()
	defer row.Unlock()
	var rollbacks, unleashes []Rollback
	for _, index := range c.Indexes[1:] {
		one, key, ok := index.Put(index.Key(item), row)
		if ok {
			if one != row {
				return rollback(rollbacks...)
			}
			continue
		}
		rollbacks = append(rollbacks, Rollback{index: index, key: key})
		unleashes = append(unleashes, Rollback{index: index, key: index.Key(row)})
	}
	return commit(row, item, unleashes...)
}

func (c *Collection) insert(row *Row, item Item, rollbacks ...Rollback) (uint64, bool) {
	for _, index := range c.Indexes[1:] {
		_, key, ok := index.Put(index.Key(item), row)
		if ok {
			return rollback(rollbacks...)
		}
		rollbacks = append(rollbacks, Rollback{key: key, index: index})
	}
	return commit(row, item)
}

func rollback(rollbacks ...Rollback) (uint64, bool) {
	for _, r := range rollbacks {
		r.index.Delete(r.key)
	}
	return 0, false
}

func commit(row *Row, item Item, rollbacks ...Rollback) (uint64, bool) {
	rollback(rollbacks...)
	row.Item = item
	row.cas++
	return row.cas, true
}
