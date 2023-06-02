package memdb

type Mapper interface {
	Load(key interface{}) (value []interface{}, ok bool)
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

type Item interface {
	Field(string) interface{}
	Copy(Item) (Item, bool)
}

type Rollback struct {
	key   string
	index Index
}

type Index struct {
	Indexer
	Mapper
	Field []string
}

func (i Index) Get(key string) (rows []*Row) {
	vv, ok := i.Load(key)
	if ok {
		for _, v := range vv {
			rows = append(rows, v.(*Row))
		}
	}
	return
}

func (i Index) Put(key string, row *Row) (*Row, string, bool) {
	v, ok := i.LoadOrStore(key, row)
	return v.(*Row), key, ok
}

func (i Index) Key(item Item) string {
	var values []interface{}
	for _, f := range i.Field {
		values = append(values, item.Field(f))
	}
	return i.Index(values...)
}

func (i Index) Index(values ...interface{}) string {
	return i.Indexer(values...)
}

type Collection struct {
	Indexes []Index
}

// Get ...
func (c Collection) Get(tx *Tx, i int, values ...[]interface{}) []Item {
	var rows []*Row
	for _, value := range values {
		rows = append(rows, c.Indexes[i].Get(c.Indexes[i].Index(value...))...)
	}
	var items []Item
	for _, row := range rows {
		item, _, ok := row.get(tx)
		if ok {
			items = append(items, item)
		}
	}
	return items
}

// Put ...
func (c Collection) Put(tx *Tx, item Item, cas uint64) (uint64, bool) {
	one := &Row{}
	one.lock(tx)
	defer one.unlock(tx)
index:
	row, key, ok := c.Indexes[0].Put(c.Indexes[0].Key(item), one)
	if ok {
		if row.committed(tx) {
			return c.update(tx, row, item, cas)
		}
		goto index
	}
	return c.insert(tx, row, item, cas, Rollback{index: c.Indexes[0], key: key})
}

func (c Collection) update(tx *Tx, row *Row, item Item, cas uint64) (uint64, bool) {
	row.lock(tx)
	defer row.unlock(tx)
	var rollbacks, unleashes []Rollback
	for _, index := range c.Indexes[1:] {
	index:
		one, key, ok := index.Put(index.Key(item), row)
		if ok {
			if one != row {
				if one.committed(tx) {
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

func (c Collection) insert(tx *Tx, row *Row, item Item, cas uint64, rollbacks ...Rollback) (uint64, bool) {
	for _, index := range c.Indexes[1:] {
	index:
		one, key, ok := index.Put(index.Key(item), row)
		if ok {
			if one.committed(tx) {
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
	if cas == 0 {
		cas = row.cas + 1
	} else if cas <= row.cas {
		return 0, false
	}
	item, ok := item.Copy(row.Item)
	if !ok {
		return 0, false
	}
	c.rollback(rollbacks...)
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
