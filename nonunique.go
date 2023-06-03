package memdb

import "sync"

type NonUniqueIndex struct {
	x sync.Map
}

func (i *NonUniqueIndex) Load(key interface{}) (values []interface{}, ok bool) {
	u, ok := i.x.Load(key)
	if ok {
		u.(*sync.Map).Range(func(_, value interface{}) bool {
			values = append(values, value)
			return true
		})
	}
	return
}

func (i *NonUniqueIndex) LoadOrStore(key, value interface{}) (interface{}, bool) {
	u, _ := i.x.LoadOrStore(key, &sync.Map{})
	return u.(*sync.Map).LoadOrStore(value, value)
}

func (i *NonUniqueIndex) LoadAndDelete(key, value interface{}) (interface{}, bool) {
	u, ok := i.x.Load(key)
	if ok {
		return u.(*sync.Map).LoadAndDelete(value)
	}
	return nil, false
}

func (i *NonUniqueIndex) Range(f func(key, value interface{}) bool) {
	i.x.Range(func(key, u interface{}) bool {
		u.(*sync.Map).Range(func(_, value interface{}) bool {
			return f(key, value)
		})
		return true
	})
}
