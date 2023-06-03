package memdb

import "sync"

type UniqueIndex struct {
	x sync.Map
}

func (i *UniqueIndex) LoadOrStore(key, value interface{}) (interface{}, bool) {
	return i.x.LoadOrStore(key, value)
}

func (i *UniqueIndex) Range(f func(key, value interface{}) bool) {
	i.x.Range(f)
}

func (i *UniqueIndex) Load(key interface{}) ([]interface{}, bool) {
	v, ok := i.x.Load(key)
	if ok {
		return []interface{}{v}, true
	}
	return nil, false
}

func (i *UniqueIndex) LoadAndDelete(key, _ interface{}) (interface{}, bool) {
	return i.x.LoadAndDelete(key)
}
