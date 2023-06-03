package memdb

import (
	"bytes"
	"fmt"
	"sync"
)

type UniqueIndex struct {
	x sync.Map
}

func (i *UniqueIndex) Store(key, value interface{}) {
	i.x.Store(key, value)
}

func (i *UniqueIndex) LoadOrStore(key, value interface{}) (interface{}, bool) {
	return i.x.LoadOrStore(key, value)
}

func (i *UniqueIndex) LoadAndDelete(key interface{}) (interface{}, bool) {
	return i.x.LoadAndDelete(key)
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

func (i *UniqueIndex) Delete(key, _ interface{}) {
	i.x.Delete(key)
}

func (i *UniqueIndex) Unique() bool {
	return true
}

type NonUniqueIndex struct {
	x sync.Map
}

func (i *NonUniqueIndex) Load(key interface{}) (values []interface{}, ok bool) {
	u, ok := i.x.Load(key)
	if ok {
		u.(*sync.Map).Range(func(key, value interface{}) bool {
			values = append(values, value)
			return true
		})
	}
	return
}

func (i *NonUniqueIndex) Store(key, value interface{}) {
	//TODO implement me
	panic("implement me")
}

func (i *NonUniqueIndex) LoadOrStore(key, value interface{}) (interface{}, bool) {
	u, _ := i.x.LoadOrStore(key, &sync.Map{})
	switch {
	default:
		return u.(*sync.Map).LoadOrStore(value, value)
	}
}

func (i *NonUniqueIndex) LoadAndDelete(key interface{}) (interface{}, bool) {
	//TODO implement me
	panic("implement me")
}

func (i *NonUniqueIndex) Delete(key, value interface{}) {
	u, ok := i.x.Load(key)
	if ok {
		u.(*sync.Map).Delete(value)
	}
}

func (i *NonUniqueIndex) Range(f func(key, value interface{}) bool) {
	i.x.Range(f)
}

func (i *NonUniqueIndex) Unique() bool {
	return false
}

func Format(values ...interface{}) string {
	var b bytes.Buffer
	for _, value := range values {
		_, _ = fmt.Fprintf(&b, "::%v", value)
	}
	return b.String()
}
