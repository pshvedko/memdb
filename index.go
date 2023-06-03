package memdb

import (
	"bytes"
	"fmt"
	"sync"
)

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

func (i *UniqueIndex) Delete(key, _ interface{}) {
	i.x.Delete(key)
}

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
	switch {
	default:
		return u.(*sync.Map).LoadOrStore(value, value)
	}
}

func (i *NonUniqueIndex) Delete(key, value interface{}) {
	u, ok := i.x.Load(key)
	if ok {
		u.(*sync.Map).Delete(value)
	}
}

func (i *NonUniqueIndex) Range(f func(key, value interface{}) bool) {
	i.x.Range(func(key, u interface{}) bool {
		u.(*sync.Map).Range(func(_, value interface{}) bool {
			return f(key, value)
		})
		return true
	})
}

func Format(values ...interface{}) string {
	var b bytes.Buffer
	for _, value := range values {
		_, _ = fmt.Fprintf(&b, "::%v", value)
	}
	return b.String()
}
