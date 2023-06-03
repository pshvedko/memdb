package memdb

import (
	"bytes"
	"fmt"
)

type Indexer func(...interface{}) string

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

func (i Index) Put(key string, row *Row) (*Row, bool) {
	v, ok := i.LoadOrStore(key, row)
	return v.(*Row), ok
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

func Format(values ...interface{}) string {
	var b bytes.Buffer
	for _, value := range values {
		_, _ = fmt.Fprintf(&b, "::%v", value)
	}
	return b.String()
}
