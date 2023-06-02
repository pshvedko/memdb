package index

import (
	"bytes"
	"fmt"
	"sync"
)

type UniqueIndex struct {
	sync.Map
}

func (i *UniqueIndex) Unique() bool {
	return true
}

func (i *UniqueIndex) Load(key interface{}) ([]interface{}, bool) {
	v, ok := i.Map.Load(key)
	if ok {
		return []interface{}{v}, true
	}
	return nil, false
}

func (i *UniqueIndex) Delete(key, _ interface{}) {
	i.Map.Delete(key)
}

type NonUniqueIndex struct {
	sync.Map
}

func (i *NonUniqueIndex) Unique() bool {
	return false
}

func Index(values ...interface{}) string {
	var b bytes.Buffer
	for _, value := range values {
		_, _ = fmt.Fprintf(&b, "::%v", value)
	}
	return b.String()
}
