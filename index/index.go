package index

import (
	"bytes"
	"fmt"
	"sync"
)

type UniqueIndex struct {
	sync.Map
}

func (i *UniqueIndex) Load(key interface{}) ([]interface{}, bool) {
	v, ok := i.Map.Load(key)
	if ok {
		return []interface{}{v}, true
	}
	return nil, false
}

type NonUniqueIndex struct {
	sync.Map
}

func Index(values ...interface{}) string {
	var b bytes.Buffer
	for _, value := range values {
		_, _ = fmt.Fprintf(&b, "::%v", value)
	}
	return b.String()
}
