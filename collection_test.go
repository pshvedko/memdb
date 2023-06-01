package memdb

import (
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
)

type X1 struct {
	ID   uuid.UUID `json:"id"`
	Type string    `json:"type"`
	Code int       `json:"code"`
	Name int       `json:"name"`

	F func() bool
}

func (x X1) Copy(item Item) (Item, bool) {
	if x.F != nil && !x.F() {
		return nil, false
	}
	switch item.(type) {
	case nil:
	case X1:
	default:
		panic(item)
	}
	return x, true
}

func (x X1) Field(name string) interface{} {
	if x.F != nil {
		x.F()
	}
	switch name {
	case "id":
		return x.ID
	case "type":
		return x.Type
	case "code":
		return x.Code
	case "name":
		return x.Name
	default:
		panic(name)
	}
}

func newCollection(h testing.TB) Collection {
	h.Helper()
	return Collection{
		Indexes: []Index{
			{
				Field:   []string{"id"},
				Mapper:  &sync.Map{},
				Indexer: fmt.Sprint,
			}, {
				Field:   []string{"type", "name"},
				Mapper:  &sync.Map{},
				Indexer: fmt.Sprint,
			}, {
				Field:   []string{"code"},
				Mapper:  &sync.Map{},
				Indexer: fmt.Sprint,
			},
		},
	}
}

func BenchmarkCollection_Put(b *testing.B) {
	collection := newCollection(b)
	var tx Tx
	for i := 0; i < b.N; i++ {
		cas, ok := collection.Put(&tx, X1{
			ID:   uuid.UUID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)},
			Type: "audio",
			Code: i,
			Name: i,
		}, 0)
		if !ok || cas == 0 {
			b.FailNow()
		}
	}
}

// DEADLOCK
//
//  insert row1= code:1 name:1
//  insert row2= code:2 name:2
//
//  update row1= code:2 name:1                  |
//  row1.committed ? true                       *
//    c.update(row1)                            |
//      row1.lock() <---------------------------X
//        put index code:2 -> return row2       |
//        row2.committed ? true                -*
//                                              |
//                                              |  update row2= code:1 name:2
//                                              *- row2.committed ? true
//                                              |    c.update(row2)
//          SLEEP!!!                            X----> row2.lock()
//                                              |        put index code:1 -> return row1
//                                              *-       row1.committed ? +
//                                              |                         |
//                                              |                         |
//                                              |                         |
//                                              |                         |
//          rollback                            |                         |
//      row1.unlock() X------------------------->                         + true
//                                              |          rollback
//                                              <----X row2.unlock()
//                                              |
//                                              |
//  update row1= code:2 name:1                  |  update row2= code:1 name:2
//  row1.committed ? true                      -*- row2.committed ? true
//    c.update(row1)                            |    c.update(row2)
//      row1.lock() <---------------------------X----> row2.lock()
//        put index code:2 -> return row2       |        put index code:1 -> return row1
//        row2.committed ???                DEAD*LOCK    row1.committed ???
//                                              |
//
func TestCollection_Put_update_with_collision(t *testing.T) {
	collection := newCollection(t)
	id1 := uuid.New()
	id2 := uuid.New()
	collection.Put(&Tx{}, X1{
		ID:   id1,
		Type: "update",
		Code: 1,
		Name: 1,
	}, 0)
	collection.Put(&Tx{}, X1{
		ID:   id2,
		Type: "update",
		Code: 2,
		Name: 2,
	}, 0)
	c1 := make(chan bool, 2)
	c2 := make(chan bool, 2)
	c3 := make(chan bool, 2)
	go func() {
		collection.Put(&Tx{}, X1{
			ID:   id1,
			Type: "update",
			Code: 2, // <-- collision id2
			Name: 1,
			F: func() bool {
				c2 <- <-c1
				return true
			},
		}, 0)
		c3 <- true
	}()
	go func() {
		collection.Put(&Tx{}, X1{
			ID:   id2,
			Type: "update",
			Code: 1,
			Name: 2, // <-- collision id1
			F: func() bool {
				c1 <- <-c2
				return true
			},
		}, 0)
		c3 <- true
	}()
	c2 <- true
	<-c3
	<-c3
	for _, index := range collection.Indexes {
		t.Log(index.Field)
		index.Range(func(key, value interface{}) bool {
			t.Log(key, value)
			return true
		})
	}
}

func TestCollection_Put_insert_with_collision(t *testing.T) {
	collection := newCollection(t)
	c3 := make(chan bool, 1)
	c2 := make(chan bool, 1)
	c1 := make(chan bool, 1)
	id1 := uuid.New()
	id2 := uuid.New()
	go func() {
		collection.Put(&Tx{}, X1{
			ID:   id2,
			Type: "insert",
			Code: 0, // <-- collision id1
			Name: 2,
			F: func() bool {
				<-c2
				return true
			},
		}, 0)
		c3 <- true
	}()
	go func() {
		collection.Put(&Tx{}, X1{
			ID:   id1,
			Type: "insert",
			Code: 0,
			Name: 1,
			F: func() bool {
				c2 <- <-c1
				return false
			},
		}, 0)
		c3 <- true
	}()
	c1 <- true
	c1 <- true
	c1 <- true
	c1 <- true
	c1 <- true
	<-c3
	c2 <- true
	<-c3
	for _, index := range collection.Indexes {
		t.Log(index.Field)
		index.Range(func(key, value interface{}) bool {
			t.Log(key, value)
			return true
		})
	}
}

func TestCollection_Put(t *testing.T) {
	collection := newCollection(t)
	type args struct {
		item Item
		cas  uint64
	}
	tests := []struct {
		name string
		args args
		cas  uint64
		ok   bool
	}{
		// TODO: Add test cases.
		{
			args: args{
				item: &X1{
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae0000"),
					Type: "audio",
					Code: 0,
					Name: 0,
				},
			},
			cas: 1,
			ok:  true,
		},
		{
			args: args{
				item: X1{
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae0000"),
					Type: "audio",
					Code: 1,
					Name: 1,
				},
			},
			cas: 2,
			ok:  true,
		},
		{
			args: args{
				item: X1{
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae1111"),
					Type: "audio",
					Code: 1,
					Name: 2,
				},
			},
			cas: 0,
			ok:  false,
		},
		{
			args: args{
				item: X1{
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae1111"),
					Type: "audio",
					Code: 0,
					Name: 3,
				},
			},
			cas: 1,
			ok:  true,
		},
		{
			args: args{
				item: X1{
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae1111"),
					Type: "audio",
					Code: 1,
					Name: 4,
				},
			},
			cas: 0,
			ok:  false,
		},
		{
			args: args{
				item: X1{
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae1111"),
					Type: "audio",
					Code: 2,
					Name: 5,
				},
			},
			cas: 2,
			ok:  true,
		},
		{
			args: args{
				item: X1{
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae1111"),
					Type: "audio",
					Code: 5,
					Name: 5,
				},
				cas: 5,
			},
			cas: 5,
			ok:  true,
		},
		{
			args: args{
				item: X1{
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae1111"),
					Type: "audio",
					Code: 6,
					Name: 6,
				},
				cas: 5,
			},
			cas: 0,
			ok:  false,
		},
		{
			args: args{
				item: X1{
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae1111"),
					Type: "audio",
					Code: 6,
					Name: 6,
					F:    func() bool { return false },
				},
				cas: 6,
			},
			cas: 0,
			ok:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cas, ok := collection.Put(&Tx{}, tt.args.item, tt.args.cas)
			if cas != tt.cas {
				t.Errorf("Put() cas = %v, want %v", cas, tt.cas)
			}
			if ok != tt.ok {
				t.Errorf("Put() ok = %v, want %v", ok, tt.ok)
			}
		})
	}
	for _, index := range collection.Indexes {
		t.Log(index.Field)
		index.Range(func(key, value interface{}) bool {
			t.Log(key, value)
			return true
		})
	}
}
