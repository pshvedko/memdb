package memdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/google/uuid"
)

type X1 struct {
	ID   uuid.UUID `json:"id"`
	Type string    `json:"type"`
	Name int       `json:"name"`
	Code int       `json:"code"`
	Ages []int     `json:"ages"`
	Time time.Time `json:"time"`

	F func(string) bool
}

func (x X1) Copy(item Item) (Item, bool) {
	if x.F != nil && !x.F("=") {
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
		x.F(name)
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
	case "time":
		return x.Time.UTC().Truncate(time.Second)
	default:
		panic(name)
	}
}

func newCollection(t testing.TB, items ...Item) Collection {
	t.Helper()
	collection := Collection{
		Indexes: []Index{
			{
				Field:   []string{"id"},
				Mapper:  &UniqueIndex{},
				Indexer: Format,
			}, {
				Field:   []string{"type", "name"},
				Mapper:  &UniqueIndex{},
				Indexer: Format,
			}, {
				Field:   []string{"code"},
				Mapper:  &UniqueIndex{},
				Indexer: Format,
			}, {
				Field:   []string{"time"},
				Mapper:  &NonUniqueIndex{},
				Indexer: Format,
			},
		},
	}
	for _, item := range items {
		collection.Put(&Tx{}, item, 0)
	}
	return collection
}

func printCollection(t testing.TB, collection Collection) {
	t.Helper()
	for _, i := range collection.Indexes {
		t.Log(i.Field)
		i.Range(func(key, value interface{}) bool {
			t.Log(key, value)
			return true
		})
	}
}

func BenchmarkCollection_Put(b *testing.B) {
	collection := newCollection(b)
	for i := 0; i < b.N; i++ {
		cas, ok := collection.Put(&Tx{}, X1{
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
	c1 := make(chan bool)
	c2 := make(chan bool)
	c3 := make(chan bool)
	go func() {
		collection.Put(&Tx{}, X1{
			ID:   id1,
			Type: "update",
			Code: 2, // <-- collision id2
			Name: 1,
			F: func(string) bool {
				return <-c1
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
			F: func(string) bool {
				return <-c2
			},
		}, 0)
		c3 <- true
	}()
	c1 <- true
	c2 <- true
	c1 <- true
	c2 <- true
	c1 <- true
	c2 <- true
	c1 <- true
	c2 <- true
	<-c3
	<-c3
	printCollection(t, collection)
}

func TestCollection_Put_insert_with_collision(t *testing.T) {
	collection := newCollection(t)
	c3 := make(chan bool)
	c2 := make(chan bool)
	c1 := make(chan bool)
	id1 := uuid.New()
	id2 := uuid.New()
	go func() {
		collection.Put(&Tx{}, X1{
			ID:   id2,
			Type: "insert",
			Code: 0, // <-- collision id1
			Name: 2,
			F: func(string) bool {
				return <-c2
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
			F: func(string) bool {
				return <-c1
			},
		}, 0)
		c3 <- true
	}()
	c1 <- true
	c2 <- true
	c1 <- true
	c2 <- true
	c1 <- true
	c2 <- true
	c1 <- true
	c2 <- true
	c1 <- true
	c1 <- false
	c2 <- true
	c2 <- true
	<-c3
	<-c3
	printCollection(t, collection)
}

func TestCollection_Put(t *testing.T) {
	collection := newCollection(t)
	type args struct {
		item Item
		cas  uint64
	}
	id1 := uuid.New()
	id2 := uuid.New()
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
					ID:   id1,
					Type: "audio",
					Code: 0,
					Name: 0,
				},
			},
			cas: 1,
			ok:  true,
		}, {
			args: args{
				item: X1{
					ID:   id1,
					Type: "audio",
					Code: 1,
					Name: 1,
				},
			},
			cas: 2,
			ok:  true,
		}, {
			args: args{
				item: X1{
					ID:   id2,
					Type: "audio",
					Code: 1,
					Name: 2,
				},
			},
			cas: 0,
			ok:  false,
		}, {
			args: args{
				item: X1{
					ID:   id2,
					Type: "audio",
					Code: 0,
					Name: 3,
				},
			},
			cas: 1,
			ok:  true,
		}, {
			args: args{
				item: X1{
					ID:   id2,
					Type: "audio",
					Code: 1,
					Name: 4,
				},
			},
			cas: 0,
			ok:  false,
		}, {
			args: args{
				item: X1{
					ID:   id2,
					Type: "audio",
					Code: 2,
					Name: 5,
				},
			},
			cas: 2,
			ok:  true,
		}, {
			args: args{
				item: X1{
					ID:   id2,
					Type: "audio",
					Code: 5,
					Name: 5,
				},
				cas: 5,
			},
			cas: 5,
			ok:  true,
		}, {
			args: args{
				item: X1{
					ID:   id2,
					Type: "audio",
					Code: 6,
					Name: 6,
				},
				cas: 5,
			},
			cas: 0,
			ok:  false,
		}, {
			args: args{
				item: X1{
					ID:   id2,
					Type: "audio",
					Code: 6,
					Name: 6,
					F:    func(string) bool { return false },
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
	printCollection(t, collection)
}

func TestCollection_Get(t *testing.T) {
	id := []uuid.UUID{
		uuid.New(),
		uuid.New(),
		uuid.New(),
		uuid.New(),
	}
	item := []Item{
		X1{
			ID:   id[0],
			Type: "get",
			Code: 0,
			Name: 0,
			Ages: nil,
		},
		X1{
			ID:   id[1],
			Type: "get",
			Code: 1,
			Name: 1,
			Ages: []int{},
		},
		X1{
			ID:   id[2],
			Type: "get",
			Code: 2,
			Name: 2,
			Ages: []int{2},
		},
		X1{
			ID:   id[3],
			Type: "get",
			Code: 3,
			Name: 3,
			Ages: []int{3, 3},
			Time: time.Now(),
		},
	}
	collection := newCollection(t, item...)
	type args struct {
		tx     *Tx
		i      int
		values [][]interface{}
	}
	tests := []struct {
		name string
		args args
		want []Item
	}{
		// TODO: Add test cases.
		{
			args: args{
				tx:     &Tx{},
				i:      0,
				values: [][]interface{}{{id[1]}},
			},
			want: []Item{item[1]},
		}, {
			args: args{
				tx:     &Tx{},
				i:      1,
				values: [][]interface{}{{"get", 1}},
			},
			want: []Item{item[1]},
		}, {
			args: args{
				tx:     &Tx{},
				i:      2,
				values: [][]interface{}{{1}},
			},
			want: []Item{item[1]},
		}, {
			args: args{
				tx:     &Tx{},
				i:      3,
				values: [][]interface{}{{time.Time{}}},
			},
			want: []Item{item[0], item[1], item[2]},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := collection.Get(tt.args.tx, tt.args.i, tt.args.values...); !assert.ElementsMatch(t, got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
	printCollection(t, collection)
}

func TestCollection_Delete(t *testing.T) {
	id := []uuid.UUID{
		uuid.New(),
		uuid.New(),
		uuid.New(),
		uuid.New(),
	}
	item := []Item{
		X1{
			ID:   id[0],
			Type: "get",
			Code: 0,
			Name: 0,
			Ages: nil,
		},
		X1{
			ID:   id[1],
			Type: "get",
			Code: 1,
			Name: 1,
			Ages: []int{},
		},
		X1{
			ID:   id[2],
			Type: "get",
			Code: 2,
			Name: 2,
			Ages: []int{2},
		},
		X1{
			ID:   id[3],
			Type: "get",
			Code: 3,
			Name: 3,
			Ages: []int{3, 3},
			Time: time.Now(),
		},
	}
	collection := newCollection(t, item...)
	type args struct {
		tx   *Tx
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
				tx:   &Tx{},
				item: item[1],
			},
			cas: 2,
			ok:  true,
		}, {
			args: args{
				tx:   &Tx{},
				item: item[1],
			},
			ok: false,
		}, {
			args: args{
				tx:   &Tx{},
				item: item[0],
				cas:  1,
			},
			ok: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cas, ok := collection.Delete(tt.args.tx, tt.args.item, tt.args.cas)
			if cas != tt.cas {
				t.Errorf("Delete() cas = %v, want %v", cas, tt.cas)
			}
			if ok != tt.ok {
				t.Errorf("Delete() ok = %v, want %v", ok, tt.ok)
			}
		})
	}
	printCollection(t, collection)
}
