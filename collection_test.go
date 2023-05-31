package memdb

import (
	"fmt"
	"github.com/google/uuid"
	"sync"
	"testing"
	"time"
)

type F func() bool

type X struct {
	ID   uuid.UUID `json:"id"`
	Type string    `json:"type"`
	Code int       `json:"code"`
	Name int       `json:"name"`
	F
}

func (x X) Copy(item Item) (Item, bool) {
	switch item.(type) {
	case nil:
	case X:
	default:
		panic(item)
	}
	if x.F != nil && !x.F() {
		return nil, false
	}
	return x, true
}

func (x X) Field(name string) interface{} {
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

func BenchmarkCollection_Put(b *testing.B) {
	collection := Collection{
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
	for i := 0; i < b.N; i++ {
		cas, ok := collection.Put(X{
			ID:   uuid.UUID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)},
			Type: "audio",
			Code: i,
			Name: i,
		})
		if !ok || cas == 0 {
			b.FailNow()
		}
	}
}

func TestCollection_Put_update_with_collision(t *testing.T) {
	collection := Collection{
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
	id1 := uuid.New()
	id2 := uuid.New()
	collection.Put(X{
		ID:   id1,
		Type: "update",
		Code: 1,
		Name: 1,
	})
	collection.Put(X{
		ID:   id2,
		Type: "update",
		Code: 2,
		Name: 2,
	})
	c1 := make(chan bool)
	c2 := make(chan bool)
	go func() {
		ok := <-c2
		time.AfterFunc(time.Second/10, func() {
			c1 <- ok
		})
		collection.Put(X{
			ID:   uuid.UUID{},
			Type: "update",
			Code: 1,
			Name: 2,
		})
		c2 <- ok
	}()
	collection.Put(X{
		ID:   id1,
		Type: "update",
		Code: 2,
		Name: 1,
		F: func() bool {
			c2 <- false
			return <-c1
		},
	})
	<-c2
	for _, index := range collection.Indexes {
		t.Log(index.Field)
		index.Range(func(key, value interface{}) bool {
			t.Log(key, value)
			return true
		})
	}
}

func TestCollection_Put_insert_with_collision(t *testing.T) {
	collection := Collection{
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
	c1 := make(chan bool)
	collection.Put(X{
		ID:   uuid.New(),
		Type: "insert",
		Code: 0,
		Name: 1,
		F: func() bool {
			go func() {
				c1 <- <-c1
				collection.Put(X{
					ID:   uuid.New(),
					Type: "insert",
					Code: 0,
					Name: 2,
				})
				c1 <- true
			}()
			c1 <- false
			time.Sleep(time.Second / 10)
			return <-c1
		},
	})
	<-c1
	for _, index := range collection.Indexes {
		t.Log(index.Field)
		index.Range(func(key, value interface{}) bool {
			t.Log(key, value)
			return true
		})
	}
}

func TestCollection_Put(t *testing.T) {
	collection := Collection{
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
	type args struct {
		item Item
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
				item: &X{
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
				item: X{
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
				item: X{
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
				item: X{
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
				item: X{
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
				item: X{
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
				item: X{
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae1111"),
					Type: "audio",
					Code: 5,
					Name: 5,
				},
			},
			cas: 3,
			ok:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cas, ok := collection.Put(tt.args.item)
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
