package memdb

import (
	"fmt"
	"github.com/google/uuid"
	"sync"
	"testing"
)

type X struct {
	ID   uuid.UUID `json:"id"`
	Code int       `json:"code"`
	Name int
}

func (x X) Field(names ...string) (values []interface{}) {
	for _, name := range names {
		switch name {
		case "id":
			values = append(values, x.ID)
		case "code":
			values = append(values, x.Code)
		}
	}
	return
}

func TestCollection_Put(t *testing.T) {
	collection := Collection{
		Indexes: []Index{
			{
				Field:   []string{"id"},
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
				item: X{
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae0000"),
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
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae7777"),
					Code: 1,
					Name: 1,
				},
			},
			cas: 0,
			ok:  false,
		},
		{
			args: args{
				item: X{
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae7777"),
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
					ID:   uuid.MustParse("0a2f37be-6e18-4944-8273-9db2a0ae7777"),
					Code: 1,
					Name: 1,
				},
			},
			cas: 0,
			ok:  false,
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
		index.Range(func(key, value interface{}) bool {
			t.Log(key, value)
			return true
		})
	}
}
