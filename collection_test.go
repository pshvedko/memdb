package memdb

import (
	"fmt"
	"github.com/google/uuid"
	"sync"
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

func ExampleCollection_Put() {
	c := Collection{
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

	// NEW
	x1 := X{
		ID:   uuid.New(),
		Name: 1,
		Code: 100,
	}
	fmt.Println(c.Put(x1))

	// ERROR CODE
	x2 := X{
		ID:   uuid.New(),
		Name: 2,
		Code: 100,
	}
	fmt.Println(c.Put(x2))

	// UPDATE NAME CODE
	x3 := X{
		ID:   x1.ID,
		Name: 3,
		Code: 200,
	}
	fmt.Println(c.Put(x3))

	// UPDATE NAME
	x4 := X{
		ID:   x1.ID,
		Name: 4,
		Code: 400,
	}
	fmt.Println(c.Put(x4))

	c.Indexes[0].Range(func(key, value interface{}) bool {
		fmt.Println(key, value)
		return true
	})
	c.Indexes[1].Range(func(key, value interface{}) bool {
		fmt.Println(key, value)
		return true
	})

	//Output:
	//
}
