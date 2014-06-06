package mongo

import (
	"github.com/go4r/handy"
	"labix.org/v2/mgo"
)

//Type Hook Interfaces

type DocumentWithPrimaryKey interface {
	PrimaryKey(c *handy.Context) interface{}
}

// Hook On|After Search
type HookOnSearch interface {
	HookOnSearch(c *handy.Context, document_selector interface{}) error
}

type HookAfterSearch interface {
	HookAfterSearch(c *handy.Context, document_selector interface{}) error
}

// Hook On|After Loading

type HookOnLoad interface {
	HookOnLoad(c *handy.Context) error
}

type HookAfterLoad interface {
	HookAfterLoad(c *handy.Context) error
}

// Hook On|After Update
type HookOnUpdate interface {
	HookOnUpdate(c *handy.Context, document_selector interface{}) error
}

type HookAfterUpdate interface {
	HookAfterUpdate(c *handy.Context, document_selector interface{}) error
}

// Hook On|After Update
type HookOnDelete interface {
	HookOnDelete(c *handy.Context, document_query interface{}) error
}

type HookAfterDelete interface {
	HookAfterDelete(c *handy.Context, document_query interface{}) error
}

// Hook On|After Insert
type HookOnInsert interface {
	HookOnInsert(c *handy.Context) error
}

type HookAfterInsert interface {
	HookAfterInsert(c *handy.Context) error
}

// Hook On|After Save
type HookOnSave interface {
	HookOnSave(c *handy.Context) error
}
type HookAfterSave interface {
	HookAfterSave(c *handy.Context, changeInfo *mgo.ChangeInfo) error
}
