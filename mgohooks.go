package mongo

import (
	"github.com/go4r/handy"
)

//Mgo Hooks

type MongoNamedType interface {
	CollectionName() string
}

//type MongoPrimaryKey interface {
//	Key() interface{}
//}
//
//type MongoChecker interface {
//	IsNew() bool
//}
//

type MongoLoader interface {
MongoNamedType
	AutoLoad(*handy.Context) interface{}
}

type MongoBeforeDelete interface {
	BeforeDelete(*handy.Context) error
}

type MongoAfterDelete interface {
	AfterDelete(*handy.Context)
}

type MongoBeforeInsert interface {
	BeforeInsert(*handy.Context) error
}

type MongoAfterInsert interface {
	AfterInsert(*handy.Context)
}

type MongoBeforeUpdate interface {
	BeforeUpdate(*handy.Context) error
}

type MongoAfterUpdate interface {
	AfterUpdate(*handy.Context)
}


type MongoBeforeSave interface {
	BeforeSave(*handy.Context) error
}

type MongoAfterSave interface {
	AfterSave(*handy.Context)
}

type MongoBeforeFetch interface {
	BeforeFetch(*handy.Context) error
}

type MongoAfterFetch interface {
	AfterFetch(*handy.Context)
}
