package mongo

import (
	"reflect"
	"github.com/go4r/handy"
	"errors"
	"strings"
)

type repository struct {
	collection string
	typE       reflect.Type
	nilInst    interface{}
}

func NewRepository(nilInst interface{}) func(interface{}) *repositoryOperator {
	var collectionName string


	typ := reflect.TypeOf(nilInst)
	if typ.Kind() != reflect.Ptr {
		panic(errors.New("Invalid Argument the second argument shoul'd to be a pointer"))
	}
	typ = typ.Elem()

	if col, is := nilInst.(interface{ CollectionName() string; }); is {
		collectionName = col.CollectionName()
	}else {
		collectionName = strings.ToLower(typ.Name())
	}

	repo := &repository{collection:collectionName, typE:typ, nilInst:nilInst}
	return func(rc interface{}) *repositoryOperator {
		return repo.Operator(rc)
	}
}


func NewRepositoryCollection(collectionName string, nilInst interface{}) func(interface{}) *repositoryOperator {
	typ := reflect.TypeOf(nilInst)

	if typ.Kind() != reflect.Ptr {
		panic(errors.New("Invalid Argument the second argument shoul'd to be a pointer"))
	}

	repo := &repository{collection:collectionName, typE:typ.Elem(), nilInst:nilInst}
	return func(rc interface{}) *repositoryOperator {
		return repo.Operator(rc)
	}
}

func (self *repository) Operator(rc interface{}) (*repositoryOperator) {
	c := handy.CContext(rc)

	repo := c.GetFactory("mongo.repository." + self.collection)

	if repo != nil {
		return repo().(*repositoryOperator)
	}

	repository := &repositoryOperator{self, c, CCollection(rc, self.collection)}
	c.SetValue("mongo.repository."+self.collection, repository)
	return repository
}
