package mongo

import (
	"errors"
	"github.com/go4r/handy"
	"labix.org/v2/mgo"
	"reflect"
	"labix.org/v2/mgo/bson"
)

var (
	MongoNeedAuth = false
	MongoServer   = "localhost"
	MongoDBName   = "test"
	MongoUser     = ""
	MongoPass     = ""
	MongoSession  *mgo.Session
	MongoDB       *mgo.Database
)

func init() {
	handy.Server.Context.SetProviderMap(handy.ContextProviderMap{
		"mongo.session": func(c *handy.Context) func() interface{} {

			var err error
			if MongoSession == nil {
				MongoSession, err = mgo.Dial(MongoServer)
				if err != nil {
					panic(errors.New("Can't Connect to the mongod server!"))
				}
			}

			sessCopy := MongoSession.Copy()

			c.CleanupFunc(func() {
				sessCopy.Close()
			})

			return func() interface{} {
				return sessCopy
			}
		},
		"mongo.db": func(c *handy.Context) func() interface{} {
			session := c.Get("mongo.session").(*mgo.Session)
			if MongoDB == nil {
				MongoDB = session.DB(MongoDBName)

				if MongoNeedAuth == true {
					MongoDB.Login(MongoUser, MongoPass)
				}

			}
			return func() interface{} {
				return MongoDB
			}
		},
	})
}

func MongoDb(r interface{}, name string) *mgo.Database {
	return handy.CContext(r).Get("mongo.session").(*mgo.Session).DB(name)
}

func Collection(r interface{}, name string) *mgo.Collection {
	return handy.CContext(r).Get("mongo.db").(*mgo.Database).C(name)
}

func _collection(r interface{}, name string) (*mgo.Collection, *handy.Context) {
	c := handy.CContext(r)
	return c.Get("mongo.db").(*mgo.Database).C(name), c
}



func MongoInsert(r interface{}, nameOrElement interface{}, add ...interface{}) error {

	var (
		collection *mgo.Collection
		c *handy.Context
	)

	add = append([]interface{}{nameOrElement}, add...)

	for _, v := range add {

		switch v := v.(type){
		case string:
			collection, c = _collection(r, v)
			continue
		case MongoNamedType:
			collection, c = _collection(r, v.CollectionName())
		}

		if collection == nil {
			panic(errors.New("No Collection Especified"))
		}

		if v, ok := v.(MongoBeforeSave); ok {
			err := v.BeforeSave(c)
			if err != nil {
				return err
			}
		}

		if v, ok := v.(MongoBeforeInsert); ok {
			err := v.BeforeInsert(c)
			if err != nil {
				return err
			}
		}

		err := collection.Insert(v)

		if err != nil {
			return err
		}

		if v, ok := v.(MongoAfterInsert); ok {
			v.AfterInsert(c)
		}

		if v, ok := v.(MongoAfterSave); ok {
			v.AfterSave(c)
		}


	}

	return nil
}

func MongoChangeInfo(r interface{}) (*mgo.ChangeInfo) {
	return handy.CContext(r).Get("mongo.changeinfo").(*mgo.ChangeInfo)
}

func MongoLastId(r interface{}) interface{} {
	return MongoChangeInfo(r).UpsertedId
}

func MongoLastObjectId(r interface{}) (bson.ObjectId) {
	return MongoChangeInfo(r).UpsertedId.(bson.ObjectId)
}



func MongoUpdate(r interface{}, nameOrElement interface{}, add ...interface{}) error {

	var (
		collection *mgo.Collection
		findEr interface{}
		c *handy.Context
	)

	add = append([]interface{}{nameOrElement}, add...)

	for _, v := range add {

		switch v := v.(type){
		case string:
			collection, c = _collection(r, v)
			continue
		case MongoLoader:
			if v, ok := v.(MongoNamedType); ok {
				collection, c = _collection(r, v.CollectionName())
			}
			findEr = v.Load(c)
		case MongoNamedType:
			collection, c = _collection(r, v.CollectionName())
		case MongoQuery:
			findEr = v.Query
			continue
		case *MongoQuery:
			findEr = v.Query
			continue
		}

		if collection == nil {
			panic(errors.New("No Collection Especified"))
		}

		if findEr == nil {
			panic(errors.New("No Selector Was Especified"))
		}

		if v, ok := v.(MongoBeforeSave); ok {
			err := v.BeforeSave(c)
			if err != nil {
				return err
			}
		}

		if v, ok := v.(MongoBeforeUpdate); ok {
			err := v.BeforeUpdate(c)
			if err != nil {
				return err
			}
		}


		err := collection.Update(findEr, v)

		if err != nil {
			return err
		}

		if v, ok := v.(MongoAfterUpdate); ok {
			v.AfterUpdate(c)
		}

		if v, ok := v.(MongoAfterSave); ok {
			v.AfterSave(c)
		}


	}

	return nil
}

func MongoSave(r interface{}, nameOrElement interface{}, add ...interface{}) error {

	var (
		collection *mgo.Collection
		findEr interface{}
		c *handy.Context
	)

	add = append([]interface{}{nameOrElement}, add...)

	for _, v := range add {

		switch v := v.(type){
		case string:
			collection, c = _collection(r, v)
			continue
		case MongoLoader:
			if v, ok := v.(MongoNamedType); ok {
				collection, c = _collection(r, v.CollectionName())
			}
			findEr = v.Load(c)
		case MongoNamedType:
			collection, c = _collection(r, v.CollectionName())
		case MongoQuery:
			findEr = v.Query
			continue
		case *MongoQuery:
			findEr = v.Query
			continue
		}

		if collection == nil {
			panic(errors.New("No Collection Especified"))
		}

		if findEr == nil {
			findEr = v
		}

		if v, ok := v.(MongoBeforeSave); ok {
			err := v.BeforeSave(c)
			if err != nil {
				return err
			}
		}


		info, err := collection.Upsert(findEr, v)

		if err != nil {
			return err
		}


		c.SetValue("mongo.changeinfo", info)

		if info.Updated == 1 {
			if v, ok := v.(MongoAfterUpdate); ok {
				v.AfterUpdate(c)
			}
		}else {
			if v, ok := v.(MongoAfterInsert); ok {
				v.AfterInsert(c)
			}
		}

		if v, ok := v.(MongoAfterSave); ok {
			v.AfterSave(c)
		}

		findEr = nil
	}

	return nil
}

func MongoDelete(r interface{}, nameOrElement interface{}, add ...interface{}) error {

	var (
		c *handy.Context
		collection *mgo.Collection
		findEr interface{}
	)

	add = append([]interface{}{nameOrElement}, add...)

	for _, v := range add {

		switch v := v.(type){
		case string:
			collection, c = _collection(r, v)
			continue

		case MongoNamedType:
			collection, c = _collection(r, v.CollectionName())
			findEr = v
		case MongoQuery:
			findEr = v.Query
		case *MongoQuery:
			findEr = v.Query
		}

		if collection == nil {
			panic(errors.New("No Collection Especified"))
		}

		if findEr == nil {
			panic(errors.New("No Selector Was Especified"))
		}

		if findEr, ok := findEr.(MongoBeforeDelete); ok {
			err := findEr.BeforeDelete(c)
			if err != nil {
				return err
			}
		}

		err := collection.Remove(findEr)

		if err != nil {
			return err
		}

		if findEr, ok := findEr.(MongoAfterDelete); ok {
			findEr.AfterDelete(c)
		}

	}

	return nil
}


func MongoFind(r interface{}, nameOrElement interface{}, add ...interface{}) *MongoResult {

	var (
		c *handy.Context
		collection *mgo.Collection
		findEr interface{}
		resultSet MongoResult
	)

	add = append([]interface{}{nameOrElement}, add...)

	for _, v := range add {

		switch v := v.(type){
		case string:
			collection, c = _collection(r, v)
			continue
		case MongoLoader:
			if v, ok := v.(MongoNamedType); ok {
				collection, c = _collection(r, v.CollectionName())
			}
			findEr = v.Load(c)
		case MongoNamedType:
			collection, c = _collection(r, v.CollectionName())
		case MongoQuery:
			findEr = v.Query
			continue
		case *MongoQuery:
			findEr = v.Query
			continue
		}


		if collection == nil {
			panic(errors.New("No Collection Especified"))
		}

		value := reflect.ValueOf(v)
		if value.Kind() != reflect.Ptr {
			panic(errors.New("Invalid target element, is not a pointer"))
		}
		if value.Elem().Kind() == reflect.Slice || value.Elem().Kind() == reflect.Array {
			resultSet.executors = append(resultSet.executors, func() error {
				return executeQueryAll(collection, c, findEr, value)
			})
		}else {
			resultSet.executors = append(resultSet.executors, func() error {
				return executeQueryOne(collection, c, findEr, v)
			})
		}

	}

	return &resultSet
}

func executeQueryAll(collection *mgo.Collection, c *handy.Context, findEr interface{}, resultv reflect.Value) error {
	iter := collection.Find(findEr).Iter()

	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}
	slicev := resultv.Elem()
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()
	i := 0
	for {
		if slicev.Len() == i {
			elemp := reflect.New(elemt)
			newElement := elemp.Interface()
			if newElement, ok := newElement.(MongoBeforeFetch); ok {
				err := newElement.BeforeFetch(c)
				if err != nil {
					return err
				}
			}
			if !iter.Next(newElement) {
				break
			}
			if newElement, ok := newElement.(MongoAfterFetch); ok {
				newElement.AfterFetch(c)
			}
			slicev = reflect.Append(slicev, elemp.Elem())
			slicev = slicev.Slice(0, slicev.Cap())
		} else {
			element := slicev.Index(i).Addr().Interface()
			if element, ok := element.(MongoBeforeFetch); ok {
				err := element.BeforeFetch(c)
				if err != nil {
					return err
				}
			}
			if !iter.Next(element) {
				break
			}
			if element, ok := element.(MongoAfterFetch); ok {
				element.AfterFetch(c)
			}
		}
		i++
	}
	resultv.Elem().Set(slicev.Slice(0, i))
	return iter.Close()
}

func executeQueryOne(collection *mgo.Collection, c *handy.Context, findEr interface{}, v interface{}) error {

	if v, ok := v.(MongoBeforeFetch); ok {
		err := v.BeforeFetch(c)
		if err != nil {
			return err
		}
	}

	err := collection.Find(findEr).One(v)
	if err != nil {
		return err
	}

	if v, ok := v.(MongoAfterFetch); ok {
		v.AfterFetch(c)
	}

	return nil
}


func MongoCount(r interface{}, name string, find interface{}) int {
	value, err := Collection(r, name).Find(find).Count()
	if err != nil {
		return -1
	}
	return value
}
