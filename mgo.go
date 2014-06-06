package mongo

import (
	"github.com/go4r/handy"

	"errors"
	"labix.org/v2/mgo"
)

var (

	MongoNeedAuth = false
	MongoServer   = "localhost"
	MongoDBName   = "test"
	MongoUser     = ""
	MongoPass     = ""
	MongoSession  = (*mgo.Session)(nil)
	MongoDatabase = (*mgo.Database)(nil)

	_ = handy.Server.Context().SetProvider(
		"mongo.session", func(c *handy.Context) func() interface{} {
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
		}).SetProvider(
		"mongo.db", func(c *handy.Context) func() interface{} {
			session := c.Get("mongo.session").(*mgo.Session)
			if MongoDatabase == nil {
				MongoDatabase = session.DB(MongoDBName)

				if MongoNeedAuth == true {
					MongoDatabase.Login(MongoUser, MongoPass)
				}

			}
			return func() interface{} {
				return MongoDatabase
			}
		})
)

func CSession(r interface{}) (*mgo.Session) {
	return handy.CContext(r).Get("mongo.session").(*mgo.Session)
}

func CDB(r interface{}, name string) *mgo.Database {
	return handy.CContext(r).Get("mongo.session").(*mgo.Session).DB(name)
}

func CCollection(r interface{}, name string) *mgo.Collection {
	return handy.CContext(r).Get("mongo.db").(*mgo.Database).C(name)
}
