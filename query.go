package mongo

import (
	"labix.org/v2/mgo"
	"reflect"
)

type query struct {
	operator *repositoryOperator
	query *mgo.Query
	limit int
}

// Count returns the total number of documents in the result set.
func (q *query) Count() int {
	count, err := q.query.Count()
	if err != nil {
		return -1
	}
	return count
}

// Distinct returns a list of distinct values for the given key within
// the result set.  The list of distinct values will be unmarshalled
// in the "values" key of the provided result parameter.
//
// For example:
//
//     var result []int
//     err := collection.Find(bson.M{"gender": "F"}).Distinct("age", &result)
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Aggregation
//
func (q *query) Distinct(key string, result interface{}) error {
	return q.query.Distinct(key, result)
}

// MapReduce executes a map/reduce job for documents covered by the query.
// That kind of job is suitable for very flexible bulk aggregation of data
// performed at the server side via Javascript functions.
//
// Results from the job may be returned as a result of the query itself
// through the result parameter in case they'll certainly fit in memory
// and in a single document.  If there's the possibility that the amount
// of data might be too large, results must be stored back in an alternative
// collection or even a separate database, by setting the Out field of the
// provided MapReduce job.  In that case, provide nil as the result parameter.
//
// These are some of the ways to set Out:
//
//     nil
//         Inline results into the result parameter.
//
//     bson.M{"replace": "mycollection"}
//         The output will be inserted into a collection which replaces any
//         existing collection with the same name.
//
//     bson.M{"merge": "mycollection"}
//         This option will merge new data into the old output collection. In
//         other words, if the same key exists in both the result set and the
//         old collection, the new key will overwrite the old one.
//
//     bson.M{"reduce": "mycollection"}
//         If documents exist for a given key in the result set and in the old
//         collection, then a reduce operation (using the specified reduce
//         function) will be performed on the two values and the result will be
//         written to the output collection. If a finalize function was
//         provided, this will be run after the reduce as well.
//
//     bson.M{...., "db": "mydb"}
//         Any of the above options can have the "db" key included for doing
//         the respective action in a separate database.
//
// The following is a trivial example which will count the number of
// occurrences of a field named n on each document in a collection, and
// will return results inline:
//
//     job := &mgo.MapReduce{
//             Map:      "function() { emit(this.n, 1) }",
//             Reduce:   "function(key, values) { return Array.sum(values) }",
//     }
//     var result []struct { Id int "_id"; Value int }
//     _, err := collection.Find(nil).MapReduce(job, &result)
//     if err != nil {
//         return err
//     }
//     for _, item := range result {
//         fmt.Println(item.Value)
//     }
//
// This function is compatible with MongoDB 1.7.4+.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/MapReduce
//
func (q *query) MapReduce(job *mgo.MapReduce, result interface{}) (info *mgo.MapReduceInfo, err error) {
	return q.query.MapReduce(job, result)
}

// Apply runs the findAndModify MongoDB command, which allows updating, upserting
// or removing a document matching a query and atomically returning either the old
// version (the default) or the new version of the document (when ReturnNew is true).
// If no objects are found Apply returns ErrNotFound.
//
// The Sort and Select query methods affect the result of Apply.  In case
// multiple documents match the query, Sort enables selecting which document to
// act upon by ordering it first.  Select enables retrieving only a selection
// of fields of the new or old document.
//
// This simple example increments a counter and prints its new value:
//
//     change := mgo.Change{
//             Update: bson.M{"$inc": bson.M{"n": 1}},
//             ReturnNew: true,
//     }
//     info, err = col.Find(M{"_id": id}).Apply(change, &doc)
//     fmt.Println(doc.N)
//
// This method depends on MongoDB >= 2.0 to work properly.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/findAndModify+Command
//     http://www.mongodb.org/display/DOCS/Updating
//     http://www.mongodb.org/display/DOCS/Atomic+Operations
//
func (q *query) Apply(change mgo.Change, result interface{}) (info *mgo.ChangeInfo, err error) {
	return q.query.Apply(change, result)
}

// Batch sets the batch size used when fetching documents from the database.
// It's possible to change this setting on a per-session basis as well, using
// the Batch method of Session.
//
// The default batch size is defined by the database itself.  As of this
// writing, MongoDB will use an initial size of min(100 docs, 4MB) on the
// first batch, and 4MB on remaining ones.
func (q *query) Batch(n int) *query {
	q.query.Batch(n)
	return q
}

// Prefetch sets the point at which the next batch of results will be requested.
// When there are p*batch_size remaining documents cached in an Iter, the next
// batch will be requested in background. For instance, when using this:
//
//     query.Batch(200).Prefetch(0.25)
//
// and there are only 50 documents cached in the Iter to be processed, the
// next batch of 200 will be requested. It's possible to change this setting on
// a per-session basis as well, using the SetPrefetch method of Session.
//
// The default prefetch value is 0.25.
func (q *query) Prefetch(p float64) *query {
	q.query.Prefetch(p)
	return q
}

// Skip skips over the n initial documents from the query results.  Note that
// this only makes sense with capped collections where documents are naturally
// ordered by insertion time, or with sorted results.
func (q *query) Skip(n int) *query {
	q.query.Skip(n)
	return q
}

// Limit restricts the maximum number of documents retrieved to n, and also
// changes the batch size to the same value.  Once n documents have been
// returned by Next, the following call will return ErrNotFound.
func (q *query) Limit(n int) *query {
	q.limit = n
	q.query.Limit(n)
	return q
}

// Select enables selecting which fields should be retrieved for the results
// found. For example, the following query would only retrieve the name field:
//
//     err := collection.Find(nil).Select(bson.M{"name": 1}).One(&result)
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Retrieving+a+Subset+of+Fields
//
func (q *query) Select(selector interface{}) *query {
	q.query.Select(selector)
	return q
}

// Sort asks the database to order returned documents according to the
// provided field names. A field name may be prefixed by - (minus) for
// it to be sorted in reverse order.
//
// For example:
//
//     query1 := collection.Find(nil).Sort("firstname", "lastname")
//     query2 := collection.Find(nil).Sort("-age")
//     query3 := collection.Find(nil).Sort("$natural")
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Sorting+and+Natural+Order
//
func (q *query) Sort(fields ...string) *query {
	q.query.Sort(fields...)
	return q
}

// Explain returns a number of details about how the MongoDB server would
// execute the requested query, such as the number of objects examined,
// the number of time the read lock was yielded to allow writes to go in,
// and so on.
//
// For example:
//
//     m := bson.M{}
//     err := collection.Find(bson.M{"filename": name}).Explain(m)
//     if err == nil {
//         fmt.Printf("Explain: %#v\n", m)
//     }
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Optimization
//     http://www.mongodb.org/display/DOCS/Query+Optimizer
//
func (q *query) Explain(result interface{}) error {
	return q.query.Explain(result)
}

// Hint will include an explicit "hint" in the query to force the server
// to use a specified index, potentially improving performance in some
// situations.  The provided parameters are the fields that compose the
// key of the index to be used.  For details on how the indexKey may be
// built, see the EnsureIndex method.
//
// For example:
//
//     query := collection.Find(bson.M{"firstname": "Joe", "lastname": "Winter"})
//     query.Hint("lastname", "firstname")
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Optimization
//     http://www.mongodb.org/display/DOCS/Query+Optimizer
//
func (q *query) Hint(indexKey ...string) *query {
	q.query.Hint(indexKey...)
	return q
}

// Snapshot will force the performed query to make use of an available
// index on the _id field to prevent the same document from being returned
// more than once in a single iteration. This might happen without this
// setting in situations when the document changes in size and thus has to
// be moved while the iteration is running.
//
// Because snapshot mode traverses the _id index, it may not be used with
// sorting or explicit hints. It also cannot use any other index for the
// query.
//
// Even with snapshot mode, items inserted or deleted during the query may
// or may not be returned; that is, this mode is not a true point-in-time
// snapshot.
//
// The same effect of Snapshot may be obtained by using any unique index on
// field(s) that will not be modified (best to use Hint explicitly too).
// A non-unique index (such as creation time) may be made unique by
// appending _id to the index when creating it.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/How+to+do+Snapshotted+Queries+in+the+Mongo+Database
//
func (q *query) Snapshot() *query {
	q.query.Snapshot()
	return q
}

// LogReplay enables an option that optimizes queries that are typically
// made on the MongoDB oplog for replaying it. This is an internal
// implementation aspect and most likely uninteresting for other uses.
// It has seen at least one use case, though, so it's exposed via the API.
func (q *query) LogReplay() *query {
	q.query.LogReplay()
	return q
}

func (self *query) MGOQuery() *mgo.Query {
	return self.query
}

func (self *query) One(target interface{}) error {

	if target, ok := target.(HookOnLoad); ok {
		err := target.HookOnLoad(self.operator.context)
		if err != nil {
			return err
		}
	}

	err := self.query.One(target)

	if err != nil {
		return err
	}

	if target, ok := target.(HookAfterLoad); ok {
		target.HookAfterLoad(self.operator.context)
	}

	return nil
}


func (self *query) All(target interface{}) error {
	iter := self.query.Iter()
	defer func() {
		//Make sure to close the iterator
		if err := recover(); err != nil {
			iter.Close()
		}
	}()

	resultv := reflect.ValueOf(target)

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

			if newElement, ok := newElement.(HookOnLoad); ok {
				err := newElement.HookOnLoad(self.operator.context)
				if err != nil {
					return err
				}
			}

			if !iter.Next(newElement) {
				break
			}

			if newElement, ok := newElement.(HookAfterLoad); ok {
				newElement.HookAfterLoad(self.operator.context)
			}

			slicev = reflect.Append(slicev, elemp.Elem())
			slicev = slicev.Slice(0, slicev.Cap())

		} else {
			element := slicev.Index(i).Addr().Interface()

			if element, ok := element.(HookOnLoad); ok {
				err := element.HookOnLoad(self.operator.context)
				if err != nil {
					return err
				}
			}

			if !iter.Next(element) {
				break
			}

			if element, ok := element.(HookAfterLoad); ok {
				element.HookAfterLoad(self.operator.context)
			}
		}
		i++
	}

	return iter.Close()
}

func (self *query) GetOne() interface{} {
	m := reflect.New(self.operator.repository.typE).Interface()
	err := self.One(m)
	if err != nil {
		return nil
	}
	return m
}



func (self *query) GetAll() interface{} {
	iter := self.query.Iter()
	defer iter.Close()

	slicev := reflect.MakeSlice(reflect.SliceOf(self.operator.repository.typE), self.limit, self.limit)
	elemt := slicev.Type().Elem()
	i := 0

	for {
		if slicev.Len() == i {
			elemp := reflect.New(elemt)
			newElement := elemp.Interface()

			if newElement, ok := newElement.(HookOnLoad); ok {
				err := newElement.HookOnLoad(self.operator.context)
				if err != nil {
					return nil
				}
			}

			if !iter.Next(newElement) {
				break
			}

			if newElement, ok := newElement.(HookAfterLoad); ok {
				newElement.HookAfterLoad(self.operator.context)
			}

			slicev = reflect.Append(slicev, elemp.Elem())
			slicev = slicev.Slice(0, slicev.Cap())

		} else {
			element := slicev.Index(i).Addr().Interface()

			if element, ok := element.(HookOnLoad); ok {
				err := element.HookOnLoad(self.operator.context)
				if err != nil {
					return nil
				}
			}

			if !iter.Next(element) {
				break
			}

			if element, ok := element.(HookAfterLoad); ok {
				element.HookAfterLoad(self.operator.context)
			}
		}
		i++
	}

	return slicev.Interface()
}
