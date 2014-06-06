package mongo

import (
	"labix.org/v2/mgo"
	"github.com/go4r/handy"
)

type repositoryOperator struct {
	repository *repository
	context *handy.Context
	collection *mgo.Collection
}

func (self *repositoryOperator) Context() *handy.Context {
	return self.context
}


func (self *repositoryOperator) Collection() *mgo.Collection {
	return self.collection
}


func (self *repositoryOperator) Search(selector interface{}) *query {
	return &query{self, self.collection.Find(selector), 0}
}

func (self *repositoryOperator) LoadDocument(doc DocumentWithPrimaryKey) error {

	if doc, is := doc.(HookOnLoad); is {
		err := doc.HookOnLoad(self.context)
		if err != nil {
			return err
		}
	}

	err := self.Collection().Find(doc.PrimaryKey(self.context)).One(doc)

	if err == nil {

		if doc, is := doc.(HookAfterLoad); is {
			err := doc.HookAfterLoad(self.context)
			if err != nil {
				return err
			}
		}
	}

	return err
}


func (self *repositoryOperator) Insert(doc interface{}) error {

	if doc, is := doc.(HookOnInsert); is {
		err := doc.HookOnInsert(self.context)
		if err != nil {
			return err
		}

	}

	err := self.collection.Insert(doc)

	if err == nil {
		if doc, is := doc.(HookAfterInsert); is {
			err := doc.HookAfterInsert(self.context)
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (self *repositoryOperator) Update(document_selector, doc interface{}) error {

	if doc, is := doc.(HookOnUpdate); is {
		err := doc.HookOnUpdate(self.context, document_selector)
		if err != nil {
			return err
		}

	}

	err := self.collection.Update(document_selector, doc)

	if err == nil {
		if doc, is := doc.(HookAfterUpdate); is {
			err := doc.HookAfterUpdate(self.context, document_selector)
			if err != nil {
				return err
			}

		}
	}

	return err
}


func (self *repositoryOperator) SaveDocument(doc DocumentWithPrimaryKey) error {

	document_selector := doc.PrimaryKey(self.context)

	if doc, is := doc.(HookOnSave); is {
		err := doc.HookOnSave(self.context)
		if err != nil {
			return err
		}

	}

	changes, err := self.collection.Upsert(doc.PrimaryKey(self.context), doc)

	if err == nil {
		if changes.Updated != 0 {
			if doc, is := doc.(HookAfterUpdate); is {
				err := doc.HookAfterUpdate(self.context, document_selector)
				if err != nil {
					return err
				}
			}
		}else {
			if doc, is := doc.(HookAfterInsert); is {
				err := doc.HookAfterInsert(self.context)
				if err != nil {
					return err
				}
			}
		}

		if doc, is := doc.(HookAfterSave); is {
			err := doc.HookAfterSave(self.context, changes)
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (self *repositoryOperator) UpdateDocument(doc DocumentWithPrimaryKey) error {

	document_selector := doc.PrimaryKey(self.context)

	if doc, is := doc.(HookOnUpdate); is {
		err := doc.HookOnUpdate(self.context, document_selector)
		if err != nil {
			return err
		}

	}

	err := self.collection.Update(document_selector, doc)

	if err == nil {
		if doc, is := doc.(HookAfterUpdate); is {
			err := doc.HookAfterUpdate(self.context, document_selector)
			if err != nil {
				return err
			}

		}
	}

	return err
}

func (self *repositoryOperator) Delete(document_query interface{}) error {

	if doc, is := self.repository.nilInst.(HookOnDelete); is {
		err := doc.HookOnDelete(self.context, document_query)
		if err != nil {
			return err
		}
	}

	err := self.collection.Remove(document_query)

	if err == nil {
		if doc, is := self.repository.nilInst.(HookAfterDelete); is {
			err := doc.HookAfterDelete(self.context, document_query)
			if err != nil {
				return err
			}
		}
	}

	return err
}


func (self *repositoryOperator) DeleteDocument(doc DocumentWithPrimaryKey) error {
	document_selector := doc.PrimaryKey(self.context)

	if doc, is := doc.(HookOnDelete); is {
		err := doc.HookOnDelete(self.context, document_selector)
		if err != nil {
			return err
		}
	}

	err := self.collection.Remove(document_selector)

	if err == nil {
		if doc, is := doc.(HookAfterDelete); is {
			err := doc.HookAfterDelete(self.context, document_selector)
			if err != nil {
				return err
			}
		}
	}

	return err
}
