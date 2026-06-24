// Copyright © 2023 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbsql

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Masterminds/squirrel"
	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestCRUDable struct {
	ResourceBase
	NS     *string             `json:"namespace"`
	Name   *string             `json:"name"`
	Field1 *string             `json:"f1"`
	Field2 *fftypes.FFBigInt   `json:"f2"`
	Field3 *fftypes.JSONAny    `json:"f3"`
	Field4 *int64              `json:"f4"`
	Field5 *bool               `json:"f5"`
	Field6 *fftypes.FFDuration `json:"f6"`
}

var CRUDableQueryFactory = &ffapi.QueryFields{
	"ns":      &ffapi.StringField{},
	"name":    &ffapi.StringField{},
	"id":      &ffapi.UUIDField{},
	"created": &ffapi.TimeField{},
	"updated": &ffapi.TimeField{},
	"f1":      &ffapi.StringField{},
	"f2":      &ffapi.BigIntField{},
	"f3":      &ffapi.JSONField{},
	"f4":      &ffapi.Int64Field{},
	"f5":      &ffapi.BoolField{},
	"f6":      &ffapi.Int64Field{},
}

// TestHistory shows a simple object:
// - without pointer fields (so no patch support)
// - with just an insert time, not an updated field
// - with a simple string ID
// - without namespacing
type TestHistory struct {
	ID         string          `json:"id"`
	Time       *fftypes.FFTime `json:"time"`
	SequenceID string          `json:"sequenceId"`
	Subject    string          `json:"subject"`
	Info       string          `json:"info"`
}

var HistoryQueryFactory = &ffapi.QueryFields{
	"id":      &ffapi.UUIDField{},
	"time":    &ffapi.TimeField{},
	"subject": &ffapi.StringField{},
	"info":    &ffapi.StringField{},
}

func (h *TestHistory) GetID() string {
	return h.ID
}

func (h *TestHistory) SetCreated(t *fftypes.FFTime) {
	h.Time = t
}

func (h *TestHistory) SetUpdated(t *fftypes.FFTime) {
	panic("should not be called when marked NoUpdateColumn")
}

func (h *TestHistory) SetSequence(i int64) {
	h.SequenceID = fmt.Sprintf("%.9d", i)
}

type TestLinkable struct {
	ResourceBase
	NS          string           `json:"namespace"`
	CrudID      *fftypes.UUID    `json:"crudId"`
	Description string           `json:"description"`
	Field1      string           `json:"f1"`
	Field2      fftypes.FFBigInt `json:"f2"`
	Field3      *fftypes.JSONAny `json:"f3"`
}

var LinkableQueryFactory = &ffapi.QueryFields{
	"id":      &ffapi.UUIDField{},
	"created": &ffapi.TimeField{},
	"updated": &ffapi.TimeField{},
	"crud":    &ffapi.UUIDField{},
}

type TestCRUD struct {
	*CrudBase[*TestCRUDable]
	events      []ChangeEventType
	postCommit  func()
	postCommits int
}

type TestHistoryCRUD struct {
	*CrudBase[*TestHistory]
	events      []ChangeEventType
	postCommit  func()
	postCommits int
}

type testSQLiteProvider struct {
	db     *Database
	t      *testing.T
	config config.Section
}

// newTestProvider creates a real in-memory database provider for e2e testing
func newSQLiteTestProvider(t *testing.T) (*testSQLiteProvider, func()) {
	conf := config.RootSection("unittest.db")
	conf.AddKnownKey("url", "test")

	InitSQLiteConfig(conf)

	dir, err := os.MkdirTemp("", "")
	assert.NoError(t, err)
	conf.Set(SQLConfDatasourceURL, "file::memory:")
	conf.Set(SQLConfMigrationsAuto, true)
	conf.Set(SQLConfMigrationsDirectory, "../../test/dbmigrations")
	conf.Set(SQLConfMaxConnections, 1)

	sql, err := NewSQLiteProvider(context.Background(), conf)
	assert.NoError(t, err)
	tp := &testSQLiteProvider{
		db:     sql,
		t:      t,
		config: conf,
	}
	assert.Equal(t, "sqlite3", sql.provider.(*sqLiteProvider).Name())

	return tp, func() {
		sql.Close()
		_ = os.RemoveAll(dir)
	}
}

func newCRUDCollection(db *Database, ns string) *TestCRUD {
	tc := &TestCRUD{
		CrudBase: &CrudBase[*TestCRUDable]{
			DB:    db,
			Table: "crudables",
			Columns: []string{
				ColumnID,
				ColumnCreated,
				ColumnUpdated,
				"ns",
				"name",
				"field1",
				"field2",
				"field3",
				"field4",
				"field5",
				"field6",
			},
			FilterFieldMap: map[string]string{
				"f1": "field1",
				"f2": "field2",
				"f3": "field3",
			},
			NilValue:     func() *TestCRUDable { return nil },
			NewInstance:  func() *TestCRUDable { return &TestCRUDable{} },
			ScopedFilter: func() squirrel.Eq { return sq.Eq{"ns": ns} },
			EventHandler: nil,  // set below
			IDField:      "id", // same as default
			NameField:    "name",
			QueryFactory: CRUDableQueryFactory,
			IDValidator:  UUIDValidator,
			GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
				switch col {
				case ColumnID:
					return &inst.ID
				case ColumnCreated:
					return &inst.Created
				case ColumnUpdated:
					return &inst.Updated
				case "ns":
					return &inst.NS
				case "name":
					return &inst.Name
				case "field1":
					return &inst.Field1
				case "field2":
					return &inst.Field2
				case "field3":
					return &inst.Field3
				case "field4":
					return &inst.Field4
				case "field5":
					return &inst.Field5
				case "field6":
					return &inst.Field6
				}
				return nil
			},
		},
	}
	tc.CrudBase.EventHandler = func(id string, eventType ChangeEventType) {
		tc.events = append(tc.events, eventType)
	}
	tc.postCommit = func() { tc.postCommits++ }
	return tc
}

func newHistoryCollection(db *Database) *TestHistoryCRUD {
	tc := &TestHistoryCRUD{
		CrudBase: &CrudBase[*TestHistory]{
			DB:    db,
			Table: "history",
			Columns: []string{
				ColumnID,
				ColumnCreated,
				"subject",
				"info",
			},
			FilterFieldMap: map[string]string{
				"time": ColumnCreated,
			},
			TimesDisabled: true,
			PatchDisabled: true,
			NilValue:      func() *TestHistory { return nil },
			NewInstance:   func() *TestHistory { return &TestHistory{} },
			ScopedFilter:  nil, // no scoping on this collection
			EventHandler:  nil, // set below
			GetFieldPtr: func(inst *TestHistory, col string) interface{} {
				switch col {
				case ColumnID:
					return &inst.ID
				case ColumnCreated:
					return &inst.Time
				case "subject":
					return &inst.Subject
				case "info":
					return &inst.Info
				}
				return nil
			},
		},
	}
	tc.CrudBase.EventHandler = func(id string, eventType ChangeEventType) {
		tc.events = append(tc.events, eventType)
	}
	tc.postCommit = func() { tc.postCommits++ }
	return tc
}

func newLinkableCollection(db *Database, ns string) *CrudBase[*TestLinkable] {
	tc := &CrudBase[*TestLinkable]{
		DB:    db,
		Table: "linkables",
		Columns: []string{
			ColumnID,
			ColumnCreated,
			ColumnUpdated,
			"ns",
			"desc",
			"crud_id",
		},
		ReadTableAlias: "l",
		ReadOnlyColumns: []string{
			"c.field1",
			"c.field2",
			"c.field3",
		},
		FilterFieldMap: map[string]string{
			"description": "desc",
			"crud":        "crud_id",
		},
		ReadQueryModifier: func(query sq.SelectBuilder) (sq.SelectBuilder, error) {
			return query.LeftJoin("crudables AS c ON c.id = l.crud_id"), nil
		},
		DefaultSort: func() []interface{} {
			// Return an empty list
			return []interface{}{}
		},
		NilValue:     func() *TestLinkable { return nil },
		NewInstance:  func() *TestLinkable { return &TestLinkable{} },
		ScopedFilter: func() squirrel.Eq { return sq.Eq{"l.ns": ns} },
		EventHandler: nil, // set below
		GetFieldPtr: func(inst *TestLinkable, col string) interface{} {
			switch col {
			case ColumnID:
				return &inst.ID
			case ColumnCreated:
				return &inst.Created
			case ColumnUpdated:
				return &inst.Updated
			case "ns":
				return &inst.NS
			case "desc":
				return &inst.Description
			case "crud_id":
				return &inst.CrudID
			case "c.field1":
				return &inst.Field1
			case "c.field2":
				return &inst.Field2
			case "c.field3":
				return &inst.Field3
			}
			panic(fmt.Sprintf("unknown column: '%s'", col))
		},
	}
	return tc
}

func checkEqualExceptTimes(t *testing.T, o1, o2 TestCRUDable) {
	o1.Updated = nil
	o1.Created = nil
	j1, err := json.Marshal(o1)
	assert.NoError(t, err)
	o2.Updated = nil
	o2.Created = nil
	j2, err := json.Marshal(o2)
	assert.NoError(t, err)
	assert.JSONEq(t, string(j1), string(j2))
}

func ptrTo[T any](v T) *T {
	return &v
}

func TestCRUDWithDBEnd2End(t *testing.T) {
	log.SetLevel("trace")

	sql, done := newSQLiteTestProvider(t)
	defer done()
	ctx := context.Background()

	c1 := &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
		Name:   ptrTo("bob"),
		NS:     ptrTo("ns1"),
		Field1: ptrTo("hello1"),
		Field2: fftypes.NewFFBigInt(12345),
		Field3: fftypes.JSONAnyPtr(`{"some":"stuff"}`),
		Field4: ptrTo(int64(12345)),
		Field5: ptrTo(true),
	}

	collection := newCRUDCollection(sql.db, "ns1")
	var iCrud CRUD[*TestCRUDable] = collection.CrudBase
	iCrud.Validate()

	// Add a row
	err := iCrud.Insert(ctx, c1, collection.postCommit)
	assert.NoError(t, err)
	assert.Len(t, collection.events, 1)
	assert.Equal(t, Created, collection.events[0])
	collection.events = nil

	// Install an AfterLoad handler
	afterLoadCalled := false
	collection.AfterLoad = func(ctx context.Context, inst *TestCRUDable) error {
		afterLoadCalled = true
		return nil
	}

	// Check we get it back
	c1copy, err := iCrud.GetByID(ctx, c1.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1, *c1copy)
	assert.True(t, afterLoadCalled)
	collection.AfterLoad = nil

	// Check we get it back by name
	c1copy, err = iCrud.GetByName(ctx, *c1.Name)
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1, *c1copy)

	// Check we don't get it back by name if we scope to a different ns
	c1NotFound, err := iCrud.Scoped(sq.Eq{"ns": "ns2"}).GetByName(ctx, *c1.Name)
	assert.NoError(t, err)
	assert.Nil(t, c1NotFound)

	// Check we get it back by name, in name or UUID
	c1copy, err = iCrud.GetByUUIDOrName(ctx, *c1.Name)
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1, *c1copy)

	// Check we get it back by UUID, in name or UUID
	c1copy, err = iCrud.GetByUUIDOrName(ctx, c1.GetID())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1, *c1copy)

	// Check we get it back with custom modifiers
	collection.ReadQueryModifier = func(sb sq.SelectBuilder) (sq.SelectBuilder, error) {
		return sb.Where(sq.Eq{"ns": "ns1"}), nil
	}
	c1copy, err = iCrud.ModifyQuery(func(sb sq.SelectBuilder) (sq.SelectBuilder, error) {
		return sb.Where(sq.Eq{"field1": "hello1"}), nil
	}).GetByName(ctx, *c1.Name)
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1, *c1copy)

	// Check AfterLoad error behavior
	collection.AfterLoad = func(ctx context.Context, inst *TestCRUDable) error {
		return fmt.Errorf("pop")
	}
	_, _, err = iCrud.GetMany(ctx, CRUDableQueryFactory.NewFilter(ctx).And())
	assert.EqualError(t, err, "pop")
	collection.AfterLoad = nil

	// Upsert the existing row optimized
	c1copy.Field1 = ptrTo("hello again - 1")
	created, err := iCrud.Upsert(ctx, c1copy, UpsertOptimizationExisting)
	assert.NoError(t, err)
	assert.False(t, created)
	c1copy1, err := iCrud.GetByID(ctx, c1.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1copy, *c1copy1)
	assert.Len(t, collection.events, 1)
	assert.Equal(t, Updated, collection.events[0])
	collection.events = nil

	// Upsert the existing row un-optimized
	c1copy.Field1 = ptrTo("hello again - 2")
	created, err = iCrud.Upsert(ctx, c1copy, UpsertOptimizationNew, collection.postCommit)
	assert.NoError(t, err)
	assert.False(t, created)
	c1copy2, err := iCrud.GetByID(ctx, c1.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1copy, *c1copy2)

	// Explicitly replace it
	c1copy.Field1 = ptrTo("hello again - 3")
	err = iCrud.Replace(ctx, c1copy, collection.postCommit)
	assert.NoError(t, err)
	c1copy3, err := iCrud.GetByID(ctx, c1.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1copy, *c1copy3)

	// Explicitly update it
	c1copy.Field1 = ptrTo("hello again - 4")
	err = iCrud.Update(ctx, c1copy.ID.String(), CRUDableQueryFactory.NewUpdate(ctx).Set(
		"f1", *c1copy.Field1,
	), collection.postCommit)
	assert.NoError(t, err)
	c1copy4, err := iCrud.GetByID(ctx, c1.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1copy, *c1copy4)

	// Use simple PATCH semantics to updated it
	c1copy.Field1 = ptrTo("hello again - 5")
	sparseUpdate := &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: c1copy.ID,
		},
		Field1: ptrTo("hello again - 5"),
	}
	err = iCrud.UpdateSparse(ctx, sparseUpdate, collection.postCommit)
	assert.NoError(t, err)
	c1copy5, err := iCrud.GetByID(ctx, c1.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *c1copy, *c1copy5)

	// Cannot replace something that doesn't exist
	c2 := *c1
	c2.ID = fftypes.NewUUID()
	c2.NS = ptrTo("ns1")
	c2.Field1 = ptrTo("bonjour")
	err = iCrud.Replace(ctx, &c2, collection.postCommit)
	assert.Regexp(t, "FF00205", err)

	// Optimized insert of another
	created, err = iCrud.Upsert(ctx, &c2, UpsertOptimizationNew)
	assert.NoError(t, err)
	assert.True(t, created)
	c2copy1, err := iCrud.GetByID(ctx, c2.ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, c2, *c2copy1)

	// Check we can filter it with the new value
	cs, _, err := iCrud.GetMany(ctx, CRUDableQueryFactory.NewFilter(ctx).Eq(
		"f1", "bonjour",
	))
	assert.NoError(t, err)
	assert.Len(t, cs, 1)
	checkEqualExceptTimes(t, c2, *cs[0])

	// Check we can grab just a couple of fields
	cs, _, err = iCrud.GetMany(ctx, CRUDableQueryFactory.NewFilter(ctx).Eq(
		"f1", "bonjour",
	).RequiredFields("ns", "f2", "f3"))
	assert.NoError(t, err)
	assert.Len(t, cs, 1)
	assert.Empty(t, cs[0].ID)   // yes - we didn't even request the ID! (feature needed for group-by use cases)
	assert.Nil(t, cs[0].Field1) // yes - we didn't request this, even though we filtered on it
	assert.Equal(t, c2.Field2, cs[0].Field2)
	assert.Equal(t, c2.Field3, cs[0].Field3)
	assert.Equal(t, *c2.NS, *cs[0].NS)

	// Insert a bunch in a batch
	bunchOfCRUDables := make([]*TestCRUDable, 10)
	for i := range bunchOfCRUDables {
		bunchOfCRUDables[i] = &TestCRUDable{
			ResourceBase: ResourceBase{
				ID: fftypes.NewUUID(),
			},
			NS:     ptrTo("ns1"),
			Field1: ptrTo(fmt.Sprintf("crudable[%.5d]", i)),
			Field2: fftypes.NewFFBigInt(919191),
		}
	}
	err = iCrud.InsertMany(ctx, bunchOfCRUDables, false, collection.postCommit)
	assert.NoError(t, err)

	// Grab one
	bunch5copy, err := iCrud.GetByID(ctx, bunchOfCRUDables[4].ID.String())
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *bunchOfCRUDables[4], *bunch5copy)

	// Update them all
	err = iCrud.UpdateMany(ctx, CRUDableQueryFactory.NewFilter(ctx).Eq(
		"f2", "919191",
	), CRUDableQueryFactory.NewUpdate(ctx).Set(
		"f2", "929292",
	), collection.postCommit)
	assert.NoError(t, err)
	checkEqualExceptTimes(t, *bunchOfCRUDables[4], *bunch5copy)

	for i := range bunchOfCRUDables {
		ci, err := iCrud.GetByID(ctx, bunchOfCRUDables[i].ID.String())
		assert.NoError(t, err)
		assert.Equal(t, int64(929292), ci.Field2.Int64())
	}

	// Delete it
	err = iCrud.Delete(ctx, bunchOfCRUDables[4].ID.String(), collection.postCommit)
	assert.NoError(t, err)

	// Check it's gone
	goneOne, err := iCrud.GetByID(ctx, bunchOfCRUDables[4].ID.String())
	assert.NoError(t, err)
	assert.Nil(t, goneOne)

	// Check all the post commits above fired
	assert.Equal(t, 8, collection.postCommits)

}

func TestCRUDScoped(t *testing.T) {
	log.SetLevel("trace")

	sql, done := newSQLiteTestProvider(t)
	defer done()
	ctx := context.Background()

	collection := newCRUDCollection(sql.db, "ns1")
	scopedCollection := collection.Scoped(sq.Eq{"field1": "scope1"})

	bobScope1 := &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
		Name:   ptrTo("bob"),
		NS:     ptrTo("ns1"),
		Field1: ptrTo("scope1"),
		Field2: fftypes.NewFFBigInt(12345),
		Field3: fftypes.JSONAnyPtr(`{"some":"stuff"}`),
		Field4: ptrTo(int64(12345)),
		Field5: ptrTo(true),
	}
	err := collection.Insert(ctx, bobScope1, collection.postCommit)
	assert.NoError(t, err)

	bobScope2 := &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
		Name:   ptrTo("bob"),
		NS:     ptrTo("ns1"),
		Field1: ptrTo("scope2"),
		Field2: fftypes.NewFFBigInt(12345),
		Field3: fftypes.JSONAnyPtr(`{"some":"stuff"}`),
		Field4: ptrTo(int64(12345)),
		Field5: ptrTo(true),
	}
	err = collection.Insert(ctx, bobScope2, collection.postCommit)
	assert.NoError(t, err)

	aliceScope1 := &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
		Name:   ptrTo("bob"),
		NS:     ptrTo("ns1"),
		Field1: ptrTo("scope1"),
		Field2: fftypes.NewFFBigInt(12345),
		Field3: fftypes.JSONAnyPtr(`{"some":"stuff"}`),
		Field4: ptrTo(int64(12345)),
		Field5: ptrTo(true),
	}
	err = collection.Insert(ctx, aliceScope1, collection.postCommit)
	assert.NoError(t, err)

	aliceScope2 := &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
		Name:   ptrTo("alice"),
		NS:     ptrTo("ns1"),
		Field1: ptrTo("scope2"),
		Field2: fftypes.NewFFBigInt(12345),
		Field3: fftypes.JSONAnyPtr(`{"some":"stuff"}`),
		Field4: ptrTo(int64(12345)),
		Field5: ptrTo(true),
	}
	err = collection.Insert(ctx, aliceScope2, collection.postCommit)
	assert.NoError(t, err)

	//Get many only returns scope1 rows
	cs, _, err := scopedCollection.GetMany(ctx, CRUDableQueryFactory.NewFilter(ctx).And())
	assert.NoError(t, err)
	assert.Len(t, cs, 2)
	assert.Equal(t, bobScope1.ID.String(), cs[1].ID.String())
	assert.Equal(t, aliceScope1.ID.String(), cs[0].ID.String())

	//Get many on the non scoped collection only returns all rows
	cs2, _, err := collection.GetMany(ctx, CRUDableQueryFactory.NewFilter(ctx).And())
	assert.NoError(t, err)
	assert.Len(t, cs2, 4)

	//Get by id fails if the id is not in the scoped collection
	_, err = scopedCollection.GetByID(ctx, aliceScope2.ID.String(), FailIfNotFound)
	assert.Regexp(t, "FF00164", err)

	//Get by id succeeds if the id is in the scoped collection
	cs1, err := scopedCollection.GetByID(ctx, aliceScope1.ID.String(), FailIfNotFound)
	assert.NoError(t, err)
	assert.Equal(t, aliceScope1.ID.String(), cs1.ID.String())

	//Get many still returns all scope1 rows
	cs, _, err = scopedCollection.GetMany(ctx, CRUDableQueryFactory.NewFilter(ctx).And())
	assert.NoError(t, err)
	require.Len(t, cs, 2)
	assert.Equal(t, bobScope1.ID.String(), cs[1].ID.String())
	assert.Equal(t, aliceScope1.ID.String(), cs[0].ID.String())

}

func TestHistoryExampleNoNSOrUpdateColumn(t *testing.T) {
	log.SetLevel("trace")

	sql, done := newSQLiteTestProvider(t)
	defer done()
	ctx := context.Background()

	var sub1Entries []*TestHistory
	var sub2Entries []*TestHistory
	for i := 0; i < 10; i++ {
		now := fftypes.Now() // choose to manage times ourself
		sub1Entries = append(sub1Entries, &TestHistory{
			Time:    now,
			ID:      fftypes.NewUUID().String(),
			Subject: "sub1",
			Info:    fmt.Sprintf("sub1_info%.3d", i),
		})
		sub2Entries = append(sub2Entries, &TestHistory{
			Time:    now,
			ID:      fftypes.NewUUID().String(),
			Subject: "sub2",
			Info:    fmt.Sprintf("sub2_info%.3d", i),
		})
	}

	collection := newHistoryCollection(sql.db)
	var iCrud CRUD[*TestHistory] = collection.CrudBase
	iCrud.Validate()

	// add sub1 entries 1 by 1
	for _, e := range sub1Entries {
		err := iCrud.Insert(ctx, e, collection.postCommit)
		assert.NoError(t, err)
		assert.NotEmpty(t, e.SequenceID)
	}
	// add sub2 entries all at once
	err := iCrud.InsertMany(ctx, sub2Entries, false)
	assert.NoError(t, err)
	for _, e := range sub2Entries {
		assert.NotEmpty(t, e.SequenceID)
	}

	// Check we get one back from each
	s1e0, err := iCrud.GetByID(ctx, sub1Entries[0].ID)
	assert.NoError(t, err)
	assert.Equal(t, sub1Entries[0].Info, s1e0.Info)
	s2e0, err := iCrud.GetByID(ctx, sub2Entries[0].ID)
	assert.NoError(t, err)
	assert.Equal(t, sub2Entries[0].Info, s2e0.Info)

	// Check we can get the sequence and it lines up
	s2e0Seq, err := iCrud.GetSequenceForID(ctx, s2e0.ID)
	assert.NoError(t, err)
	assert.Equal(t, s2e0.SequenceID, fmt.Sprintf("%.9d", s2e0Seq))

	// Update sparse (patch) not supported, as we don't have pointer fields
	err = iCrud.UpdateSparse(ctx, s2e0)
	assert.Regexp(t, "FF00213", err)

	// Replace one
	s2e0.Info = "new data"
	err = iCrud.Replace(ctx, s2e0, collection.postCommit)
	assert.NoError(t, err)
	s2e0, err = iCrud.GetByID(ctx, s2e0.ID)
	assert.NoError(t, err)
	assert.Equal(t, "new data", s2e0.Info)

	// Get all the entries for sub1
	allEntries, _, err := iCrud.GetMany(ctx, HistoryQueryFactory.NewFilter(ctx).Eq("subject", "sub1"))
	assert.NoError(t, err)
	assert.Len(t, allEntries, len(sub1Entries))
	for _, e := range allEntries {
		assert.Equal(t, "sub1", e.Subject)
	}

	// Count all the sub1 entries
	count, err := iCrud.Count(ctx, HistoryQueryFactory.NewFilter(ctx).Eq("subject", "sub1"))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(sub1Entries)), count)

	// Delete all the sub1 entries
	postDeleteMany := make(chan struct{})
	err = iCrud.DeleteMany(ctx, HistoryQueryFactory.NewFilter(ctx).Eq("subject", "sub1"), func() {
		close(postDeleteMany)
	})
	assert.NoError(t, err)
	<-postDeleteMany

	// Check they are all gone
	allEntries, _, err = iCrud.GetMany(ctx, HistoryQueryFactory.NewFilter(ctx).And())
	assert.NoError(t, err)
	assert.Len(t, allEntries, len(sub2Entries))
	for _, e := range allEntries {
		assert.Equal(t, "sub2", e.Subject)
	}

}

func TestLeftJOINExample(t *testing.T) {
	log.SetLevel("trace")

	sql, done := newSQLiteTestProvider(t)
	defer done()
	ctx := context.Background()

	crudables := newCRUDCollection(sql.db, "ns1")
	assert.Equal(t, "crudables", crudables.TableAlias())
	linkables := newLinkableCollection(sql.db, "ns1")
	assert.Equal(t, "l", linkables.TableAlias())

	c1 := &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
		NS:     ptrTo("ns1"),
		Field1: ptrTo("linked1"),
		Field2: fftypes.NewFFBigInt(11111),
		Field3: fftypes.JSONAnyPtr(`{"linked":1}`),
	}
	l1 := &TestLinkable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
		NS:          "ns1",
		Description: "linked to C1",
		CrudID:      c1.ID,
	}

	err := crudables.Insert(ctx, c1)
	assert.NoError(t, err)

	err = linkables.Insert(ctx, l1)
	assert.NoError(t, err)

	l1Copy, err := linkables.GetByID(ctx, l1.ID.String())
	assert.NoError(t, err)
	assert.Equal(t, c1.ID, l1Copy.CrudID)
	assert.Equal(t, "linked to C1", l1Copy.Description)
	assert.Equal(t, "linked1", l1Copy.Field1 /* from JOIN */)
	assert.Equal(t, int64(11111), l1Copy.Field2.Int64() /* from JOIN */)
	assert.Equal(t, int64(1), l1Copy.Field3.JSONObject().GetInt64("linked") /* from JOIN */)

	l1s, _, err := linkables.GetMany(ctx, LinkableQueryFactory.NewFilter(ctx).Eq("crud", c1.ID.String()))
	assert.NoError(t, err)
	assert.Len(t, l1s, 1)
	assert.Equal(t, c1.ID, l1s[0].CrudID)
	assert.Equal(t, "linked to C1", l1s[0].Description)
	assert.Equal(t, "linked1", l1s[0].Field1 /* from JOIN */)
	assert.Equal(t, int64(11111), l1s[0].Field2.Int64() /* from JOIN */)
	assert.Equal(t, int64(1), l1s[0].Field3.JSONObject().GetInt64("linked") /* from JOIN */)
}

func TestQueryFactoryAccessors(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc1 := newCRUDCollection(&db.Database, "ns1")
	assert.NotNil(t, tc1.NewFilterBuilder(context.Background()))
	assert.NotNil(t, tc1.NewUpdateBuilder(context.Background()))

	tc2 := newLinkableCollection(&db.Database, "ns1")
	assert.Nil(t, tc2.GetQueryFactory())
	assert.Nil(t, tc2.NewFilterBuilder(context.Background()))
	assert.Nil(t, tc2.NewUpdateBuilder(context.Background()))
}

func TestUpsertFailBegin(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	_, err := tc.Upsert(context.Background(), &TestCRUDable{}, UpsertOptimizationSkip)
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertFailQuery(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT.*").WillReturnError(fmt.Errorf("pop"))
	_, err := tc.Upsert(context.Background(), &TestCRUDable{}, UpsertOptimizationSkip)
	assert.Regexp(t, "FF00176", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertFailInsert(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT.*").WillReturnRows(mock.NewRows([]string{db.sequenceColumn}))
	mock.ExpectExec("INSERT.*").WillReturnError(fmt.Errorf("pop"))
	_, err := tc.Upsert(context.Background(), &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
	}, UpsertOptimizationSkip)
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertFailUpdate(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT.*").WillReturnRows(mock.NewRows([]string{db.sequenceColumn}).AddRow(12345))
	mock.ExpectExec("UPDATE.*").WillReturnError(fmt.Errorf("pop"))
	_, err := tc.Upsert(context.Background(), &TestCRUDable{}, UpsertOptimizationSkip)
	assert.Regexp(t, "FF00178", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertPSQLOptimizedCreated(t *testing.T) {
	after := (fftypes.FFTime)(fftypes.Now().Time().Add(1 * time.Hour))
	mp := NewMockProvider()
	mp.FakePSQLUpsertOptimization = true
	db, mock := mp.UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO crudables.*ON CONFLICT .* DO UPDATE SET.*RETURNING created").WillReturnRows(
		sqlmock.NewRows([]string{"created"}).AddRow(after.String()),
	)
	mock.ExpectCommit()
	created, err := tc.Upsert(context.Background(), &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
	}, UpsertOptimizationDB)
	assert.NoError(t, err)
	assert.True(t, created)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertPSQLOptimizedUpdated(t *testing.T) {
	before := fftypes.Now()
	mp := NewMockProvider()
	mp.FakePSQLUpsertOptimization = true
	db, mock := mp.UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO crudables.*ON CONFLICT .* DO UPDATE SET.*RETURNING created").WillReturnRows(
		sqlmock.NewRows([]string{"created"}).AddRow(before.String()),
	)
	mock.ExpectCommit()
	created, err := tc.Upsert(context.Background(), &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
	}, UpsertOptimizationDB)
	assert.NoError(t, err)
	assert.False(t, created)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertPSQLOptimizedBadID(t *testing.T) {
	mp := NewMockProvider()
	mp.FakePSQLUpsertOptimization = true
	db, mock := mp.UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectRollback()
	_, err := tc.Upsert(context.Background(), &TestCRUDable{}, UpsertOptimizationDB)
	assert.Regexp(t, "FF00138", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertPSQLOptimizedQueryFail(t *testing.T) {
	mp := NewMockProvider()
	mp.FakePSQLUpsertOptimization = true
	db, mock := mp.UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO crudables.*ON CONFLICT .* DO UPDATE SET.*RETURNING created").WillReturnError(fmt.Errorf("pop"))
	mock.ExpectRollback()
	_, err := tc.Upsert(context.Background(), &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
	}, UpsertOptimizationDB)
	assert.Regexp(t, "FF00176.*pop", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpsertPSQLOptimizedBadTimeReturn(t *testing.T) {
	mp := NewMockProvider()
	mp.FakePSQLUpsertOptimization = true
	db, mock := mp.UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO crudables.*ON CONFLICT .* DO UPDATE SET.*RETURNING created").WillReturnRows(
		sqlmock.NewRows([]string{"created"}).AddRow("!!!this is not a time!!!"),
	)
	mock.ExpectRollback()
	_, err := tc.Upsert(context.Background(), &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
	}, UpsertOptimizationDB)
	assert.Regexp(t, "FF00248.*FF00136", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyBeginFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.InsertMany(context.Background(), []*TestCRUDable{{}}, false)
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyMultiRowOK(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	db.FakePSQLInsert = true
	db.features.MultiRowInsert = true
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO crudables.*VALUES \(.*\),\(.*\)`).WillReturnRows(
		sqlmock.NewRows([]string{db.sequenceColumn}).
			AddRow(123).
			AddRow(234),
	)
	mock.ExpectCommit()
	err := tc.InsertMany(context.Background(), []*TestCRUDable{
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
	}, false)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyMultiRowFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	db.FakePSQLInsert = true
	db.features.MultiRowInsert = true
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO crudables.*VALUES \(.*\),\(.*\)`).WillReturnError(fmt.Errorf("pop"))
	err := tc.InsertMany(context.Background(), []*TestCRUDable{
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
	}, false)
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyMultiRowChunkedOK(t *testing.T) {
	mp := NewMockProvider()
	mp.MultiRowInsert = true
	mp.MaxPlaceholders = 22 // 11 columns per row, so 2 rows per chunk
	db, mock := mp.UTInit()
	db.FakePSQLInsert = true
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	// First chunk: 2 rows
	mock.ExpectQuery(`INSERT INTO crudables.*VALUES \(.*\),\(.*\)`).WillReturnRows(
		sqlmock.NewRows([]string{db.sequenceColumn}).
			AddRow(101).
			AddRow(102),
	)
	// Second chunk: 1 row
	mock.ExpectQuery(`INSERT INTO crudables.*VALUES \(.*\)`).WillReturnRows(
		sqlmock.NewRows([]string{db.sequenceColumn}).
			AddRow(103),
	)
	mock.ExpectCommit()
	err := tc.InsertMany(context.Background(), []*TestCRUDable{
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
	}, false)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyMultiRowChunkedFailSecondChunk(t *testing.T) {
	mp := NewMockProvider()
	mp.MultiRowInsert = true
	mp.MaxPlaceholders = 22
	db, mock := mp.UTInit()
	db.FakePSQLInsert = true
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO crudables.*VALUES \(.*\),\(.*\)`).WillReturnRows(
		sqlmock.NewRows([]string{db.sequenceColumn}).
			AddRow(101).
			AddRow(102),
	)
	mock.ExpectQuery(`INSERT INTO crudables.*VALUES \(.*\)`).WillReturnError(fmt.Errorf("pop"))
	err := tc.InsertMany(context.Background(), []*TestCRUDable{
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
	}, false)
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyMultiRowNoChunkingWhenUnderLimit(t *testing.T) {
	mp := NewMockProvider()
	mp.MultiRowInsert = true
	mp.MaxPlaceholders = 100 // 11 columns * 2 rows = 22, well under 100
	db, mock := mp.UTInit()
	db.FakePSQLInsert = true
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO crudables.*VALUES \(.*\),\(.*\)`).WillReturnRows(
		sqlmock.NewRows([]string{db.sequenceColumn}).
			AddRow(201).
			AddRow(202),
	)
	mock.ExpectCommit()
	err := tc.InsertMany(context.Background(), []*TestCRUDable{
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
	}, false)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertManyFallbackSingleRowFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO crudables.*`).WillReturnError(fmt.Errorf("pop"))
	err := tc.InsertMany(context.Background(), []*TestCRUDable{
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
		{ResourceBase: ResourceBase{ID: fftypes.NewUUID()}},
	}, false)
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertBeginFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.Insert(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertInsertFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("INSERT.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Insert(context.Background(), &TestCRUDable{
		ResourceBase: ResourceBase{
			ID: fftypes.NewUUID(),
		},
	})
	assert.Regexp(t, "FF00177", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertMissingUUID(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	err := tc.Insert(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00138", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceBeginFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.Replace(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceUpdateFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Replace(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00178", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByIDNotFound(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{}))
	_, err := tc.GetByID(context.Background(), fftypes.NewUUID().String(), FailIfNotFound)
	assert.Regexp(t, "FF00164", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByIDBadOpts(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	_, err := tc.GetByID(context.Background(), fftypes.NewUUID().String(), GetOption(999))
	assert.Regexp(t, "FF00212", err)
}

func TestGetByIDSelectFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnError(fmt.Errorf("pop"))
	_, err := tc.GetByID(context.Background(), fftypes.NewUUID().String())
	assert.Regexp(t, "FF00176", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByIDScanFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{}).AddRow())
	_, err := tc.GetByID(context.Background(), fftypes.NewUUID().String())
	assert.Regexp(t, "FF00182", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByIDReqQueryModifierFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	tc.ReadQueryModifier = func(sb sq.SelectBuilder) (sq.SelectBuilder, error) {
		return sb, fmt.Errorf("pop")
	}
	_, err := tc.GetByID(context.Background(), fftypes.NewUUID().String())
	assert.Regexp(t, "pop", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByNameNoNameSemantics(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := newLinkableCollection(&db.Database, "ns1")
	_, err := tc.GetByName(context.Background(), "any")
	assert.Regexp(t, "FF00214", err)
}

func TestGetByUUIDOrNameNotUUIDNilResult(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*name =.*").WillReturnRows(sqlmock.NewRows([]string{}))
	res, err := tc.GetByUUIDOrName(context.Background(), "something")
	assert.NoError(t, err)
	assert.Nil(t, res)
}

func TestGetByUUIDOrNameErrNotFoundUUIDParsableString(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*id =.*").WillReturnRows(sqlmock.NewRows([]string{}))
	mock.ExpectQuery("SELECT.*name =.*").WillReturnRows(sqlmock.NewRows([]string{}))
	res, err := tc.GetByUUIDOrName(context.Background(), fftypes.NewUUID().String(), FailIfNotFound)
	assert.Regexp(t, "FF00164", err)
	assert.Nil(t, res)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetFirstBadGetOpts(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	res, err := tc.GetFirst(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).And(), GetOption(-99))
	assert.Regexp(t, "FF00212", err)
	assert.Nil(t, res)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetFirstQueryFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnError(fmt.Errorf("pop"))
	res, err := tc.GetFirst(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF00176", err)
	assert.Nil(t, res)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetManyInvalidOp(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	_, _, err := tc.getManyScoped(context.Background(), "", &ffapi.FilterInfo{Op: ffapi.FilterOp("!wrong")}, nil, nil, nil)
	assert.Regexp(t, "FF00190", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetManySelectFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnError(fmt.Errorf("pop"))
	_, _, err := tc.GetMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF00176", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetManyReadModifierFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	tc.ReadQueryModifier = func(sb sq.SelectBuilder) (sq.SelectBuilder, error) {
		return sb, fmt.Errorf("pop")
	}
	_, _, err := tc.GetMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).And())
	assert.Regexp(t, "pop", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByManyScanFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{}).AddRow())
	_, _, err := tc.GetMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF00182", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetByManyFilterFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	_, _, err := tc.GetMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).Eq("wrong", 123))
	assert.Regexp(t, "FF00142", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateBeginFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	mock.ExpectExec("UPDATE.*").WillReturnResult(driver.ResultNoRows)
	err := tc.Update(context.Background(), fftypes.NewUUID().String(), CRUDableQueryFactory.NewUpdate(context.Background()).Set("f1", "12345"))
	assert.Regexp(t, "FF00175", err)
}

func TestUpdateBuildUpdateFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	err := tc.UpdateMany(context.Background(),
		CRUDableQueryFactory.NewFilter(context.Background()).Eq("wrong", 123),
		CRUDableQueryFactory.NewUpdate(context.Background()).Set("f1", "12345"))
	assert.Regexp(t, "FF00142", err)
}

func TestUpdateUpdateFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Update(context.Background(), fftypes.NewUUID().String(), CRUDableQueryFactory.NewUpdate(context.Background()).Set("f1", "12345"))
	assert.Regexp(t, "FF00178", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateNowRows(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnResult(driver.ResultNoRows)
	err := tc.Update(context.Background(), fftypes.NewUUID().String(), CRUDableQueryFactory.NewUpdate(context.Background()).Set("f1", "12345"))
	assert.Regexp(t, "FF00205", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteBeginFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	mock.ExpectExec("UPDATE.*").WillReturnResult(driver.ResultNoRows)
	err := tc.Delete(context.Background(), fftypes.NewUUID().String())
	assert.Regexp(t, "FF00175", err)
}

func TestDeleteDeleteFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("DELETE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.Delete(context.Background(), fftypes.NewUUID().String())
	assert.Regexp(t, "FF00179", err)
}

func TestIsNilNonPtrType(t *testing.T) {
	assert.False(t, isNil(0))
}

func TestUpdateSparseFailBegin(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.UpdateSparse(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateSparseFailUpdate(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.UpdateSparse(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00178", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateSparseFailNoResult(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	err := tc.UpdateSparse(context.Background(), &TestCRUDable{})
	assert.Regexp(t, "FF00205", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteManyBeginFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := tc.DeleteMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF00175", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteManyDeleteFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	mock.ExpectExec("DELETE.*").WillReturnError(fmt.Errorf("pop"))
	err := tc.DeleteMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF00179", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteManyBadFilter(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectBegin()
	err := tc.DeleteMany(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).Eq(
		"wrong", "anything",
	))
	assert.Regexp(t, "FF00142", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCountFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT COUNT.*").WillReturnError(fmt.Errorf("pop"))
	_, err := tc.Count(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).And())
	assert.Regexp(t, "FF00176", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCountBadFilter(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	_, err := tc.Count(context.Background(), CRUDableQueryFactory.NewFilter(context.Background()).Eq(
		"wrong", "anything",
	))
	assert.Regexp(t, "FF00142", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetSequenceForIDFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnError(fmt.Errorf("pop"))
	_, err := tc.GetSequenceForID(context.Background(), "id12345")
	assert.Regexp(t, "FF00176", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetSequenceForIDNotFound(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{db.SequenceColumn()}))
	_, err := tc.GetSequenceForID(context.Background(), "id12345")
	assert.Regexp(t, "FF00164", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetSequenceForIDScanFail(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	tc := newCRUDCollection(&db.Database, "ns1")
	mock.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{db.SequenceColumn()}).AddRow("not a number"))
	_, err := tc.GetSequenceForID(context.Background(), "id12345")
	assert.Regexp(t, "FF00182", err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateNewInstanceNil(t *testing.T) {
	tc := &CrudBase[*TestCRUDable]{
		NewInstance: func() *TestCRUDable { return nil },
	}
	assert.PanicsWithValue(t, "NewInstance() value must not be nil", func() {
		tc.Validate()
	})
}

func TestValidateDupSeqColumn(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{"seq"},
	}
	assert.PanicsWithValue(t, "cannot have column named 'seq'", func() {
		tc.Validate()
	})
}

func TestValidateNonPtr(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{"test1"},
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			return "not a pointer"
		},
	}
	assert.PanicsWithValue(t, "field test1 does not seem to be a pointer type - prevents null-check for PATCH semantics functioning", func() {
		tc.Validate()
	})
}

func TestValidateDupColumn(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{"test1", "test1"},
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			var t *string
			return &t
		},
	}
	assert.PanicsWithValue(t, "test1 is a duplicated column", func() {
		tc.Validate()
	})
}

func TestValidateMissingID(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{"test1"},
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			var t *string
			return &t
		},
	}
	assert.Panics(t, func() {
		tc.Validate()
	})
}

func TestValidateNilValueNonNil(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{ColumnID, ColumnCreated, ColumnUpdated},
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			var t *string
			return &t
		},
		NilValue: func() *TestCRUDable { return &TestCRUDable{} },
	}
	assert.PanicsWithValue(t, "NilValue() value must be nil", func() {
		tc.Validate()
	})
}

func TestValidateGetFieldPtrNotNilForUnknown(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:          &db.Database,
		NewInstance: func() *TestCRUDable { return &TestCRUDable{} },
		Columns:     []string{ColumnID, ColumnCreated, ColumnUpdated},
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			var t *string
			return &t
		},
		NilValue:     func() *TestCRUDable { return nil },
		ScopedFilter: func() sq.Eq { return sq.Eq{} },
	}
	assert.PanicsWithValue(t, "GetFieldPtr() must return nil for unknown column", func() {
		tc.Validate()
	})
}

func TestValidateNameSemanticsWithoutQueryFactory(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:            &db.Database,
		NewInstance:   func() *TestCRUDable { return &TestCRUDable{} },
		Columns:       []string{ColumnID},
		TimesDisabled: true,
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			if col == "id" {
				var t *string
				return &t
			}
			return nil
		},
		NilValue:     func() *TestCRUDable { return nil },
		ScopedFilter: func() sq.Eq { return sq.Eq{} },
		NameField:    "name",
	}
	assert.PanicsWithValue(t, "QueryFactory must be set when name semantics are enabled", func() {
		tc.Validate()
	})
}

func TestCustomIDColumn(t *testing.T) {
	db, _ := NewMockProvider().UTInit()
	tc := &CrudBase[*TestCRUDable]{
		DB:            &db.Database,
		NewInstance:   func() *TestCRUDable { return &TestCRUDable{} },
		NilValue:      func() *TestCRUDable { return nil },
		IDField:       "f1",
		Columns:       []string{"f1"},
		TimesDisabled: true,
		PatchDisabled: true,
		GetFieldPtr: func(inst *TestCRUDable, col string) interface{} {
			if col == "id" {
				var t *string
				return &t
			}
			return nil
		},
	}
	tc.Validate()
}
