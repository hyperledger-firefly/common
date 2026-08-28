// Copyright © 2026 Kaleido, Inc.
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
	"fmt"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	sqlite3driver "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

// fakeSQLStateError mimics the shape of *pq.Error / *pgconn.PgError without depending on a Postgres driver
type fakeSQLStateError struct {
	code string
}

func (e *fakeSQLStateError) Error() string    { return "pq: fake error " + e.code }
func (e *fakeSQLStateError) SQLState() string { return e.code }

func TestConstraintViolationSQLStateFallback(t *testing.T) {
	// No provider classifier configured, so the SQLSTATE fallback is used
	s := &Database{}

	assert.False(t, s.IsUniqueViolation(nil))
	assert.False(t, s.IsForeignKeyViolation(nil))
	assert.False(t, s.IsUniqueViolation(fmt.Errorf("pop")))
	assert.False(t, s.IsForeignKeyViolation(fmt.Errorf("pop")))
	assert.False(t, s.IsUniqueViolation(&fakeSQLStateError{code: "23502"})) // not-null violation
	assert.False(t, s.IsForeignKeyViolation(&fakeSQLStateError{code: "23502"}))

	uniqueErr := &fakeSQLStateError{code: SQLStateUniqueViolation}
	fkErr := &fakeSQLStateError{code: SQLStateForeignKeyViolation}
	assert.True(t, s.IsUniqueViolation(uniqueErr))
	assert.False(t, s.IsForeignKeyViolation(uniqueErr))
	assert.True(t, s.IsForeignKeyViolation(fkErr))
	assert.False(t, s.IsUniqueViolation(fkErr))

	// Wrapped the way InsertTx wraps driver errors, and wrapped again by a caller
	wrapped := i18n.WrapError(context.Background(), uniqueErr, i18n.MsgDBInsertFailed)
	assert.Regexp(t, "FF00177", wrapped)
	assert.True(t, s.IsUniqueViolation(wrapped))
	assert.True(t, s.IsUniqueViolation(errors.Wrap(wrapped, "outer")))
	assert.True(t, s.IsForeignKeyViolation(i18n.WrapError(context.Background(), fkErr, i18n.MsgDBDeleteFailed)))

	// The fallback cannot identify the constraint, so a request for a specific constraint fails closed
	assert.False(t, s.IsUniqueViolation(wrapped, "my_constraint"))
	assert.False(t, s.IsForeignKeyViolation(fkErr, "my_fk"))
}

func TestConstraintViolationProviderClassifier(t *testing.T) {
	uniqueErr := fmt.Errorf("duplicate key value violates unique constraint \"crudables_id\"")
	fkErr := fmt.Errorf("insert or update on table \"linkables\" violates foreign key constraint \"linkables_crud_id_fkey\"")
	otherErr := fmt.Errorf("pop")
	s := &Database{
		features: SQLFeatures{
			ConstraintViolationClassifier: func(err error) *ConstraintViolation {
				switch {
				case errors.Is(err, uniqueErr):
					return &ConstraintViolation{SQLState: SQLStateUniqueViolation, Constraint: "crudables_id"}
				case errors.Is(err, fkErr):
					return &ConstraintViolation{SQLState: SQLStateForeignKeyViolation, Constraint: "linkables_crud_id_fkey"}
				}
				return nil
			},
		},
	}

	assert.False(t, s.IsUniqueViolation(nil))
	assert.False(t, s.IsUniqueViolation(otherErr))
	assert.False(t, s.IsUniqueViolation(otherErr, "crudables_id"))
	assert.False(t, s.IsForeignKeyViolation(otherErr))

	assert.True(t, s.IsUniqueViolation(uniqueErr))
	assert.True(t, s.IsUniqueViolation(errors.Wrap(uniqueErr, "wrapped"), "crudables_id"))
	assert.True(t, s.IsUniqueViolation(uniqueErr, "some_other_index", "crudables_id"))
	assert.False(t, s.IsUniqueViolation(uniqueErr, "some_other_index"))
	assert.False(t, s.IsForeignKeyViolation(uniqueErr))

	assert.True(t, s.IsForeignKeyViolation(fkErr))
	assert.True(t, s.IsForeignKeyViolation(fkErr, "linkables_crud_id_fkey"))
	assert.False(t, s.IsForeignKeyViolation(fkErr, "some_other_fkey"))
	assert.False(t, s.IsUniqueViolation(fkErr))
}

func TestConstraintViolationThroughInsertTx(t *testing.T) {
	// Mock provider has no classifier, so this exercises the SQLSTATE fallback through the real
	// InsertTx error wrapping path
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
	assert.NoError(t, err)
	sb := sq.Insert("table").Columns("col1").Values("val1")

	mdb.ExpectExec("INSERT.*").WillReturnError(&fakeSQLStateError{code: SQLStateUniqueViolation})
	_, err = s.InsertTx(ctx, "table1", tx, sb, nil)
	assert.Regexp(t, "FF00177", err)
	assert.True(t, s.IsUniqueViolation(err))
	assert.False(t, s.IsForeignKeyViolation(err))

	mdb.ExpectExec("INSERT.*").WillReturnError(&fakeSQLStateError{code: SQLStateForeignKeyViolation})
	_, err = s.InsertTx(ctx, "table1", tx, sb, nil)
	assert.Regexp(t, "FF00177", err)
	assert.True(t, s.IsForeignKeyViolation(err))
	assert.False(t, s.IsUniqueViolation(err))

	// A different failure is not misreported
	mdb.ExpectExec("INSERT.*").WillReturnError(fmt.Errorf("pop"))
	_, err = s.InsertTx(ctx, "table1", tx, sb, nil)
	assert.Regexp(t, "FF00177", err)
	assert.False(t, s.IsUniqueViolation(err))
	assert.False(t, s.IsForeignKeyViolation(err))
}

func TestUniqueViolationSQLiteEnd2End(t *testing.T) {
	sql, done := newSQLiteTestProvider(t)
	defer done()
	ctx := context.Background()

	collection := newCRUDCollection(sql.db, "ns1")
	c1 := &TestCRUDable{
		ResourceBase: ResourceBase{ID: fftypes.NewUUID()},
		NS:           ptrTo("ns1"),
		Name:         ptrTo("bob"),
	}
	err := collection.Insert(ctx, c1)
	assert.NoError(t, err)

	// Second insert of the same (ns, id) violates the crudables_id unique index
	err = collection.Insert(ctx, c1)
	assert.Regexp(t, "FF00177", err)
	assert.True(t, sql.db.IsUniqueViolation(err))
	assert.False(t, sql.db.IsForeignKeyViolation(err))
	// SQLite does not report the constraint name, so asking for one fails closed
	assert.False(t, sql.db.IsUniqueViolation(err, "crudables_id"))

	// A row that violates nothing is fine
	c2 := &TestCRUDable{
		ResourceBase: ResourceBase{ID: fftypes.NewUUID()},
		NS:           ptrTo("ns1"),
		Name:         ptrTo("sally"),
	}
	assert.NoError(t, collection.Insert(ctx, c2))
}

func TestForeignKeyViolationSQLiteEnd2End(t *testing.T) {
	sql, done := newSQLiteTestProvider(t)
	defer done()
	ctx := context.Background()

	// SQLite has foreign key enforcement off by default, and the test schema has no FKs - set one up
	_, err := sql.db.db.ExecContext(ctx, "PRAGMA foreign_keys = ON")
	assert.NoError(t, err)
	_, err = sql.db.db.ExecContext(ctx, `CREATE TABLE fk_children (
		seq      INTEGER PRIMARY KEY AUTOINCREMENT,
		crud_seq INTEGER NOT NULL REFERENCES crudables(seq)
	)`)
	assert.NoError(t, err)

	ctx, tx, autoCommit, err := sql.db.BeginOrUseTx(ctx)
	assert.NoError(t, err)
	defer sql.db.RollbackTx(ctx, tx, autoCommit)

	// Referencing a parent row that does not exist
	_, err = sql.db.InsertTx(ctx, "fk_children", tx, sq.Insert("fk_children").Columns("crud_seq").Values(999999), nil)
	assert.Regexp(t, "FF00177", err)
	assert.True(t, sql.db.IsForeignKeyViolation(err))
	assert.False(t, sql.db.IsUniqueViolation(err))
	assert.False(t, sql.db.IsForeignKeyViolation(err, "some_fkey"))
}

func TestSQLiteConstraintViolationClassifier(t *testing.T) {
	v := sqliteConstraintViolationClassifier(sqlite3driver.Error{Code: sqlite3driver.ErrConstraint, ExtendedCode: sqlite3driver.ErrConstraintUnique})
	assert.Equal(t, &ConstraintViolation{SQLState: SQLStateUniqueViolation}, v)

	v = sqliteConstraintViolationClassifier(errors.Wrap(sqlite3driver.Error{Code: sqlite3driver.ErrConstraint, ExtendedCode: sqlite3driver.ErrConstraintPrimaryKey}, "wrapped"))
	assert.Equal(t, &ConstraintViolation{SQLState: SQLStateUniqueViolation}, v)

	v = sqliteConstraintViolationClassifier(sqlite3driver.Error{Code: sqlite3driver.ErrConstraint, ExtendedCode: sqlite3driver.ErrConstraintForeignKey})
	assert.Equal(t, &ConstraintViolation{SQLState: SQLStateForeignKeyViolation}, v)

	// Other constraint types (e.g. NOT NULL) and non-constraint errors are not classified
	assert.Nil(t, sqliteConstraintViolationClassifier(sqlite3driver.Error{Code: sqlite3driver.ErrConstraint, ExtendedCode: sqlite3driver.ErrConstraintNotNull}))
	assert.Nil(t, sqliteConstraintViolationClassifier(sqlite3driver.Error{Code: sqlite3driver.ErrBusy}))
	assert.Nil(t, sqliteConstraintViolationClassifier(fmt.Errorf("pop")))
}
