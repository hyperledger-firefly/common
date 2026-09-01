// Copyright © 2025 Kaleido, Inc.
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
	"database/sql"
	"database/sql/driver"

	sq "github.com/Masterminds/squirrel"
	migratedb "github.com/golang-migrate/migrate/v4/database"
)

type SQLFeatures struct {
	UseILIKE                   bool
	MultiRowInsert             bool
	PlaceholderFormat          sq.PlaceholderFormat
	AcquireLock                func(lockName string) string
	CustomSequenceColumnFilter bool // without this set, any use of the column "sequence" will break in filtering
	// DB specific query builder for RDBMS-side optimized upsert, returning the requested column from the query
	// (the CRUD layer will request the create time column to detect if the record was new or not)
	DBOptimizedUpsertBuilder func(ctx context.Context, table string, idColumn string, insertCols, updateCols []string, returnCol string, values map[string]driver.Value) (sq.InsertBuilder, error)
	// MaxPlaceholders caps the number of SQL placeholders ($N) in a single statement.
	// When >0 and MultiRowInsert is true, InsertMany will chunk rows so that
	// rows*columns never exceeds this limit. PostgreSQL's wire protocol limit is 65535.
	MaxPlaceholders int
}

func DefaultSQLProviderFeatures() SQLFeatures {
	return SQLFeatures{
		UseILIKE:          false,
		MultiRowInsert:    false,
		PlaceholderFormat: sq.Dollar,
	}
}

// Provider defines the interface an individual provider muse implement to customize the Database implementation
type Provider interface {

	// Name is the name of the database driver
	Name() string

	// MigrationDir is the subdirectory for migrations
	MigrationsDir() string

	// SequenceColumn is the name of the sequence column to use
	SequenceColumn() string

	// Open creates the DB instances
	Open(url string) (*sql.DB, error)

	// GetDriver returns the driver implementation - preferred if supported to implement GetMigrationDriverConn so the connection can be cleaned up (cleaning this driver up closes the whole DB)
	GetMigrationDriver(*sql.DB) (migratedb.Driver, error)

	// Features returns database specific configuration switches
	Features() SQLFeatures

	// ApplyInsertQueryCustomizations updates the INSERT query for returning the Sequence, and returns whether it needs to be run as a query to return the Sequence field
	ApplyInsertQueryCustomizations(insert sq.InsertBuilder, requestConflictEmptyResult bool) (updatedInsert sq.InsertBuilder, runAsQuery bool)
}

// Implementing this interface allows cleanup of the connection used during migration
type ProviderCloseableMigrationDriver interface {
	// Returns the driver implementation that is safe to close. Specifically this means the WithConnection() use in migration, rather than WithInstance().
	GetMigrationDriverClosable(*sql.DB) (migratedb.Driver, error)
}
