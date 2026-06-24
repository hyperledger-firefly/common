// Copyright © 2024 Kaleido, Inc.
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

	sq "github.com/Masterminds/squirrel"
	migratedb "github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/hyperledger-firefly/common/pkg/config"

	// Import SQLite driver
	_ "github.com/mattn/go-sqlite3"
)

func InitSQLiteConfig(conf config.Section) {
	(&sqLiteProvider{}).InitConfig(&sqLiteProvider{}, conf)
}

// SQLiteProvider uses simple SQLite in-process database.
// Good for building tests, and simple developer environments.
func NewSQLiteProvider(ctx context.Context, conf config.Section) (*Database, error) {
	sqlite := &sqLiteProvider{}
	return &sqlite.Database, sqlite.Init(ctx, sqlite, conf)
}

type sqLiteProvider struct {
	Database
}

func (p *sqLiteProvider) Name() string {
	return "sqlite3"
}

func (p *sqLiteProvider) MigrationsDir() string {
	return "sqlite"
}

func (p *sqLiteProvider) SequenceColumn() string {
	return "seq"
}

func (p *sqLiteProvider) Features() SQLFeatures {
	features := DefaultSQLProviderFeatures()
	features.PlaceholderFormat = sq.Dollar
	features.UseILIKE = false // Not supported
	return features
}

func (p *sqLiteProvider) ApplyInsertQueryCustomizations(insert sq.InsertBuilder, _ bool) (sq.InsertBuilder, bool) {
	// Nothing required - QL supports the query for returning the generated ID, and we use that for the sequence
	return insert, false
}

func (p *sqLiteProvider) Open(url string) (*sql.DB, error) {
	return sql.Open("sqlite3", url)
}

func (p *sqLiteProvider) GetMigrationDriver(db *sql.DB) (migratedb.Driver, error) {
	return sqlite3.WithInstance(db, &sqlite3.Config{})
}
