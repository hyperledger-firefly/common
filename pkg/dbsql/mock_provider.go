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
	"database/sql"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	sq "github.com/Masterminds/squirrel"
	migratedb "github.com/golang-migrate/migrate/v4/database"
	"github.com/hyperledger-firefly/common/mocks/dbmigratemocks"
	"github.com/hyperledger-firefly/common/pkg/config"
)

// MockProvider uses the datadog mocking framework
type MockProvider struct {
	MockProviderConfig

	Database
	config config.Section

	mockDB *sql.DB
	mdb    sqlmock.Sqlmock
	mmg    *dbmigratemocks.Driver
}

type MockProviderConnScoped struct {
	MockProvider
}

type MockProviderConfig struct {
	FakePSQLInsert             bool
	OpenError                  error
	GetDatabaseNameError       error
	GetMigrationDriverError    error
	IndividualSort             bool
	MultiRowInsert             bool
	MaxPlaceholders            int
	FakePSQLUpsertOptimization bool
}

func NewMockProvider() *MockProvider {
	config.RootConfigReset()
	conf := config.RootSection("unittest.db")
	conf.AddKnownKey("url", "test")
	mp := &MockProvider{
		config: conf,
		mmg:    &dbmigratemocks.Driver{},
	}
	mp.Database.InitConfig(mp, mp.config)
	mp.config.Set(SQLConfMaxConnections, 10)
	mp.mockDB, mp.mdb, _ = sqlmock.New()
	return mp
}

// init is a convenience to init for tests that aren't testing init itself
func (mp *MockProvider) UTInit() (*MockProvider, sqlmock.Sqlmock) {
	_ = mp.Init(context.Background(), mp, mp.config)
	return mp, mp.mdb
}

func (mp *MockProvider) Name() string {
	return "mockdb"
}

func (mp *MockProvider) SequenceColumn() string {
	return "seq"
}

func (mp *MockProvider) MigrationsDir() string {
	return mp.Name()
}

func (mp *MockProvider) Features() SQLFeatures {
	features := DefaultSQLProviderFeatures()
	features.UseILIKE = true
	features.AcquireLock = func(lockName string) string {
		return fmt.Sprintf(`<acquire lock %s>`, lockName)
	}
	features.MultiRowInsert = mp.MultiRowInsert
	features.MaxPlaceholders = mp.MaxPlaceholders
	if mp.FakePSQLUpsertOptimization {
		features.DBOptimizedUpsertBuilder = BuildPostgreSQLOptimizedUpsert
	}
	return features
}

func (mp *MockProvider) ApplyInsertQueryCustomizations(insert sq.InsertBuilder, _ bool) (sq.InsertBuilder, bool) {
	if mp.FakePSQLInsert {
		return insert.Suffix(" RETURNING seq"), true
	}
	return insert, false
}

func (mp *MockProvider) Open(_ string) (*sql.DB, error) {
	return mp.mockDB, mp.OpenError
}

func (mp *MockProvider) GetMigrationDriver(_ *sql.DB) (migratedb.Driver, error) {
	return mp.mmg, mp.GetMigrationDriverError
}

func (mp *MockProviderConnScoped) GetMigrationDriverConn(_ *sql.DB) (migratedb.Driver, error) {
	return mp.mmg, mp.GetMigrationDriverError
}

func (mp *MockProvider) GetDatabaseName(_ string) (string, error) {
	return "mockdb", mp.GetDatabaseNameError
}
