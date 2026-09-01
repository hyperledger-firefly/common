// Copyright © 2024-2026 Kaleido, Inc.
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
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
)

func TestBuildPostgreSQLOptimizedUpsert(t *testing.T) {

	now := fftypes.Now()
	q, err := BuildPostgreSQLOptimizedUpsert(context.Background(), "table1", "id", []string{
		"created",
		"updated",
		"mutable_col",
		"immutable_col",
	}, []string{
		"updated",
		"mutable_col",
	}, "created", map[string]driver.Value{
		"created":       now,
		"updated":       now,
		"mutable_col":   "value1",
		"immutable_col": "value2",
	})
	assert.NoError(t, err)

	queryStr, values, err := q.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO table1 (created,updated,mutable_col,immutable_col) VALUES (?,?,?,?) ON CONFLICT (id) DO UPDATE SET updated = ?, mutable_col = ? RETURNING created", queryStr)
	assert.Equal(t, []interface{}{
		now, now, "value1", "value2",
		now, "value1",
	}, values)

}

func TestBuildPostgreSQLOptimizedUpsertFail(t *testing.T) {

	_, err := BuildPostgreSQLOptimizedUpsert(context.Background(), "", "", []string{}, []string{}, "", map[string]driver.Value{})
	assert.Regexp(t, "FF00247", err)

}

func TestGetPostgreSQLDatabaseName(t *testing.T) {

	dbName, err := GetPostgreSQLDatabaseName("postgres://user:password@host:5432/dbname")
	assert.NoError(t, err)
	assert.Equal(t, "dbname", dbName)

	dbName, err = GetPostgreSQLDatabaseName("postgres://user:password@host:5432/dbnameWithParam?param=value")
	assert.NoError(t, err)
	assert.Equal(t, "dbnameWithParam", dbName)

	dbName, err = GetPostgreSQLDatabaseName("postgres://user:password@host:5432/dbnameWithParams?param=value&param=value")
	assert.NoError(t, err)
	assert.Equal(t, "dbnameWithParams", dbName)

	// postgresql:// scheme variant
	dbName, err = GetPostgreSQLDatabaseName("postgresql://user:password@host:5432/mydb")
	assert.NoError(t, err)
	assert.Equal(t, "mydb", dbName)

	// Password with special characters (percent-encoded per RFC 3986)
	dbName, err = GetPostgreSQLDatabaseName("postgres://user:p%40ss%2Fword@host:5432/mydb")
	assert.NoError(t, err)
	assert.Equal(t, "mydb", dbName)

	// No dbname in scheme URL — must error, not return host/user/password
	_, err = GetPostgreSQLDatabaseName("postgres://user:password@host:5432")
	assert.Regexp(t, "FF00262", err)

	_, err = GetPostgreSQLDatabaseName("postgres://user:password@host:5432/")
	assert.Regexp(t, "FF00262", err)

	_, err = GetPostgreSQLDatabaseName("postgres://user:password@host:5432?sslmode=disable")
	assert.Regexp(t, "FF00262", err)

	// No scheme — not a valid libpq URI
	_, err = GetPostgreSQLDatabaseName("host:5432/dbname")
	assert.Regexp(t, "FF00262", err)

	// Empty string
	_, err = GetPostgreSQLDatabaseName("")
	assert.Regexp(t, "FF00262", err)

}
