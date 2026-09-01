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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSQLiteDatabaseName(t *testing.T) {
	p := &sqLiteProvider{}

	dbName, err := p.GetDatabaseName("/var/data/myapp.db")
	assert.NoError(t, err)
	assert.Equal(t, "myapp", dbName)

	dbName, err = p.GetDatabaseName("/var/data/myapp.sqlite?_journal=WAL")
	assert.NoError(t, err)
	assert.Equal(t, "myapp", dbName)

	dbName, err = p.GetDatabaseName("file:/var/data/myapp.sqlite?mode=ro")
	assert.NoError(t, err)
	assert.Equal(t, "myapp", dbName)

	dbName, err = p.GetDatabaseName("myapp.db")
	assert.NoError(t, err)
	assert.Equal(t, "myapp", dbName)

	// :memory: falls back to generic "sqlite"
	dbName, err = p.GetDatabaseName(":memory:")
	assert.NoError(t, err)
	assert.Equal(t, "sqlite", dbName)

	// Empty DSN falls back to generic "sqlite"
	dbName, err = p.GetDatabaseName("")
	assert.NoError(t, err)
	assert.Equal(t, "sqlite", dbName)
}
