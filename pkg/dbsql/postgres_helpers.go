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
	"database/sql/driver"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger-firefly/common/pkg/i18n"
)

// PostgreSQLMaxPlaceholders is the limit on bind parameters in one statement, set by the PostgreSQL wire protocol
const PostgreSQLMaxPlaceholders = 65535

// DefaultPostgreSQLProviderFeatures are the features shared by PostgreSQL providers, which can add their own on top
func DefaultPostgreSQLProviderFeatures() SQLFeatures {
	features := DefaultSQLProviderFeatures()
	features.PlaceholderFormat = sq.Dollar
	features.MultiRowInsert = true
	features.MaxPlaceholders = PostgreSQLMaxPlaceholders
	features.DBOptimizedUpsertBuilder = BuildPostgreSQLOptimizedUpsert
	return features
}

// BuildPostgreSQLOptimizedUpsert is a PostgreSQL helper to avoid implementing this lots of times in child packages
func BuildPostgreSQLOptimizedUpsert(ctx context.Context, table string, idColumn string, insertCols, updateCols []string, returnCol string, values map[string]driver.Value) (insert sq.InsertBuilder, err error) {
	insertValues := make([]interface{}, 0, len(insertCols))
	for _, c := range insertCols {
		insertValues = append(insertValues, values[c])
	}
	insert = sq.Insert(table).Columns(insertCols...).Values(insertValues...)
	update := sq.Update("REMOVED_BEFORE_RUNNING" /* cheat to avoid table name */)
	for _, c := range updateCols {
		update = update.Set(c, values[c])
	}
	updateSQL, updateValues, err := update.ToSql()
	updateSQL, ok := strings.CutPrefix(updateSQL, "UPDATE REMOVED_BEFORE_RUNNING ")
	if err != nil || !ok {
		return insert, i18n.NewError(ctx, i18n.MsgDBErrorBuildingStatement, err)
	}
	return insert.Suffix(fmt.Sprintf("ON CONFLICT (%s) DO UPDATE", idColumn)).SuffixExpr(sq.Expr(updateSQL, updateValues...)).Suffix("RETURNING " + returnCol), nil
}
