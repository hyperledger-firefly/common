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
	"strconv"

	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
)

// GetChartHistogram executes a collection of queries (one per interval) and builds
// a histogram response for the specified table and time intervals.
func (s *Database) GetChartHistogram(
	ctx context.Context,
	tableName string,
	timestampColumn string,
	typeColumn string,
	namespaceColumn string,
	namespaceValue string,
	intervals []fftypes.ChartHistogramInterval,
) ([]*fftypes.ChartHistogram, error) {

	maxRows := config.GetUint64(SQLConfHistogramsMaxChartRows)

	// check if we have a type column for grouping
	hasTypeColumn := typeColumn != ""

	// Build qs for each interval
	queries := s.buildHistogramQueries(
		tableName, timestampColumn, typeColumn,
		namespaceColumn, namespaceValue,
		intervals, maxRows,
	)

	histogramList := []*fftypes.ChartHistogram{}

	for i, query := range queries {
		rows, _, err := s.Query(ctx, tableName, query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		data, total, err := s.processHistogramRows(ctx, tableName, rows, hasTypeColumn)
		if err != nil {
			return nil, err
		}

		// Build hist bucket
		histBucket := &fftypes.ChartHistogram{
			Count:     strconv.FormatUint(total, 10),
			Timestamp: intervals[i].StartTime,
			Types:     []*fftypes.ChartHistogramType{},
			IsCapped:  total == maxRows,
		}

		// Add type counts if applicable
		if hasTypeColumn {
			for t, c := range data {
				histBucket.Types = append(histBucket.Types,
					&fftypes.ChartHistogramType{
						Count: strconv.Itoa(c),
						Type:  t,
					})
			}
		}

		histogramList = append(histogramList, histBucket)
	}

	return histogramList, nil
}

// buildHistogramQueries constructs SQL queries for each time interval.
// each query selects data within the interval's time range, optionally
// filtered by namespace, and limited to maxRows.
func (s *Database) buildHistogramQueries(
	tableName string,
	timestampColumn string,
	typeColumn string,
	namespaceColumn string,
	namespaceValue string,
	intervals []fftypes.ChartHistogramInterval,
	maxRows uint64,
) []sq.SelectBuilder {

	queries := []sq.SelectBuilder{}

	// Determine columns to select
	cols := []string{timestampColumn}
	if typeColumn != "" {
		cols = append(cols, typeColumn)
	}

	for _, i := range intervals {
		whereClause := sq.And{
			sq.GtOrEq{timestampColumn: i.StartTime},
			sq.Lt{timestampColumn: i.EndTime},
		}

		// namespace filter
		if namespaceColumn != "" && namespaceValue != "" {
			whereClause = append(whereClause, sq.Eq{namespaceColumn: namespaceValue})
		}

		// Build query with PlaceholderFormat applied
		query := sq.Select(cols...).
			From(tableName).
			Where(whereClause).
			OrderBy(timestampColumn).
			Limit(maxRows).
			PlaceholderFormat(s.features.PlaceholderFormat)

		queries = append(queries, query)
	}

	return queries
}

// processHistogramRows scans SQL result rows and builds histogram data
// If hasTypeColumn is true, it groups counts by type else it just
// counts total rows.
func (s *Database) processHistogramRows(
	ctx context.Context,
	tableName string,
	rows *sql.Rows,
	hasTypeColumn bool,
) (map[string]int, uint64, error) {

	total := uint64(0)

	if !hasTypeColumn {
		// counting rows
		for rows.Next() {
			var timestamp interface{}
			if err := rows.Scan(&timestamp); err != nil {
				return nil, 0, i18n.NewError(ctx, i18n.MsgDBReadErr, tableName)
			}
			total++
		}
		return map[string]int{}, total, nil
	}

	// Count by type
	typeMap := map[string]int{}
	for rows.Next() {
		var timestamp interface{}
		var typeStr string
		if err := rows.Scan(&timestamp, &typeStr); err != nil {
			return nil, 0, i18n.NewError(ctx, i18n.MsgDBReadErr, tableName)
		}

		typeMap[typeStr]++
		total++
	}

	return typeMap, total, nil
}
