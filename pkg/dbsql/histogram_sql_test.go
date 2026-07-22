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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
)

func init() {
	config.RootSection("").AddKnownKey(SQLConfHistogramsMaxChartRows, 100)
}

func newIntervals(n int) []fftypes.ChartHistogramInterval {
	ints := make([]fftypes.ChartHistogramInterval, 0, n)
	for i := 0; i < n; i++ {
		ints = append(ints, fftypes.ChartHistogramInterval{
			StartTime: fftypes.Now(),
			EndTime:   fftypes.Now(),
		})
	}
	return ints
}

func TestBuildHistogramQueriesWithTypeAndNamespace(t *testing.T) {
	db, _ := NewMockProvider().UTInit()

	i := newIntervals(2)

	queries := db.buildHistogramQueries(
		"mytable",
		"created",
		"type",
		"namespace",
		"ns1",
		i,
		10,
	)

	assert.Len(t, queries, 2)

	sqlStr, args, err := queries[0].ToSql()
	assert.NoError(t, err)

	assert.Regexp(t,
		`SELECT created, type FROM mytable WHERE \(created >= .+ AND created < .+ AND namespace = .+\) ORDER BY created LIMIT 10`,
		sqlStr,
	)

	assert.Len(t, args, 3)
	assert.NotNil(t, args[0])
	assert.NotNil(t, args[1])
	assert.Equal(t, "ns1", args[2])
}

func TestBuildHistogramQueriesWithoutTypeOrNamespace(t *testing.T) {
	db, _ := NewMockProvider().UTInit()

	i := newIntervals(1)

	queries := db.buildHistogramQueries(
		"mytable",
		"created",
		"", 
		"", 
		"", 
		i,
		5,
	)

	assert.Len(t, queries, 1)

	sqlStr, args, err := queries[0].ToSql()
	assert.NoError(t, err)

	assert.Regexp(t,
		`SELECT created FROM mytable WHERE \(created >= .+ AND created < .+\) ORDER BY created LIMIT 5`,
		sqlStr,
	)

	assert.Len(t, args, 2)
	assert.NotNil(t, args[0])
	assert.NotNil(t, args[1])
}

func TestProcessHistogramRowsNoTypeColumn(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	ctx := context.Background()

	mock.ExpectQuery("SELECT.*FROM mytable").
		WillReturnRows(
			sqlmock.NewRows([]string{"created"}).
				AddRow("2024-01-01T00:00:00Z").
				AddRow("2024-01-02T00:00:00Z"),
		)

	r, _, e := db.Query(ctx, "mytable", sq.Select("created").From("mytable"))
	assert.NoError(t, e)
	defer r.Close()

	typeMap, total, e := db.processHistogramRows(ctx, "mytable", r, false)
	assert.NoError(t, e)
	assert.Equal(t, uint64(2), total)
	assert.Empty(t, typeMap)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessHistogramRowsWithTypeColumn(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	ctx := context.Background()

	mock.ExpectQuery("SELECT.*FROM mytable").
		WillReturnRows(
			sqlmock.NewRows([]string{"created", "type"}).
				AddRow("2024-01-01T00:00:00Z", "A").
				AddRow("2024-01-01T00:01:00Z", "B").
				AddRow("2024-01-01T00:02:00Z", "A"),
		)

	rows, _, e := db.Query(ctx, "mytable", sq.Select("created", "type").From("mytable"))
	assert.NoError(t, e)
	defer rows.Close()

	typeMap, total, e := db.processHistogramRows(ctx, "mytable", rows, true)
	assert.NoError(t, e)
	assert.Equal(t, uint64(3), total)
	assert.Equal(t, map[string]int{
		"A": 2,
		"B": 1,
	}, typeMap)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessHistogramRowsScanErrorwithoutTypeColumn(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	ctx := context.Background()

	mock.ExpectQuery("SELECT.*FROM mytable").
		WillReturnRows(
			sqlmock.NewRows([]string{"created", "extra"}).
				AddRow("2024-01-01T00:00:00Z", "x"),
		)

	r, _, err := db.Query(ctx, "mytable", sq.Select("created", "extra").From("mytable"))
	assert.NoError(t, err)
	defer r.Close()

	_, _, err = db.processHistogramRows(ctx, "mytable", r, false)
	assert.Error(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessHistogramRowsScanErrorWithTypeColumn(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	ctx := context.Background()

	mock.ExpectQuery("SELECT.*FROM mytable").
		WillReturnRows(
			sqlmock.NewRows([]string{"created"}).
				AddRow("2024-01-01T00:00:00Z"),
		)

	r, _, e := db.Query(ctx, "mytable", sq.Select("created").From("mytable"))
	assert.NoError(t, e)
	defer r.Close()

	_, _, e = db.processHistogramRows(ctx, "mytable", r, true)
	assert.Error(t, e)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetChartHistogramNoTypeColumn(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	ctx := context.Background()

	i := newIntervals(1)

	mock.ExpectQuery("SELECT.*FROM hist_table").
		WillReturnRows(
			sqlmock.NewRows([]string{"created"}).
				AddRow("2024-01-01T00:00:00Z").
				AddRow("2024-01-01T00:01:00Z").
				AddRow("2024-01-01T00:02:00Z"),
		)

	hist, err := db.GetChartHistogram(
		ctx,
		"hist_table",
		"created",
		"", 
		"", 
		"", 
		i,
	)
	assert.NoError(t, err)
	assert.Len(t, hist, 1)

	assert.Equal(t, "3", hist[0].Count)
	assert.Equal(t, i[0].StartTime, hist[0].Timestamp)
	assert.Empty(t, hist[0].Types)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetChartHistogramWithTypeColumn(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	ctx := context.Background()

	i := newIntervals(2)

	// two rows A type
	mock.ExpectQuery("SELECT.*FROM hist_table").
		WillReturnRows(
			sqlmock.NewRows([]string{"created", "type"}).
				AddRow("2024-01-01T00:00:00Z", "A").
				AddRow("2024-01-01T00:01:00Z", "A"),
		)

	// one A and one B
	mock.ExpectQuery("SELECT.*FROM hist_table").
		WillReturnRows(
			sqlmock.NewRows([]string{"created", "type"}).
				AddRow("2024-01-01T00:02:00Z", "A").
				AddRow("2024-01-01T00:03:00Z", "B"),
		)

	hist, err := db.GetChartHistogram(
		ctx,
		"hist_table",
		"created",
		"type",
		"", 
		"", 
		i,
	)
	assert.NoError(t, err)
	assert.Len(t, hist, 2)

	// only A
	assert.Equal(t, "2", hist[0].Count)
	assert.Equal(t, i[0].StartTime, hist[0].Timestamp)
	assert.Len(t, hist[0].Types, 1)
	assert.Equal(t, "A", hist[0].Types[0].Type)
	assert.Equal(t, "2", hist[0].Types[0].Count)

	// A=1 B=1
	assert.Equal(t, "2", hist[1].Count)
	assert.Equal(t, i[1].StartTime, hist[1].Timestamp)

	typeCounts := map[string]string{}
	for _, ty := range hist[1].Types {
		typeCounts[ty.Type] = ty.Count
	}
	assert.Equal(t, map[string]string{"A": "1", "B": "1"}, typeCounts)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetChartHistogramQueryError(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	ctx := context.Background()

	i := newIntervals(1)

	mock.ExpectQuery("SELECT.*FROM hist_table").
		WillReturnError(assert.AnError)

	_, err := db.GetChartHistogram(
		ctx,
		"hist_table",
		"created",
		"type",
		"", "",
		i,
	)
	assert.Error(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetChartHistogramProcessError(t *testing.T) {
	db, mock := NewMockProvider().UTInit()
	ctx := context.Background()

	i := newIntervals(1)

	mock.ExpectQuery("SELECT.*FROM hist_table").
		WillReturnRows(
			sqlmock.NewRows([]string{"created"}).
				AddRow("2024-01-01T00:00:00Z"),
		)

	_, e := db.GetChartHistogram(
		ctx,
		"hist_table",
		"created",
		"type",
		"", "",
		i,
	)
	assert.Error(t, e)

	assert.NoError(t, mock.ExpectationsWereMet())
}
