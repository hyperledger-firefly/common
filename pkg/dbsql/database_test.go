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
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type testPreCommitAccumulator struct {
	testFunc func(ctx context.Context, tx *TXWrapper) error
}

func (t *testPreCommitAccumulator) PreCommit(ctx context.Context, tx *TXWrapper) error {
	return t.testFunc(ctx, tx)
}

func TestInitDatabaseMissingProvider(t *testing.T) {
	s := &Database{}
	err := s.Init(context.Background(), nil, nil)
	assert.Regexp(t, "FF00173", err)
}

func TestInitDatabaseMissingURL(t *testing.T) {
	s := &Database{}
	tp := NewMockProvider()
	s.InitConfig(tp, tp.config)
	tp.config.Set("url", "")
	err := s.Init(context.Background(), tp, tp.config)
	assert.Regexp(t, "FF00183.*url", err)
}

func TestInitDatabaseConnsAndSeqCol(t *testing.T) {
	s := &Database{}
	tp := NewMockProvider()
	s.InitConfig(tp, tp.config)
	err := s.Init(context.Background(), tp, tp.config)
	assert.NoError(t, err)
	assert.NotNil(t, s.DB())

	assert.Equal(t, 10, s.ConnLimit())
	assert.Equal(t, "seq", s.SequenceColumn())
}

func TestInitDatabaseFeatures(t *testing.T) {
	s := &Database{}
	tp := NewMockProvider()
	s.InitConfig(tp, tp.config)
	err := s.Init(context.Background(), tp, tp.config)
	assert.NoError(t, err)
	assert.NotNil(t, s.DB())

	assert.NotNil(t, s.Features())
	assert.Equal(t, true, s.Features().UseILIKE)
	assert.Equal(t, false, s.Features().MultiRowInsert)
}

func TestInitDatabaseGetDatabaseNameFailed(t *testing.T) {
	mp := NewMockProvider()
	mp.GetDatabaseNameError = fmt.Errorf("pop")
	err := mp.Database.Init(context.Background(), mp, mp.config)
	assert.Regexp(t, "FF00173.*pop", err)
}

func TestInitDatabaseMaxPlaceholdersProviderDefault(t *testing.T) {
	mp := NewMockProvider()
	mp.MaxPlaceholders = 65535
	mp.Database.InitConfig(mp, mp.config)
	err := mp.Database.Init(context.Background(), mp, mp.config)
	assert.NoError(t, err)
	assert.Equal(t, 65535, mp.Database.Features().MaxPlaceholders)
}

func TestInitDatabaseOpenFailed(t *testing.T) {
	mp := NewMockProvider()
	mp.OpenError = fmt.Errorf("pop")
	err := mp.Database.Init(context.Background(), mp, mp.config)
	assert.Regexp(t, "FF00173.*pop", err)
}

func TestInitDatabaseMigrationOpenFailed(t *testing.T) {
	mp := NewMockProvider()
	mp.config.Set(SQLConfMigrationsAuto, true)
	mp.GetMigrationDriverError = fmt.Errorf("pop")
	err := mp.Database.Init(context.Background(), mp, mp.config)
	assert.Regexp(t, "FF00184.*pop", err)
}

func TestInitDatabaseMigrationOk(t *testing.T) {
	mp := NewMockProvider()
	defer mp.Close()
	mr := metric.NewPrometheusMetricsRegistry("ut")
	err := EnableDBMetrics(context.Background(), mr)
	require.NoError(t, err)
	defer func() { metricsRegistry = nil; metricsManager = nil }()
	mp.mmg.On("Lock").Return(nil)
	mp.mmg.On("Version").Return(-1, false, nil)
	mp.mmg.On("SetVersion", 1, true).Return(nil)
	mp.mmg.On("SetVersion", 2, true).Return(nil)
	mp.mmg.On("SetVersion", 3, true).Return(nil)
	mp.mmg.On("Run", mock.Anything).Return(nil)
	mp.mmg.On("SetVersion", 1, false).Return(nil)
	mp.mmg.On("SetVersion", 2, false).Return(nil)
	mp.mmg.On("SetVersion", 3, false).Return(nil)
	mp.mmg.On("Unlock").Return(nil)
	mp.config.Set(SQLConfMigrationsAuto, true)
	mp.config.Set(SQLConfMigrationsDirectory, "../../test/dbmigrations")
	err = mp.Database.Init(context.Background(), mp, mp.config)
	assert.NoError(t, err)
}

func TestInitDatabaseMigrationFailed(t *testing.T) {
	mp := NewMockProvider()
	defer mp.Close()
	mp.mmg.On("Lock").Return(nil)
	mp.mmg.On("Version").Return(-1, false, nil)
	mp.mmg.On("SetVersion", 1, true).Return(nil)
	mp.mmg.On("Run", mock.Anything).Return(fmt.Errorf("pop"))
	mp.mmg.On("SetVersion", 1, false).Return(nil)
	mp.mmg.On("Unlock").Return(nil)
	mp.config.Set(SQLConfMigrationsAuto, true)
	mp.config.Set(SQLConfMigrationsDirectory, "../../test/dbmigrations")
	err := mp.Database.Init(context.Background(), mp, mp.config)
	assert.Regexp(t, "FF00184", err)
}

func TestQueryTxBadSQL(t *testing.T) {
	tp := NewMockProvider()
	_, _, err := tp.QueryTx(context.Background(), "table1", nil, sq.SelectBuilder{})
	assert.Regexp(t, "FF00174", err)
}

func TestQueryNoTxOk(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	mp, mdb := NewMockProvider().UTInit()
	mdb.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{"seq"}).AddRow(12345))
	f := TestQueryFactory.NewFilter(context.Background()).Eq("id", "12345")
	sel, _, _, err := mp.FilterSelect(context.Background(), "table1", sq.Select("*").From("table1"), f, map[string]string{}, nil)
	assert.NoError(t, err)
	_, _, err = mp.Query(context.Background(), "table1", sel)
	assert.NoError(t, err)
}

func TestQueryNoTxErr(t *testing.T) {
	mp, mdb := NewMockProvider().UTInit()
	mdb.ExpectQuery("SELECT.*").WillReturnError(fmt.Errorf("pop"))
	f := TestQueryFactory.NewFilter(context.Background()).Eq("id", "12345")
	sel, _, _, err := mp.FilterSelect(context.Background(), "table1", sq.Select("*").From("table1"), f, map[string]string{}, nil)
	assert.NoError(t, err)
	_, _, err = mp.Query(context.Background(), "table1", sel)
	assert.Regexp(t, "FF00176", err)
}

func TestInsertTxPostgreSQLReturnedSyntax(t *testing.T) {
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectQuery("INSERT.*").WillReturnRows(sqlmock.NewRows([]string{s.SequenceColumn()}).AddRow(12345))
	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
	assert.NoError(t, err)
	s.FakePSQLInsert = true
	sb := sq.Insert("table").Columns("col1").Values(("val1"))
	sequence, err := s.InsertTx(ctx, "table1", tx, sb, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), sequence)
}

func TestInsertFailMultipleReturnPartialResult(t *testing.T) {
	mp := NewMockProvider()
	mp.MultiRowInsert = true
	s, mdb := mp.UTInit()
	mdb.ExpectBegin()
	mdb.ExpectQuery("INSERT.*").WillReturnRows(sqlmock.NewRows([]string{s.SequenceColumn()}))
	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
	assert.NoError(t, err)
	s.FakePSQLInsert = true
	sb := sq.Insert("table").Columns("col1").Values(("val1"))
	_, err = s.InsertTxExt(ctx, "table1", tx, sb, nil, true)
	assert.Regexp(t, "FF00177", err)
}

func TestInsertTxPostgreSQLReturnedSyntaxFail(t *testing.T) {
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectQuery("INSERT.*").WillReturnError(fmt.Errorf("pop"))
	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
	assert.NoError(t, err)
	s.FakePSQLInsert = true
	sb := sq.Insert("table").Columns("col1").Values(("val1"))
	_, err = s.InsertTx(ctx, "table1", tx, sb, nil)
	assert.Regexp(t, "FF00177", err)
}

func TestInsertTxOkPreAndPostCommit(t *testing.T) {
	mp, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectExec("INSERT.*").WillReturnResult(sqlmock.NewResult(12345, 1))
	mdb.ExpectCommit()

	ctx, tx, autoCommit, err := mp.BeginOrUseTx(context.Background())
	assert.NoError(t, err)

	preCalled := false
	tx.SetPreCommitAccumulator(&testPreCommitAccumulator{
		testFunc: func(ctx context.Context, tx *TXWrapper) error {
			preCalled = true
			return nil
		},
	})

	q := sq.Insert("table1").Columns("c1").Values("v1")

	postCalled := false
	seq, err := mp.InsertTx(ctx, "table1", tx, q, func() {
		postCalled = true
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), seq)

	err = mp.CommitTx(ctx, tx, autoCommit)
	assert.NoError(t, err)

	assert.True(t, preCalled)
	assert.True(t, postCalled)
}

func TestInsertTxFail(t *testing.T) {
	mp, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectExec("INSERT.*").WillReturnError(fmt.Errorf("pop"))

	ctx, tx, autoCommit, err := mp.BeginOrUseTx(context.Background())
	assert.NoError(t, err)

	q := sq.Insert("table1").Columns("c1").Values("v1")

	pcCalled := false
	seq, err := mp.InsertTx(ctx, "table1", tx, q, func() {
		pcCalled = true
	})
	assert.Regexp(t, "FF00177", err)
	assert.Equal(t, int64(-1), seq)

	mp.RollbackTx(ctx, tx, autoCommit)

	assert.False(t, pcCalled)
	mdb.ExpectationsWereMet()
}

func TestInsertTxBadSQL(t *testing.T) {
	s, _ := NewMockProvider().UTInit()
	_, err := s.InsertTx(context.Background(), "table1", nil, sq.InsertBuilder{}, nil)
	assert.Regexp(t, "FF00174", err)
}

func TestUpdateTxBadSQL(t *testing.T) {
	s, _ := NewMockProvider().UTInit()
	_, err := s.UpdateTx(context.Background(), "table1", nil, sq.UpdateBuilder{}, nil)
	assert.Regexp(t, "FF00174", err)
}

func TestDeleteTxBadSQL(t *testing.T) {
	s, _ := NewMockProvider().UTInit()
	err := s.DeleteTx(context.Background(), "table1", nil, sq.DeleteBuilder{}, nil)
	assert.Regexp(t, "FF00174", err)
}

func TestDeleteTxZeroRowsAffected(t *testing.T) {
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectExec("DELETE.*").WillReturnResult(driver.ResultNoRows)
	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
	assert.NoError(t, err)
	s.FakePSQLInsert = true
	sb := sq.Delete("table")
	err = s.DeleteTx(ctx, "table1", tx, sb, nil)
	assert.Regexp(t, "FF00167", err)
}

func TestDeleteTxFail(t *testing.T) {
	mp, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectExec("DELETE.*").WillReturnError(fmt.Errorf("pop"))

	ctx, tx, autoCommit, err := mp.BeginOrUseTx(context.Background())
	assert.NoError(t, err)

	q := sq.Delete("table1")

	err = mp.DeleteTx(ctx, "table1", tx, q, nil)
	assert.Regexp(t, "FF00179", err)

	mp.RollbackTx(ctx, tx, autoCommit)

}

func TestDeleteTxPostCommitOk(t *testing.T) {
	mp, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectExec("DELETE.*").WillReturnResult(sqlmock.NewResult(-1, 1))
	mdb.ExpectCommit()

	ctx, tx, autoCommit, err := mp.BeginOrUseTx(context.Background())
	assert.NoError(t, err)

	q := sq.Delete("table1")

	pcCalled := false
	err = mp.DeleteTx(ctx, "table1", tx, q, func() { pcCalled = true })
	assert.NoError(t, err)

	err = mp.CommitTx(ctx, tx, autoCommit)
	assert.NoError(t, err)

	mdb.ExpectationsWereMet()
	assert.True(t, pcCalled)

}

func TestUpdateTxPostCommitOk(t *testing.T) {
	mp, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectExec("UPDATE.*").WillReturnResult(sqlmock.NewResult(12345, 1))
	mdb.ExpectCommit()

	ctx, tx, autoCommit, err := mp.BeginOrUseTx(context.Background())
	assert.NoError(t, err)

	q := sq.Update("table1").Set("c1", "v1")

	pcCalled := false
	seq, err := mp.UpdateTx(ctx, "table1", tx, q, func() { pcCalled = true })
	assert.NoError(t, err)
	assert.Equal(t, int64(1), seq)

	err = mp.CommitTx(ctx, tx, autoCommit)
	assert.NoError(t, err)

	mdb.ExpectationsWereMet()
	assert.True(t, pcCalled)

}

func TestUpdateTxPostCommitErr(t *testing.T) {
	mp, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectExec("UPDATE.*").WillReturnError(fmt.Errorf("pop"))
	mdb.ExpectCommit()

	ctx, tx, autoCommit, err := mp.BeginOrUseTx(context.Background())
	assert.NoError(t, err)

	q := sq.Update("table1").Set("c1", "v1")

	pcCalled := false
	_, err = mp.UpdateTx(ctx, "table1", tx, q, func() { pcCalled = true })
	assert.Regexp(t, "FF00178", err)

	mp.RollbackTx(ctx, tx, autoCommit)

	mdb.ExpectationsWereMet()
	assert.False(t, pcCalled)

}

func TestCommitTxPreCommitFail(t *testing.T) {
	mp, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectCommit()

	ctx, tx, autoCommit, err := mp.BeginOrUseTx(context.Background())
	assert.NoError(t, err)

	tx.SetPreCommitAccumulator(&testPreCommitAccumulator{
		testFunc: func(ctx context.Context, tx *TXWrapper) error {
			return fmt.Errorf("pop")
		},
	})
	assert.NotNil(t, tx.PreCommitAccumulator())

	err = mp.CommitTx(ctx, tx, autoCommit)
	assert.Regexp(t, "pop", err)

	mdb.ExpectationsWereMet()

}

func TestAquireLockTx(t *testing.T) {
	mp, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectExec("acquire lock table1").WillReturnResult(sqlmock.NewResult(12345, 1))
	mdb.ExpectCommit()

	ctx, tx, autoCommit, err := mp.BeginOrUseTx(context.Background())
	assert.NoError(t, err)

	err = mp.AcquireLockTx(ctx, "table1", tx)
	assert.NoError(t, err)

	err = mp.CommitTx(ctx, tx, autoCommit)
	assert.NoError(t, err)

	mdb.ExpectationsWereMet()

}

func TestAquireLockTxFail(t *testing.T) {
	mp, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectExec("acquire lock table1").WillReturnError(fmt.Errorf("pop"))
	mdb.ExpectCommit()

	ctx, tx, _, err := mp.BeginOrUseTx(context.Background())
	assert.NoError(t, err)

	err = mp.AcquireLockTx(ctx, "table1", tx)
	assert.Regexp(t, "FF00187", err)

	mdb.ExpectationsWereMet()

}

func TestRunAsGroup(t *testing.T) {
	s, mock := NewMockProvider().UTInit()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT.*").WillReturnResult(driver.ResultNoRows)
	mock.ExpectExec("INSERT.*").WillReturnResult(driver.ResultNoRows)
	mock.ExpectQuery("SELECT.*").WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectCommit()

	err := s.RunAsGroup(context.Background(), func(ctx context.Context) (err error) {
		// First insert
		ctx, tx, ac, err := s.BeginOrUseTx(ctx)
		assert.NoError(t, err)
		_, err = s.InsertTx(ctx, "table1", tx, sq.Insert("test").Columns("test").Values("test"), nil)
		assert.NoError(t, err)
		err = s.CommitTx(ctx, tx, ac)
		assert.NoError(t, err)

		// Second insert
		ctx, tx, ac, err = s.BeginOrUseTx(ctx)
		assert.NoError(t, err)
		_, err = s.InsertTx(ctx, "table1", tx, sq.Insert("test").Columns("test").Values("test"), nil)
		assert.NoError(t, err)
		err = s.CommitTx(ctx, tx, ac)
		assert.NoError(t, err)

		// Query, not specifying a transaction
		_, _, err = s.Query(ctx, "table1", sq.Select("test").From("test"))
		assert.NoError(t, err)

		// Nested call
		err = s.RunAsGroup(ctx, func(ctx2 context.Context) error {
			assert.Equal(t, ctx, ctx2)
			return nil
		})
		assert.NoError(t, err)

		return
	})

	assert.NoError(t, mock.ExpectationsWereMet())
	assert.NoError(t, err)
}

func TestRunAsGroupBeginFail(t *testing.T) {
	s, mock := NewMockProvider().UTInit()
	mock.ExpectBegin().WillReturnError(fmt.Errorf("pop"))
	err := s.RunAsGroup(context.Background(), func(ctx context.Context) (err error) {
		return
	})
	assert.NoError(t, mock.ExpectationsWereMet())
	assert.Regexp(t, "FF00175", err)
}

func TestRunAsGroupFunctionFails(t *testing.T) {
	s, mock := NewMockProvider().UTInit()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT.*").WillReturnResult(driver.ResultNoRows)
	mock.ExpectRollback()
	err := s.RunAsGroup(context.Background(), func(ctx context.Context) (err error) {
		ctx, tx, ac, err := s.BeginOrUseTx(ctx)
		assert.NoError(t, err)
		_, err = s.InsertTx(ctx, "table1", tx, sq.Insert("test").Columns("test").Values("test"), nil)
		assert.NoError(t, err)
		s.RollbackTx(ctx, tx, ac) // won't actually rollback
		assert.NoError(t, err)

		return fmt.Errorf("pop")
	})
	assert.NoError(t, mock.ExpectationsWereMet())
	assert.Regexp(t, "pop", err)
}

func TestRunAsGroupCommitFail(t *testing.T) {
	s, mock := NewMockProvider().UTInit()
	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(fmt.Errorf("pop"))
	err := s.RunAsGroup(context.Background(), func(ctx context.Context) (err error) {
		return
	})
	assert.NoError(t, mock.ExpectationsWereMet())
	assert.Regexp(t, "FF00180", err)
}

func TestRollbackFail(t *testing.T) {
	s, mock := NewMockProvider().UTInit()
	mock.ExpectBegin()
	tx, _ := s.db.Begin()
	mock.ExpectRollback().WillReturnError(fmt.Errorf("pop"))
	s.RollbackTx(context.Background(), &TXWrapper{sqlTX: tx}, false)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCountQueryBadSQL(t *testing.T) {
	s, _ := NewMockProvider().UTInit()
	_, err := s.CountQuery(context.Background(), "table1", nil, sq.Insert("wrong"), nil, "")
	assert.Regexp(t, "FF00174", err)
}

func TestCountQueryQueryFailed(t *testing.T) {
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectQuery("^SELECT COUNT\\(\\*\\)").WillReturnError(fmt.Errorf("pop"))
	_, err := s.CountQuery(context.Background(), "table1", nil, sq.Eq{"col1": "val1"}, nil, "")
	assert.Regexp(t, "FF00176.*pop", err)
}

func TestCountQueryScanFailTx(t *testing.T) {
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectQuery("^SELECT COUNT\\(\\*\\)").WillReturnRows(sqlmock.NewRows([]string{"col1"}).AddRow("not a number"))
	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
	assert.NoError(t, err)
	_, err = s.CountQuery(ctx, "table1", tx, sq.Eq{"col1": "val1"}, nil, "")
	assert.Regexp(t, "FF00182", err)
}

func TestCountQueryWithExpr(t *testing.T) {
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectQuery("^SELECT COUNT\\(DISTINCT key\\)").WillReturnRows(sqlmock.NewRows([]string{"col1"}).AddRow(10))
	_, err := s.CountQuery(context.Background(), "table1", nil, sq.Eq{"col1": "val1"}, nil, "DISTINCT key")
	assert.NoError(t, err)
	assert.NoError(t, mdb.ExpectationsWereMet())
}

func TestCountQueryWithModifier(t *testing.T) {
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectQuery("^SELECT COUNT\\(\\*\\)").WillReturnRows(sqlmock.NewRows([]string{"col1"}).AddRow(10))
	qm := func(sb sq.SelectBuilder) (sq.SelectBuilder, error) {
		return sb.Where(sq.Eq{"col1": "val1"}), nil
	}
	_, err := s.CountQuery(context.Background(), "table1", nil, sq.Eq{"col1": "val1"}, qm, "")
	assert.NoError(t, err)
	assert.NoError(t, mdb.ExpectationsWereMet())
}

func TestCountQueryWithModifierErr(t *testing.T) {
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectQuery("^SELECT COUNT\\(\\*\\)").WillReturnRows(sqlmock.NewRows([]string{"col1"}).AddRow(10))
	qm := func(sb sq.SelectBuilder) (sq.SelectBuilder, error) {
		return sb, fmt.Errorf("pop")
	}
	_, err := s.CountQuery(context.Background(), "table1", nil, sq.Eq{"col1": "val1"}, qm, "")
	assert.Regexp(t, "pop", err)
}

func TestQueryResSwallowError(t *testing.T) {
	s, _ := NewMockProvider().UTInit()
	res := s.QueryRes(context.Background(), "table1", nil, sq.Insert("wrong"), nil, &ffapi.FilterInfo{
		Count: true,
	})
	assert.Equal(t, int64(-1), *res.TotalCount)
}

func TestInsertTxRowsBadConfig(t *testing.T) {
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
	assert.NoError(t, err)
	s.FakePSQLInsert = false
	sb := sq.Insert("table").Columns("col1").Values(("val1"))
	err = s.InsertTxRows(ctx, "table1", tx, sb, nil, []int64{1, 2}, false)
	assert.Regexp(t, "FF00186", err)
}

func TestInsertTxRowsIncompleteReturn(t *testing.T) {
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectQuery("INSERT.*").WillReturnRows(sqlmock.NewRows([]string{s.SequenceColumn()}).AddRow(int64(1001)))
	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
	assert.NoError(t, err)
	s.FakePSQLInsert = true
	sb := sq.Insert("table").Columns("col1").Values(("val1"))
	err = s.InsertTxRows(ctx, "table1", tx, sb, nil, []int64{1, 2}, false)
	assert.Regexp(t, "FF00177", err)
}

func TestExecTxOk(t *testing.T) {
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectExec("INSERT.*").WillReturnResult(driver.ResultNoRows)
	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
	assert.NoError(t, err)
	q := sq.Insert("mytable").Columns("some").Values("thing")
	sqlQuery, args, err := q.PlaceholderFormat(s.features.PlaceholderFormat).ToSql()
	assert.NoError(t, err)
	res, err := s.ExecTx(ctx, "mytable", tx, sqlQuery, args)
	assert.NoError(t, err)
	assert.NotNil(t, res)
}

func TestExecTxFail(t *testing.T) {
	s, mdb := NewMockProvider().UTInit()
	mdb.ExpectBegin()
	mdb.ExpectExec("INSERT.*").WillReturnError(fmt.Errorf("pop"))
	ctx, tx, _, err := s.BeginOrUseTx(context.Background())
	assert.NoError(t, err)
	q := sq.Insert("mytable").Columns("some").Values("thing")
	sqlQuery, args, err := q.PlaceholderFormat(s.features.PlaceholderFormat).ToSql()
	assert.NoError(t, err)
	_, err = s.ExecTx(ctx, "mytable", tx, sqlQuery, args)
	assert.Regexp(t, "pop", err)
}
