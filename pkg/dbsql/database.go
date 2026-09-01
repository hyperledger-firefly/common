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
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/golang-migrate/migrate/v4"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"

	"github.com/golang-migrate/migrate/v4/database"
	// Import migrate file source
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type QueryModifier = func(sq.SelectBuilder) (sq.SelectBuilder, error)

type Database struct {
	db             *sql.DB
	provider       Provider
	features       SQLFeatures
	connLimit      int
	sequenceColumn string
}

// PreCommitAccumulator is a structure that can accumulate state during
// the transaction, then has a function that is called just before commit.
type PreCommitAccumulator interface {
	PreCommit(ctx context.Context, tx *TXWrapper) error
}

type txContextKey struct{}

type TXWrapper struct {
	sqlTX                *sql.Tx
	preCommitAccumulator PreCommitAccumulator
	postCommit           []func()
}

func (tx *TXWrapper) AddPostCommitHook(fn func()) {
	tx.postCommit = append(tx.postCommit, fn)
}

func (tx *TXWrapper) PreCommitAccumulator() PreCommitAccumulator {
	return tx.preCommitAccumulator
}

func (tx *TXWrapper) SetPreCommitAccumulator(pca PreCommitAccumulator) {
	tx.preCommitAccumulator = pca
}

func (s *Database) Init(ctx context.Context, provider Provider, config config.Section) (err error) {
	if provider == nil || config == nil || provider.Features().PlaceholderFormat == nil || provider.SequenceColumn() == "" {
		log.L(ctx).Errorf("Invalid SQL options from provider '%T'", provider)
		return i18n.NewError(ctx, i18n.MsgDBInitFailed)
	}
	s.provider = provider
	s.features = s.provider.Features()
	s.sequenceColumn = s.provider.SequenceColumn()

	if config.GetString(SQLConfDatasourceURL) == "" {
		return i18n.NewError(ctx, i18n.MsgMissingConfig, "url", fmt.Sprintf("database.%s", s.provider.Name()))
	}

	if s.db, err = provider.Open(config.GetString(SQLConfDatasourceURL)); err != nil {
		return i18n.WrapError(ctx, err, i18n.MsgDBInitFailed)
	}
	s.connLimit = config.GetInt(SQLConfMaxConnections)
	if s.connLimit > 0 {
		s.db.SetMaxOpenConns(s.connLimit)
		s.db.SetConnMaxIdleTime(config.GetDuration(SQLConfMaxConnIdleTime))
		maxIdleConns := config.GetInt(SQLConfMaxIdleConns)
		if maxIdleConns <= 0 {
			// By default we rely on the idle time, rather than a maximum number of conns to leave open
			maxIdleConns = s.connLimit
		}
		s.db.SetMaxIdleConns(maxIdleConns)
		s.db.SetConnMaxLifetime(config.GetDuration(SQLConfMaxConnLifetime))
	}

	if config.GetBool(SQLConfMigrationsAuto) {
		if err = s.applyDBMigrations(ctx, config, provider); err != nil {
			return i18n.WrapError(ctx, err, i18n.MsgDBMigrationFailed)
		}
	}

	return nil
}

func (s *Database) ConnLimit() int {
	return s.connLimit
}

func (s *Database) Features() SQLFeatures {
	return s.features
}

func (s *Database) SequenceColumn() string {
	return s.sequenceColumn
}

func (s *Database) RunAsGroup(ctx context.Context, fn func(ctx context.Context) error) error {
	if tx := GetTXFromContext(ctx); tx != nil {
		// transaction already exists - just continue using it
		return fn(ctx)
	}

	ctx, tx, _, err := s.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer s.RollbackTx(ctx, tx, false /* we _are_ the auto-committer */)

	if err = fn(ctx); err != nil {
		return err
	}

	return s.CommitTx(ctx, tx, false /* we _are_ the auto-committer */)
}

func (s *Database) applyDBMigrations(ctx context.Context, config config.Section, provider Provider) (err error) {
	var driver database.Driver
	providerClosable, isClosable := provider.(ProviderCloseableMigrationDriver)
	if isClosable {
		driver, err = providerClosable.GetMigrationDriverClosable(s.db)
		defer func() {
			_ = driver.Close()
		}()
	} else {
		driver, err = provider.GetMigrationDriver(s.db)
	}
	if err == nil {
		fileURL := "file://" + config.GetString(SQLConfMigrationsDirectory)
		log.L(ctx).Infof("Running migrations in: %s", fileURL)
		var m *migrate.Migrate
		m, err = migrate.NewWithDatabaseInstance(
			fileURL,
			provider.MigrationsDir(), driver)
		if err == nil {
			err = m.Up()
		}
		version, dirty, _ := m.Version()
		log.L(ctx).Infof("Migrations now at: v=%d dirty=%t", version, dirty)
	}
	if err != nil && err != migrate.ErrNoChange {
		return i18n.WrapError(ctx, err, i18n.MsgDBMigrationFailed)
	}
	return nil
}

func GetTXFromContext(ctx context.Context) *TXWrapper {
	ctxKey := txContextKey{}
	txi := ctx.Value(ctxKey)
	if txi != nil {
		if tx, ok := txi.(*TXWrapper); ok {
			return tx
		}
	}
	return nil
}

func (s *Database) BeginOrUseTx(ctx context.Context) (ctx1 context.Context, tx *TXWrapper, autoCommit bool, err error) {

	tx = GetTXFromContext(ctx)
	if tx != nil {
		// There is s transaction on the context already.
		// return existing with auto-commit flag, to prevent early commit
		return ctx, tx, true, nil
	}

	l := log.L(ctx).WithField("dbtx", fftypes.ShortID())
	ctx1 = log.WithLogger(ctx, l)
	before := time.Now()
	l.Tracef("SQL-> begin")
	sqlTX, err := s.db.BeginTx(ctx /* transaction should auto-rollback on context cancel */, nil /* default options */)
	if err != nil {
		return ctx1, nil, false, i18n.WrapError(ctx1, err, i18n.MsgDBBeginFailed)
	}
	tx = &TXWrapper{
		sqlTX: sqlTX,
	}
	ctx1 = context.WithValue(ctx1, txContextKey{}, tx)
	l.Debugf("SQL<- begin (%.2fms)", floatMillisSince(before))
	return ctx1, tx, false, err
}

func (s *Database) QueryTx(ctx context.Context, table string, tx *TXWrapper, q sq.SelectBuilder) (*sql.Rows, *TXWrapper, error) {
	if tx == nil {
		// If there is a transaction in the context, we should use it to provide consistency
		// in the read operations (read after insert for example).
		tx = GetTXFromContext(ctx)
	}
	return s.RunAsQueryTx(ctx, table, tx, q.PlaceholderFormat(s.features.PlaceholderFormat))
}

func (s *Database) RunAsQueryTx(ctx context.Context, table string, tx *TXWrapper, q sq.Sqlizer) (*sql.Rows, *TXWrapper, error) {

	l := log.L(ctx)
	sqlQuery, args, err := q.ToSql()
	if err != nil {
		return nil, tx, i18n.WrapError(ctx, err, i18n.MsgDBQueryBuildFailed)
	}

	before := time.Now()
	l.Tracef(`SQL-> query: %s (args: %+v)`, sqlQuery, args)
	var rows *sql.Rows
	if tx != nil {
		rows, err = tx.sqlTX.QueryContext(ctx, sqlQuery, args...)
	} else {
		rows, err = s.db.QueryContext(ctx, sqlQuery, args...)
	}
	if err != nil {
		l.Errorf(`SQL query failed: %s sql=[ %s ]`, err, sqlQuery)
		return nil, tx, i18n.WrapError(ctx, err, i18n.MsgDBQueryFailed)
	}
	l.Debugf(`SQL<- query %s (%.2fms)`, table, floatMillisSince(before))
	return rows, tx, nil
}

func (s *Database) Query(ctx context.Context, table string, q sq.SelectBuilder) (*sql.Rows, *TXWrapper, error) {
	return s.QueryTx(ctx, table, nil, q)
}

func (s *Database) CountQuery(ctx context.Context, table string, tx *TXWrapper, fop sq.Sqlizer, qm QueryModifier, countExpr string) (count int64, err error) {
	count = -1
	l := log.L(ctx)
	if tx == nil {
		// If there is a transaction in the context, we should use it to provide consistency
		// in the read operations (read after insert for example).
		tx = GetTXFromContext(ctx)
	}
	if countExpr == "" {
		countExpr = "*"
	}
	q := sq.Select(fmt.Sprintf("COUNT(%s)", countExpr)).From(table).Where(fop)
	if qm != nil {
		if q, err = qm(q); err != nil {
			return -1, err
		}
	}
	sqlQuery, args, err := q.PlaceholderFormat(s.features.PlaceholderFormat).ToSql()
	if err != nil {
		return count, i18n.WrapError(ctx, err, i18n.MsgDBQueryBuildFailed)
	}
	before := time.Now()
	l.Tracef(`SQL-> count query: %s (args: %+v)`, sqlQuery, args)
	var rows *sql.Rows
	if tx != nil {
		rows, err = tx.sqlTX.QueryContext(ctx, sqlQuery, args...)
	} else {
		rows, err = s.db.QueryContext(ctx, sqlQuery, args...)
	}
	if err != nil {
		l.Errorf(`SQL count query failed: %s sql=[ %s ]`, err, sqlQuery)
		return count, i18n.WrapError(ctx, err, i18n.MsgDBQueryFailed)
	}
	defer rows.Close()
	if rows.Next() {
		if err = rows.Scan(&count); err != nil {
			return count, i18n.WrapError(ctx, err, i18n.MsgDBReadErr, table)
		}
	}
	l.Debugf(`SQL<- count query %s: %d (%.2fms)`, table, count, floatMillisSince(before))
	return count, nil
}

func (s *Database) QueryRes(ctx context.Context, table string, tx *TXWrapper, fop sq.Sqlizer, qm QueryModifier, fi *ffapi.FilterInfo) *ffapi.FilterResult {
	fr := &ffapi.FilterResult{}
	if fi.Count {
		count, err := s.CountQuery(ctx, table, tx, fop, qm, fi.CountExpr)
		if err != nil {
			// Log, but continue
			log.L(ctx).Warnf("Unable to return count for query: %s", err)
		}
		fr.TotalCount = &count // could be -1 if the count extract fails - we still return the result
	}
	return fr
}

func (s *Database) InsertTx(ctx context.Context, table string, tx *TXWrapper, q sq.InsertBuilder, postCommit func()) (int64, error) {
	return s.InsertTxExt(ctx, table, tx, q, postCommit, false)
}

func (s *Database) InsertTxExt(ctx context.Context, table string, tx *TXWrapper, q sq.InsertBuilder, postCommit func(), requestConflictEmptyResult bool) (int64, error) {
	sequences := []int64{-1}
	err := s.InsertTxRows(ctx, table, tx, q, postCommit, sequences, requestConflictEmptyResult)
	return sequences[0], err
}

func (s *Database) InsertTxRows(ctx context.Context, table string, tx *TXWrapper, q sq.InsertBuilder, postCommit func(), sequences []int64, requestConflictEmptyResult bool) error {
	l := log.L(ctx)
	q, useQuery := s.provider.ApplyInsertQueryCustomizations(q, requestConflictEmptyResult)

	sqlQuery, args, err := q.PlaceholderFormat(s.features.PlaceholderFormat).ToSql()
	if err != nil {
		return i18n.WrapError(ctx, err, i18n.MsgDBQueryBuildFailed)
	}
	before := time.Now()
	l.Tracef(`SQL-> insert query: %s (args: %+v)`, sqlQuery, args)
	if useQuery {
		noInsert := false
		result, err := tx.sqlTX.QueryContext(ctx, sqlQuery, args...)
		for i := 0; i < len(sequences) && err == nil; i++ {
			if result.Next() {
				err = result.Scan(&sequences[i])
			} else {
				noInsert = true
				err = i18n.NewError(ctx, i18n.MsgDBNoSequence, i+1)
			}
		}
		if result != nil {
			result.Close()
		}
		if err != nil {
			if requestConflictEmptyResult && noInsert {
				l.Infof(`SQL insert returning partial result: %s`, err)
			} else {
				l.Errorf(`SQL insert failed (conflictEmptyRequested=%t) sql=[ %s ]: %s`, requestConflictEmptyResult, sqlQuery, err)
			}
			return i18n.WrapError(ctx, err, i18n.MsgDBInsertFailed)
		}
	} else {
		if len(sequences) > 1 {
			return i18n.WrapError(ctx, err, i18n.MsgDBMultiRowConfigError)
		}
		res, err := tx.sqlTX.ExecContext(ctx, sqlQuery, args...)
		if err != nil {
			l.Errorf(`SQL insert failed: %s sql=[ %s ]: %s`, err, sqlQuery, err)
			return i18n.WrapError(ctx, err, i18n.MsgDBInsertFailed)
		}
		sequences[0], _ = res.LastInsertId()
	}
	l.Debugf(`SQL<- insert %s sequences=%v (%.2fms)`, table, sequences, floatMillisSince(before))

	if postCommit != nil {
		tx.AddPostCommitHook(postCommit)
	}
	return nil
}

func (s *Database) DeleteTx(ctx context.Context, table string, tx *TXWrapper, q sq.DeleteBuilder, postCommit func()) error {
	l := log.L(ctx)
	sqlQuery, args, err := q.PlaceholderFormat(s.features.PlaceholderFormat).ToSql()
	if err != nil {
		return i18n.WrapError(ctx, err, i18n.MsgDBQueryBuildFailed)
	}
	before := time.Now()
	l.Tracef(`SQL-> delete query: %s args: %+v`, sqlQuery, args)
	res, err := tx.sqlTX.ExecContext(ctx, sqlQuery, args...)
	if err != nil {
		l.Errorf(`SQL delete failed: %s sql=[ %s ]: %s`, err, sqlQuery, err)
		return i18n.WrapError(ctx, err, i18n.MsgDBDeleteFailed)
	}
	ra, _ := res.RowsAffected()
	l.Debugf(`SQL<- delete %s affected=%d (%.2fms)`, table, ra, floatMillisSince(before))
	if ra < 1 {
		return fftypes.DeleteRecordNotFound
	}

	if postCommit != nil {
		tx.AddPostCommitHook(postCommit)
	}
	return nil
}

func (s *Database) ExecTx(ctx context.Context, table string, tx *TXWrapper, sqlQuery string, args []interface{}) (sql.Result, error) {
	l := log.L(ctx)
	before := time.Now()
	l.Tracef(`SQL-> exec: %s args: %+v`, sqlQuery, args)
	res, err := tx.sqlTX.ExecContext(ctx, sqlQuery, args...)
	if err != nil {
		l.Errorf(`SQL exec: %s sql=[ %s ]: %s`, err, sqlQuery, err)
		return nil, i18n.WrapError(ctx, err, i18n.MsgDBExecFailed)
	}
	ra, _ := res.RowsAffected()
	l.Debugf(`SQL<- exec: %s affected=%d (%.2fms)`, table, ra, floatMillisSince(before))
	return res, nil
}

func (s *Database) UpdateTx(ctx context.Context, table string, tx *TXWrapper, q sq.UpdateBuilder, postCommit func()) (int64, error) {
	l := log.L(ctx)
	sqlQuery, args, err := q.PlaceholderFormat(s.features.PlaceholderFormat).ToSql()
	if err != nil {
		return -1, i18n.WrapError(ctx, err, i18n.MsgDBQueryBuildFailed)
	}
	before := time.Now()
	l.Tracef(`SQL-> update query: %s (args: %+v)`, sqlQuery, args)
	res, err := tx.sqlTX.ExecContext(ctx, sqlQuery, args...)
	if err != nil {
		l.Errorf(`SQL update failed: %s sql=[ %s ]`, err, sqlQuery)
		return -1, i18n.WrapError(ctx, err, i18n.MsgDBUpdateFailed)
	}
	ra, _ := res.RowsAffected()
	l.Debugf(`SQL<- update %s affected=%d (%.2fms)`, table, ra, floatMillisSince(before))

	if postCommit != nil {
		tx.AddPostCommitHook(postCommit)
	}
	return ra, nil
}

func (s *Database) AcquireLockTx(ctx context.Context, lockName string, tx *TXWrapper) error {
	l := log.L(ctx)
	if s.features.AcquireLock != nil {
		sqlQuery := s.features.AcquireLock(lockName)

		before := time.Now()
		l.Tracef(`SQL-> lock %s`, lockName)
		_, err := tx.sqlTX.ExecContext(ctx, sqlQuery)
		if err != nil {
			l.Errorf(`SQL lock failed: %s sql=[ %s ]`, err, sqlQuery)
			return i18n.WrapError(ctx, err, i18n.MsgDBLockFailed)
		}
		l.Debugf(`SQL<- lock %s (%.2fms)`, lockName, floatMillisSince(before))
	}
	return nil
}

// RollbackTx be safely called as a defer, as it is a cheap no-op if the transaction is complete
func (s *Database) RollbackTx(ctx context.Context, tx *TXWrapper, autoCommit bool) {
	if autoCommit {
		// We're inside of a wide transaction boundary with an auto-commit
		return
	}

	err := tx.sqlTX.Rollback()
	if err == nil {
		log.L(ctx).Warnf("SQL! transaction rollback")
	}
	if err != nil && err != sql.ErrTxDone {
		log.L(ctx).Errorf(`SQL rollback failed: %s`, err)
	}
}

func (s *Database) CommitTx(ctx context.Context, tx *TXWrapper, autoCommit bool) error {
	if autoCommit {
		// We're inside of a wide transaction boundary with an auto-commit
		return nil
	}
	l := log.L(ctx)

	// Only at this stage do we write to the special events Database table, so we know
	// regardless of the higher level logic, the events are always written at this point
	// at the end of the transaction
	if tx.preCommitAccumulator != nil {
		if err := tx.preCommitAccumulator.PreCommit(ctx, tx); err != nil {
			s.RollbackTx(ctx, tx, false)
			return err
		}
	}

	before := time.Now()
	l.Tracef(`SQL-> commit`)
	err := tx.sqlTX.Commit()
	if err != nil {
		l.Errorf(`SQL commit failed: %s`, err)
		return i18n.WrapError(ctx, err, i18n.MsgDBCommitFailed)
	}
	l.Debugf(`SQL<- commit (%.2fms)`, floatMillisSince(before))

	// Emit any post commit events (these aren't currently allowed to cause errors)
	for i, pce := range tx.postCommit {
		l.Tracef(`-> post commit event %d`, i)
		pce()
		l.Tracef(`<- post commit event %d`, i)
	}

	return nil
}

func floatMillisSince(t time.Time) float64 {
	return (float64)(time.Since(t)) / (float64)(time.Millisecond)
}

func (s *Database) DB() *sql.DB {
	return s.db
}

func (s *Database) Close() {
	if s.db != nil {
		err := s.db.Close()
		log.L(context.Background()).Debugf("Database closed (err=%v)", err)
	}
}
