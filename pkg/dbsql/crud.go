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
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
)

const (
	ColumnID      = "id"
	ColumnCreated = "created"
	ColumnUpdated = "updated"
)

type ChangeEventType int

const (
	Created ChangeEventType = iota
	Updated
	Deleted
)

type UpsertOptimization int

const (
	UpsertOptimizationSkip UpsertOptimization = iota
	UpsertOptimizationNew
	UpsertOptimizationExisting
	UpsertOptimizationDB // only supported if the DB layer support ON CONFLICT semantics
)

type GetOption int

const (
	FailIfNotFound GetOption = iota
)

type PostCompletionHook func()

type Resource interface {
	GetID() string
	SetCreated(*fftypes.FFTime)
	SetUpdated(*fftypes.FFTime)
}

type ResourceSequence interface {
	SetSequence(int64)
}

// Resource is the default implementation of the Resource interface, but consumers of this
// package can implement the interface directly if they want to use different field names
// or field types for the fields.
type ResourceBase struct {
	ID      *fftypes.UUID   `ffstruct:"ResourceBase" json:"id"`
	Created *fftypes.FFTime `ffstruct:"ResourceBase" json:"created"`
	Updated *fftypes.FFTime `ffstruct:"ResourceBase" json:"updated"`
}

func (r *ResourceBase) GetID() string {
	return r.ID.String() // nil safe
}

func (r *ResourceBase) SetCreated(t *fftypes.FFTime) {
	r.Created = t
}

func (r *ResourceBase) SetUpdated(t *fftypes.FFTime) {
	r.Updated = t
}

type CRUDQuery[T Resource] interface {
	GetByID(ctx context.Context, id string, getOpts ...GetOption) (inst T, err error)
	GetByUUIDOrName(ctx context.Context, uuidOrName string, getOpts ...GetOption) (result T, err error)
	GetByName(ctx context.Context, name string, getOpts ...GetOption) (instance T, err error)
	GetFirst(ctx context.Context, filter ffapi.Filter, getOpts ...GetOption) (instance T, err error)
	GetSequenceForID(ctx context.Context, id string) (seq int64, err error)
	GetMany(ctx context.Context, filter ffapi.Filter) (instances []T, fr *ffapi.FilterResult, err error)
	Count(ctx context.Context, filter ffapi.Filter) (count int64, err error)
	ModifyQuery(modifier QueryModifier) CRUDQuery[T]
}

type CRUD[T Resource] interface {
	CRUDQuery[T]
	Validate()
	Upsert(ctx context.Context, inst T, optimization UpsertOptimization, hooks ...PostCompletionHook) (created bool, err error)
	InsertMany(ctx context.Context, instances []T, allowPartialSuccess bool, hooks ...PostCompletionHook) (err error)
	Insert(ctx context.Context, inst T, hooks ...PostCompletionHook) (err error)
	Replace(ctx context.Context, inst T, hooks ...PostCompletionHook) (err error)
	Update(ctx context.Context, id string, update ffapi.Update, hooks ...PostCompletionHook) (err error)
	UpdateSparse(ctx context.Context, sparseUpdate T, hooks ...PostCompletionHook) (err error)
	UpdateMany(ctx context.Context, filter ffapi.Filter, update ffapi.Update, hooks ...PostCompletionHook) (err error)
	Delete(ctx context.Context, id string, hooks ...PostCompletionHook) (err error)
	DeleteMany(ctx context.Context, filter ffapi.Filter, hooks ...PostCompletionHook) (err error) // no events
	NewFilterBuilder(ctx context.Context) ffapi.FilterBuilder
	NewUpdateBuilder(ctx context.Context) ffapi.UpdateBuilder
	GetQueryFactory() ffapi.QueryFactory
	TableAlias() string
	Scoped(scope sq.Eq) CRUD[T] // allows dynamic scoping to a collection
}

type CrudBase[T Resource] struct {
	DB               *Database
	Table            string
	Columns          []string
	FilterFieldMap   map[string]string
	TimesDisabled    bool // no management of the time columns
	PatchDisabled    bool // allows non-pointer fields, but prevents UpdateSparse function
	ImmutableColumns []string
	IDField          string                                        // override default ID field
	NameField        string                                        // If supporting name semantics
	QueryFactory     ffapi.QueryFactory                            // Must be set when name is set
	DefaultSort      func() []interface{}                          // optionally override the default sort - array of *ffapi.SortField or string
	IDValidator      func(ctx context.Context, idStr string) error // if IDs must conform to a pattern, such as a UUID (prebuilt UUIDValidator provided for that)

	NilValue     func() T // nil value typed to T
	NewInstance  func() T
	ScopedFilter func() sq.Eq
	EventHandler func(id string, eventType ChangeEventType)
	GetFieldPtr  func(inst T, col string) interface{}

	// Optional extensions
	ReadTableAlias    string
	ReadOnlyColumns   []string
	ReadQueryModifier QueryModifier
	AfterLoad         func(ctx context.Context, inst T) error // perform final validation/formatting after an instance is loaded from db
}

func (c *CrudBase[T]) Scoped(scope sq.Eq) CRUD[T] {
	cScoped := *c
	cScoped.ScopedFilter = func() sq.Eq { return scope }
	return &cScoped
}

func (c *CrudBase[T]) TableAlias() string {
	if c.ReadTableAlias != "" {
		return c.ReadTableAlias
	}
	return c.Table
}

func (c *CrudBase[T]) GetIDField() string {
	if c.IDField != "" {
		return c.IDField
	}
	return ColumnID
}

func (c *CrudBase[T]) GetQueryFactory() ffapi.QueryFactory {
	return c.QueryFactory
}

func (c *CrudBase[T]) NewFilterBuilder(ctx context.Context) ffapi.FilterBuilder {
	if c.QueryFactory == nil {
		return nil
	}
	return c.QueryFactory.NewFilter(ctx)
}

func (c *CrudBase[T]) NewUpdateBuilder(ctx context.Context) ffapi.UpdateBuilder {
	if c.QueryFactory == nil {
		return nil
	}
	return c.QueryFactory.NewUpdate(ctx)
}

func (c *CrudBase[T]) ModifyQuery(newModifier QueryModifier) CRUDQuery[T] {
	cModified := *c
	originalModifier := cModified.ReadQueryModifier
	cModified.ReadQueryModifier = func(sb sq.SelectBuilder) (_ sq.SelectBuilder, err error) {
		if originalModifier != nil {
			sb, err = originalModifier(sb)
		}
		if err == nil && newModifier != nil {
			sb, err = newModifier(sb)
		}
		return sb, err
	}
	return &cModified
}

func UUIDValidator(ctx context.Context, idStr string) error {
	_, err := fftypes.ParseUUID(ctx, idStr)
	return err
}

// Validate checks things that must be true about a CRUD collection using this framework.
// Intended for use in the unit tests of microservices (will exercise all the functions of the CrudBase):
// - the mandatory columns exist - id/created/updated
// - no column has the same name as the sequence column for the DB
// - a unique pointer is returned for each field column
// - the immutable columns exist
// - the other functions return valid data
func (c *CrudBase[T]) Validate() {
	inst := c.NewInstance()
	if isNil(inst) {
		panic("NewInstance() value must not be nil")
	}
	ptrs := map[string]interface{}{}
	fieldMap := map[string]bool{
		// Mandatory column checks
		c.GetIDField(): false,
	}
	if !c.TimesDisabled {
		fieldMap[ColumnCreated] = false
		fieldMap[ColumnUpdated] = false
	}
	for _, col := range c.Columns {
		if ok, set := fieldMap[col]; ok && set {
			panic(fmt.Sprintf("%s is a duplicated column", col))
		}

		fieldMap[col] = true
		if col == c.DB.sequenceColumn {
			panic(fmt.Sprintf("cannot have column named '%s'", c.DB.sequenceColumn))
		}
		fieldPtr := c.GetFieldPtr(inst, col)
		ptrVal := reflect.ValueOf(fieldPtr)
		if ptrVal.Kind() != reflect.Ptr || !isNil(ptrVal.Elem().Interface()) {
			if !c.PatchDisabled {
				panic(fmt.Sprintf("field %s does not seem to be a pointer type - prevents null-check for PATCH semantics functioning", col))
			}
		}
		ptrs[col] = fieldPtr
	}
	for col, set := range fieldMap {
		if !set {
			panic(fmt.Sprintf("mandatory column '%s' must be included in column list", col))
		}
	}
	if !isNil(c.NilValue()) {
		panic("NilValue() value must be nil")
	}
	if !isNil(c.GetFieldPtr(inst, fftypes.NewUUID().String())) {
		panic("GetFieldPtr() must return nil for unknown column")
	}
	if c.NameField != "" && c.QueryFactory == nil {
		panic("QueryFactory must be set when name semantics are enabled")
	}
}

func (c *CrudBase[T]) idField() string {
	if c.IDField != "" {
		return c.IDField
	}
	return ColumnID
}

func (c *CrudBase[T]) idFilter(id string) sq.Eq {
	filter := sq.Eq{}

	if c.ReadTableAlias != "" {
		filter[fmt.Sprintf("%s.%s", c.ReadTableAlias, c.GetIDField())] = id
	} else {
		filter[c.idField()] = id
	}
	if c.ScopedFilter != nil {
		// copy the fields from the scoped filter without modifying the original
		for k, v := range c.ScopedFilter() {
			filter[k] = v
		}
	}
	return filter
}

func (c *CrudBase[T]) isImmutable(col string) bool {
	for _, immutable := range append(c.ImmutableColumns, c.GetIDField(), ColumnCreated, ColumnUpdated, c.DB.sequenceColumn) {
		if col == immutable {
			return true
		}
	}
	return false
}

func (c *CrudBase[T]) buildUpdateList(_ context.Context, update sq.UpdateBuilder, inst T, includeNil bool) sq.UpdateBuilder {
	for _, col := range c.Columns {
		if !c.isImmutable(col) {
			value := c.getFieldValue(inst, col)
			if includeNil || !isNil(value) {
				update = update.Set(col, value)
			}
		}
	}
	if !c.TimesDisabled {
		update = update.Set(ColumnUpdated, fftypes.Now())
	}
	return update
}

func (c *CrudBase[T]) updateFromInstance(ctx context.Context, tx *TXWrapper, inst T, includeNil bool) (int64, error) {
	update := sq.Update(c.Table)
	if !c.TimesDisabled {
		inst.SetUpdated(fftypes.Now())
	}
	update = c.buildUpdateList(ctx, update, inst, includeNil)
	update = update.Where(c.idFilter(inst.GetID()))
	return c.DB.UpdateTx(ctx, c.Table, tx,
		update,
		func() {
			if c.EventHandler != nil {
				c.EventHandler(inst.GetID(), Updated)
			}
		})
}

func (c *CrudBase[T]) getFieldValue(inst T, col string) interface{} {
	// Validate() will have checked this is safe for microservices (as long as they use that at build time in their UTs)
	val := reflect.ValueOf(c.GetFieldPtr(inst, col)).Elem().Interface()
	// Primarily for debugging, we de-reference simple pointer type in fields
	switch vt := val.(type) {
	case *string:
		if vt != nil {
			val = *vt
		}
	case *int64:
		if vt != nil {
			val = *vt
		}
	case *bool:
		if vt != nil {
			val = *vt
		}
	}
	return val
}

func (c *CrudBase[T]) setInsertTimestamps(inst T, now *fftypes.FFTime) {
	if !c.TimesDisabled {
		inst.SetCreated(now)
		inst.SetUpdated(now)
	}
}

func (c *CrudBase[T]) attemptSetSequence(inst interface{}, seq int64) {
	if rs, ok := inst.(ResourceSequence); ok {
		rs.SetSequence(seq)
	}
}

func (c *CrudBase[T]) attemptInsert(ctx context.Context, tx *TXWrapper, inst T, requestConflictEmptyResult bool) (err error) {
	if c.IDValidator != nil {
		if err := c.IDValidator(ctx, inst.GetID()); err != nil {
			return err
		}
	}

	c.setInsertTimestamps(inst, fftypes.Now())
	insert := sq.Insert(c.Table).Columns(c.Columns...)
	values := make([]interface{}, len(c.Columns))
	for i, col := range c.Columns {
		values[i] = c.getFieldValue(inst, col)
	}
	insert = insert.Values(values...)
	seq, err := c.DB.InsertTxExt(ctx, c.Table, tx, insert,
		func() {
			if c.EventHandler != nil {
				c.EventHandler(inst.GetID(), Created)
			}
		}, requestConflictEmptyResult)
	if err == nil {
		c.attemptSetSequence(inst, seq)
	}
	return err
}

func (c *CrudBase[T]) Upsert(ctx context.Context, inst T, optimization UpsertOptimization, hooks ...PostCompletionHook) (created bool, err error) {
	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return false, err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	// First attempt the operation based on the optimization passed in.
	// The expectation is that the optimization will hit almost all of the time,
	// as only recovery paths require us to go down the un-optimized route.
	optimized := false
	switch {
	case optimization == UpsertOptimizationDB && c.DB.features.DBOptimizedUpsertBuilder != nil:
		optimized = true // the DB does the work here, so any failure is a straight failure
		created, err = c.dbOptimizedUpsert(ctx, tx, inst)
		if err != nil {
			return false, err
		}
	case optimization == UpsertOptimizationNew:
		opErr := c.attemptInsert(ctx, tx, inst, true /* we want a failure here we can progress past */)
		optimized = opErr == nil
		created = optimized
	default: // UpsertOptimizationExisting, or fallback if DB optimization requested
		rowsAffected, opErr := c.updateFromInstance(ctx, tx, inst, true /* full replace */)
		optimized = opErr == nil && rowsAffected == 1
	}

	if !optimized {
		// Do a select within the transaction to determine if the UUID already exists
		msgRows, _, err := c.DB.QueryTx(ctx, c.Table, tx,
			sq.Select(c.DB.sequenceColumn).
				From(c.Table).
				Where(c.idFilter(inst.GetID())),
		)
		if err != nil {
			return false, err
		}
		existing := msgRows.Next()
		msgRows.Close()

		if existing {
			// Replace the existing one
			if _, err = c.updateFromInstance(ctx, tx, inst, true /* full replace */); err != nil {
				return false, err
			}
		} else {
			// Get a useful error out of an insert attempt
			created = true
			if err = c.attemptInsert(ctx, tx, inst, false); err != nil {
				return false, err
			}
		}
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return created, c.DB.CommitTx(ctx, tx, autoCommit)
}

func (c *CrudBase[T]) dbOptimizedUpsert(ctx context.Context, tx *TXWrapper, inst T) (created bool, err error) {

	// Caller responsible for checking this is available before driving this path
	optimizedInsertBuilder := c.DB.provider.Features().DBOptimizedUpsertBuilder

	if c.IDValidator != nil {
		if err := c.IDValidator(ctx, inst.GetID()); err != nil {
			return false, err
		}
	}
	now := fftypes.Now()
	c.setInsertTimestamps(inst, now)

	values := make(map[string]driver.Value)
	updateCols := make([]string, 0, len(c.Columns))
	for _, col := range c.Columns {
		values[col] = c.getFieldValue(inst, col)
		if !c.isImmutable(col) {
			updateCols = append(updateCols, col)
		}
	}
	var rows *sql.Rows
	query, err := optimizedInsertBuilder(ctx, c.Table, c.GetIDField(), c.Columns, updateCols, ColumnCreated, values)
	if err == nil {
		rows, _, err = c.DB.RunAsQueryTx(ctx, c.Table, tx, query.PlaceholderFormat(c.DB.features.PlaceholderFormat))
	}
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if rows.Next() {
		var createTime fftypes.FFTime
		if err = rows.Scan(&createTime); err != nil {
			return false, i18n.NewError(ctx, i18n.MsgDBReadInsertTSFailed, err)
		}
		created = !createTime.Time().Before(*now.Time())
	}
	return created, nil

}

func (c *CrudBase[T]) InsertMany(ctx context.Context, instances []T, allowPartialSuccess bool, hooks ...PostCompletionHook) (err error) {

	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)
	if c.DB.Features().MultiRowInsert {
		if err := c.insertManyMultiRow(ctx, tx, instances, allowPartialSuccess); err != nil {
			return err
		}
	} else {
		for _, inst := range instances {
			err := c.attemptInsert(ctx, tx, inst, false)
			if err != nil {
				return err
			}
		}
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)

}

func (c *CrudBase[T]) insertManyMultiRow(ctx context.Context, tx *TXWrapper, instances []T, allowPartialSuccess bool) error {
	chunkSize := len(instances)
	if maxPlaceholders := c.DB.Features().MaxPlaceholders; maxPlaceholders > 0 && len(c.Columns) > 0 {
		chunkSize = maxPlaceholders / len(c.Columns)
	}

	allSequences := make([]int64, len(instances))
	for offset := 0; offset < len(instances); offset += chunkSize {
		end := offset + chunkSize
		if end > len(instances) {
			end = len(instances)
		}
		chunk := instances[offset:end]

		insert := sq.Insert(c.Table).Columns(c.Columns...)
		for _, inst := range chunk {
			c.setInsertTimestamps(inst, fftypes.Now())
			values := make([]interface{}, len(c.Columns))
			for i, col := range c.Columns {
				values[i] = c.getFieldValue(inst, col)
			}
			insert = insert.Values(values...)
		}

		sequences := allSequences[offset:end]
		err := c.DB.InsertTxRows(ctx, c.Table, tx, insert, func() {
			for _, inst := range chunk {
				if c.EventHandler != nil {
					c.EventHandler(inst.GetID(), Created)
				}
			}
		}, sequences, allowPartialSuccess)
		if err != nil {
			return err
		}
	}

	for i, seq := range allSequences {
		c.attemptSetSequence(instances[i], seq)
	}
	return nil
}

func (c *CrudBase[T]) Insert(ctx context.Context, inst T, hooks ...PostCompletionHook) (err error) {
	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	if err = c.attemptInsert(ctx, tx, inst, false); err != nil {
		return err
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)
}

func (c *CrudBase[T]) Replace(ctx context.Context, inst T, hooks ...PostCompletionHook) (err error) {
	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	rowsAffected, err := c.updateFromInstance(ctx, tx, inst, true /* full replace */)
	if err != nil {
		return err
	} else if rowsAffected < 1 {
		return i18n.NewError(ctx, i18n.MsgDBNoRowsAffected)
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)
}

func (c *CrudBase[T]) scanRow(ctx context.Context, cols []string, row *sql.Rows) (T, error) {
	inst := c.NewInstance()
	var seq int64
	fieldPointers := make([]interface{}, len(cols))
	fieldPointers[0] = &seq // The first column is alway the sequence
	for i, col := range cols {
		if i != 0 {
			fieldPointers[i] = c.GetFieldPtr(inst, col)
		}
	}
	err := row.Scan(fieldPointers...)
	if err != nil {
		return c.NilValue(), i18n.WrapError(ctx, err, i18n.MsgDBReadErr, c.Table)
	}
	c.attemptSetSequence(inst, seq)
	return inst, nil
}

func (c *CrudBase[T]) getReadCols(f *ffapi.FilterInfo) (tableFrom string, cols, readCols []string) {
	cols = append([]string{c.DB.SequenceColumn()}, c.Columns...)
	if c.ReadOnlyColumns != nil {
		cols = append(cols, c.ReadOnlyColumns...)
	}
	if f != nil && len(f.RequiredFields) > 0 {
		newCols := []string{cols[0] /* first column is always the sequence, and must be */}
		for _, requiredFieldName := range f.RequiredFields {
			requiredColName := c.FilterFieldMap[requiredFieldName]
			if requiredColName == "" {
				requiredColName = requiredFieldName
			}
			for i, col := range cols {
				if i > 0 /* idx==0 handled above */ && col == requiredColName {
					newCols = append(newCols, col)
				}
			}
		}
		cols = newCols
	}
	tableFrom = c.Table
	readCols = cols
	if c.ReadTableAlias != "" {
		tableFrom = fmt.Sprintf("%s AS %s", c.Table, c.ReadTableAlias)
		readCols = make([]string, len(cols))
		for i, col := range cols {
			if strings.Contains(col, ".") {
				readCols[i] = cols[i]
			} else {
				readCols[i] = fmt.Sprintf("%s.%s", c.ReadTableAlias, col)
			}
		}
	}
	return tableFrom, cols, readCols
}

func (c *CrudBase[T]) GetSequenceForID(ctx context.Context, id string) (seq int64, err error) {
	query := sq.Select(c.DB.SequenceColumn()).From(c.Table).Where(c.idFilter(id))
	rows, _, err := c.DB.Query(ctx, c.Table, query)
	if err != nil {
		return -1, err
	}
	defer rows.Close()
	if !rows.Next() {
		return -1, i18n.NewError(ctx, i18n.Msg404NoResult)
	}
	if err = rows.Scan(&seq); err != nil {
		return -1, i18n.WrapError(ctx, err, i18n.MsgDBReadErr, c.Table)
	}
	return seq, nil
}

func processGetOpts(ctx context.Context, getOpts []GetOption) (failNotFound bool, err error) {
	for _, o := range getOpts {
		switch o {
		case FailIfNotFound:
			failNotFound = true
		default:
			return false, i18n.NewError(ctx, i18n.MsgDBUnknownGetOption, o)
		}
	}
	return failNotFound, nil
}

func (c *CrudBase[T]) GetByID(ctx context.Context, id string, getOpts ...GetOption) (inst T, err error) {

	failNotFound, err := processGetOpts(ctx, getOpts)
	if err != nil {
		return c.NilValue(), err
	}

	tableFrom, cols, readCols := c.getReadCols(nil)
	query := sq.Select(readCols...).
		From(tableFrom).
		Where(c.idFilter(id))
	if c.ReadQueryModifier != nil {
		if query, err = c.ReadQueryModifier(query); err != nil {
			return c.NilValue(), err
		}
	}

	rows, _, err := c.DB.Query(ctx, c.Table, query)
	if err != nil {
		return c.NilValue(), err
	}
	defer rows.Close()

	if !rows.Next() {
		log.L(ctx).Debugf("%s '%s' not found", c.Table, id)
		if failNotFound {
			return c.NilValue(), i18n.NewError(ctx, i18n.Msg404NoResult)
		}
		return c.NilValue(), nil
	}

	inst, err = c.scanRow(ctx, cols, rows)
	if err != nil {
		return c.NilValue(), err
	}
	if c.AfterLoad != nil {
		return inst, c.AfterLoad(ctx, inst)
	}
	return inst, nil
}

func (c *CrudBase[T]) GetMany(ctx context.Context, filter ffapi.Filter) (instances []T, fr *ffapi.FilterResult, err error) {
	fi, err := filter.Finalize()
	if err != nil {
		return nil, nil, err
	}
	tableFrom, cols, readCols := c.getReadCols(fi)
	var preconditions []sq.Sqlizer
	if c.ScopedFilter != nil {
		preconditions = []sq.Sqlizer{c.ScopedFilter()}
	}
	return c.getManyScoped(ctx, tableFrom, fi, cols, readCols, preconditions)
}

// GetFirst returns a single match (like GetByID), but using a generic filter
func (c *CrudBase[T]) GetFirst(ctx context.Context, filter ffapi.Filter, getOpts ...GetOption) (instance T, err error) {
	failNotFound, err := processGetOpts(ctx, getOpts)
	if err != nil {
		return c.NilValue(), err
	}

	results, _, err := c.GetMany(ctx, filter.Limit(1))
	if err != nil {
		return c.NilValue(), err
	}
	if len(results) == 0 {
		if failNotFound {
			return c.NilValue(), i18n.NewError(ctx, i18n.Msg404NoResult)
		}
		return c.NilValue(), nil
	}

	return results[0], nil
}

// GetByName is a special case of GetFirst, for the designated name column
func (c *CrudBase[T]) GetByName(ctx context.Context, name string, getOpts ...GetOption) (instance T, err error) {
	if c.NameField == "" {
		return c.NilValue(), i18n.NewError(ctx, i18n.MsgCollectionNotConfiguredWithName, c.Table)
	}
	return c.GetFirst(ctx, c.QueryFactory.NewFilter(ctx).Eq(c.NameField, name), getOpts...)
}

// GetByUUIDOrName provides the following semantic, for resolving strings in a URL path to a resource that
// for convenience are able to be a UUID or a name of an object:
// - If the string is valid according to the IDValidator, we attempt a lookup by ID first
// - If not found by ID, or not a valid ID, then continue to find by name
//
// Note: The ID wins in the above logic. If resource1 has a name that matches the ID of resource2, then
//
//	resource2 will be returned (not resource1).
func (c *CrudBase[T]) GetByUUIDOrName(ctx context.Context, uuidOrName string, getOpts ...GetOption) (result T, err error) {
	validID := true
	if c.IDValidator != nil {
		idErr := c.IDValidator(ctx, uuidOrName)
		validID = idErr == nil
	}
	if validID {
		// Do a get with failure on not found - so that if this works,
		// we return the value that was retrieved (and don't need a
		// complex generic-friendly nil check).
		// Regardless of the error type, we continue to do a lookup by name
		result, err = c.GetByID(ctx, uuidOrName, FailIfNotFound)
		if err == nil {
			return result, nil
		}
	}
	return c.GetByName(ctx, uuidOrName, getOpts...)
}

func (c *CrudBase[T]) getManyScoped(ctx context.Context, tableFrom string, fi *ffapi.FilterInfo, cols, readCols []string, preconditions []sq.Sqlizer) (instances []T, fr *ffapi.FilterResult, err error) {
	defaultSort := []interface{}{
		&ffapi.SortField{Field: c.DB.sequenceColumn, Descending: true},
	}
	if c.DefaultSort != nil {
		defaultSort = c.DefaultSort()
	}
	query, fop, fi, err := c.DB.filterSelectFinalized(ctx, c.ReadTableAlias, sq.Select(readCols...).From(tableFrom), fi, c.FilterFieldMap,
		defaultSort, preconditions...)
	if err != nil {
		return nil, nil, err
	}
	if c.ReadQueryModifier != nil {
		if query, err = c.ReadQueryModifier(query); err != nil {
			return nil, nil, err
		}
	}

	rows, tx, err := c.DB.Query(ctx, c.Table, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	instances = []T{}
	for rows.Next() {
		inst, err := c.scanRow(ctx, cols, rows)
		if err != nil {
			return nil, nil, err
		}
		if c.AfterLoad != nil {
			err = c.AfterLoad(ctx, inst)
			if err != nil {
				return nil, nil, err
			}
		}
		instances = append(instances, inst)
	}
	log.L(ctx).Debugf("SQL<- GetMany(%s): %d", c.Table, len(instances))
	return instances, c.DB.QueryRes(ctx, c.Table, tx, fop, c.ReadQueryModifier, fi), err
}

func (c *CrudBase[T]) Count(ctx context.Context, filter ffapi.Filter) (count int64, err error) {
	var fop sq.Sqlizer
	fi, err := filter.Finalize()
	if err == nil {
		fop, err = c.DB.filterOp(ctx, c.Table, fi, c.FilterFieldMap)
	}
	if err != nil {
		return -1, err
	}

	if c.ScopedFilter != nil {
		fop = sq.And{
			c.ScopedFilter(),
			fop,
		}
	}
	return c.DB.CountQuery(ctx, c.Table, nil, fop, c.ReadQueryModifier, "*")
}

func (c *CrudBase[T]) Update(ctx context.Context, id string, update ffapi.Update, hooks ...PostCompletionHook) (err error) {
	return c.attemptUpdate(ctx, func(query sq.UpdateBuilder) (sq.UpdateBuilder, error) {
		return query.Where(sq.Eq{c.idField(): id}), nil
	}, update, true, hooks...)
}

// Thanks to the testify/assert package for this
func isNil(o interface{}) bool {
	if o == nil {
		return true
	}

	containsKind := func(kinds []reflect.Kind, kind reflect.Kind) bool {
		for i := 0; i < len(kinds); i++ {
			if kind == kinds[i] {
				return true
			}
		}
		return false
	}

	value := reflect.ValueOf(o)
	kind := value.Kind()
	isNillableKind := containsKind(
		[]reflect.Kind{
			reflect.Chan, reflect.Func,
			reflect.Interface, reflect.Map,
			reflect.Ptr, reflect.Slice},
		kind)

	if isNillableKind && value.IsNil() {
		return true
	}

	return false
}

func (c *CrudBase[T]) UpdateSparse(ctx context.Context, sparseUpdate T, hooks ...PostCompletionHook) (err error) {
	if c.PatchDisabled {
		return i18n.NewError(ctx, i18n.MsgDBPatchNotSupportedForCollection, c.Table)
	}

	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	updateCount, err := c.updateFromInstance(ctx, tx, sparseUpdate, false /* only non-nil fields */)
	if err != nil {
		return err
	}
	if updateCount < 1 {
		return i18n.NewError(ctx, i18n.MsgDBNoRowsAffected)
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)
}

func (c *CrudBase[T]) UpdateMany(ctx context.Context, filter ffapi.Filter, update ffapi.Update, hooks ...PostCompletionHook) (err error) {
	return c.attemptUpdate(ctx, func(query sq.UpdateBuilder) (sq.UpdateBuilder, error) {
		return c.DB.FilterUpdate(ctx, query, filter, c.FilterFieldMap)
	}, update, false, hooks...)
}

func (c *CrudBase[T]) attemptUpdate(ctx context.Context, filterFn func(sq.UpdateBuilder) (sq.UpdateBuilder, error), update ffapi.Update, requireUpdateCount bool, hooks ...PostCompletionHook) (err error) {
	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	baseQuery := sq.Update(c.Table)
	if c.ScopedFilter != nil {
		baseQuery = baseQuery.Where(c.ScopedFilter())
	}
	query, err := c.DB.BuildUpdate(baseQuery, update, c.FilterFieldMap)
	if err == nil {
		query, err = filterFn(query)
	}
	if !c.TimesDisabled {
		query = query.Set(ColumnUpdated, fftypes.Now())
	}
	if err != nil {
		return err
	}

	updateCount, err := c.DB.UpdateTx(ctx, c.Table, tx, query, nil /* no change events filter based update */)
	if err != nil {
		return err
	}

	if requireUpdateCount && updateCount < 1 {
		return i18n.NewError(ctx, i18n.MsgDBNoRowsAffected)
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)
}

func (c *CrudBase[T]) Delete(ctx context.Context, id string, hooks ...PostCompletionHook) (err error) {

	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	err = c.DB.DeleteTx(ctx, c.Table, tx, sq.Delete(c.Table).Where(
		c.idFilter(id),
	), func() {
		if c.EventHandler != nil {
			c.EventHandler(id, Deleted)
		}
	})
	if err != nil {
		return err
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)

}

func (c *CrudBase[T]) DeleteMany(ctx context.Context, filter ffapi.Filter, hooks ...PostCompletionHook) (err error) {

	ctx, tx, autoCommit, err := c.DB.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer c.DB.RollbackTx(ctx, tx, autoCommit)

	var fop sq.Sqlizer
	fi, err := filter.Finalize()
	if err == nil {
		fop, err = c.DB.filterOp(ctx, c.Table, fi, c.FilterFieldMap)
	}
	if err != nil {
		return err
	}

	if c.ScopedFilter != nil {
		fop = sq.And{
			c.ScopedFilter(),
			fop,
		}
	}
	err = c.DB.DeleteTx(ctx, c.Table, tx, sq.Delete(c.Table).Where(fop), nil /* no event hooks support for DeleteMany */)
	if err != nil && err != fftypes.DeleteRecordNotFound /* no entries is fine */ {
		return err
	}

	for _, hook := range hooks {
		tx.AddPostCommitHook(hook)
	}

	return c.DB.CommitTx(ctx, tx, autoCommit)

}
