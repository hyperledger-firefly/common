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

package ffapi

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
)

type FilterModifiers[T any] interface {
	// Sort adds a set of sort conditions (all in a single sort order)
	Sort(...string) T

	// GroupBy adds a set of fields to group rows that have the same values into summary rows. Not assured every persistence implementation will support this (doc DBs cannot)
	GroupBy(...string) T

	// Ascending sort order
	Ascending() T

	// Descending sort order
	Descending() T

	// Skip for pagination
	Skip(uint64) T

	// Limit for pagination
	Limit(uint64) T

	// Request a count to be returned on the total number that match the query
	Count(c bool) T

	// Which fields we require to be returned. Only supported when using CRUD layer on top of underlying DB.
	// Might allow optimization of the query (in the case of SQL DBs, where it can be combined with GroupBy), or request post-query redaction (in the case of document DBs).
	RequiredFields(...string) T
}

// Filter is the output of the builder
type Filter interface {
	FilterModifiers[Filter]

	// Finalize completes the filter, and for the plugin to validated output structure to convert
	Finalize() (*FilterInfo, error)

	// Builder returns the builder that made it
	Builder() FilterBuilder

	// Assert that this is a value filter, not an and/or
	ValueFilter() ValueFilter
}

// ValueFilter is accessor functions for non-Or/And filters - for advanced traversal of the un-finalized tree
type ValueFilter interface {
	// The operation for this filter
	Op() FilterOp

	// The field name
	Field() string

	// The value
	Value() interface{}

	// Set the operation for this filter
	SetOp(op FilterOp)

	// Set the field name
	SetField(f string)

	// Set the value
	SetValue(v interface{})
}

// MultiConditionFilter gives convenience methods to add conditions
type MultiConditionFilter interface {
	Filter
	// Add adds filters to the condition
	Condition(...Filter) MultiConditionFilter
	GetConditions() []Filter
}

type AndFilter interface{ MultiConditionFilter }

type OrFilter interface{ MultiConditionFilter }

// FilterOp enum of filter operations that must be implemented by plugins - the string value is
// used in the core string formatting method (for logging etc.)
type FilterOp string

// The character pairs in this are not used anywhere externally, just in a to-string representation of queries
const (
	// FilterOpAnd and
	FilterOpAnd FilterOp = "&&"
	// FilterOpOr or
	FilterOpOr FilterOp = "||"
	// FilterOpEq equal
	FilterOpEq FilterOp = "=="
	// FilterOpIEq equal
	FilterOpIEq FilterOp = ":="
	// FilterOpNe not equal
	FilterOpNeq FilterOp = "!="
	// FilterOpNIeq not equal
	FilterOpNIeq FilterOp = ";="
	// FilterOpIn in list of values
	FilterOpIn FilterOp = "IN"
	// FilterOpNotIn not in list of values
	FilterOpNotIn FilterOp = "NI"
	// FilterOpGt greater than
	FilterOpGt FilterOp = ">>"
	// FilterOpLt less than
	FilterOpLt FilterOp = "<<"
	// FilterOpGte greater than or equal
	FilterOpGte FilterOp = ">="
	// FilterOpLte less than or equal
	FilterOpLte FilterOp = "<="
	// FilterOpCont contains the specified text, case sensitive
	FilterOpCont FilterOp = "%="
	// FilterOpNotCont does not contain the specified text, case sensitive
	FilterOpNotCont FilterOp = "!%"
	// FilterOpICont contains the specified text, case insensitive
	FilterOpICont FilterOp = ":%"
	// FilterOpNotICont does not contain the specified text, case insensitive
	FilterOpNotICont FilterOp = ";%"
	// FilterOpStartsWith contains the specified text, case sensitive
	FilterOpStartsWith FilterOp = "^="
	// FilterOpNotCont does not contain the specified text, case sensitive
	FilterOpNotStartsWith FilterOp = "!^"
	// FilterOpICont contains the specified text, case insensitive
	FilterOpIStartsWith FilterOp = ":^"
	// FilterOpNotICont does not contain the specified text, case insensitive
	FilterOpNotIStartsWith FilterOp = ";^"
	// FilterOpEndsWith contains the specified text, case sensitive
	FilterOpEndsWith FilterOp = "$="
	// FilterOpNotCont does not contain the specified text, case sensitive
	FilterOpNotEndsWith FilterOp = "!$"
	// FilterOpICont contains the specified text, case insensitive
	FilterOpIEndsWith FilterOp = ":$"
	// FilterOpNotICont does not contain the specified text, case insensitive
	FilterOpNotIEndsWith FilterOp = ";$"
)

func filterOpIsStringMatch(op FilterOp) bool {
	for _, r := range string(op) {
		switch r {
		case '%', '^', '$', ':':
			// Partial or case-insensitive matches all need a string
			return true
		}
	}
	return false
}

func filterCannotAcceptNull(op FilterOp) bool {
	for _, r := range string(op) {
		switch r {
		case '%', '^', '$', ':', '>', '<':
			// string based matching, or gt/lt cannot accept null
			return true
		}
	}
	return false
}

// FilterBuilder is the syntax used to build the filter, where And() and Or() can be nested
type FilterBuilder interface {
	FilterModifiers[FilterBuilder]

	// Fields is the list of available fields
	Fields() []string
	// And requires all sub-filters to match
	And(and ...Filter) AndFilter
	// Or requires any of the sub-filters to match
	Or(and ...Filter) OrFilter
	// Eq equal - case sensitive
	Eq(name string, value driver.Value) Filter
	// Neq not equal - case sensitive
	Neq(name string, value driver.Value) Filter
	// IEq equal - case insensitive
	IEq(name string, value driver.Value) Filter
	// INeq not equal - case insensitive
	NIeq(name string, value driver.Value) Filter
	// In one of an array of values
	In(name string, value []driver.Value) Filter
	// NotIn not one of an array of values
	NotIn(name string, value []driver.Value) Filter
	// Lt less than
	Lt(name string, value driver.Value) Filter
	// Gt greater than
	Gt(name string, value driver.Value) Filter
	// Gte greater than or equal
	Gte(name string, value driver.Value) Filter
	// Lte less than or equal
	Lte(name string, value driver.Value) Filter
	// Contains allows the string anywhere - case sensitive
	Contains(name string, value driver.Value) Filter
	// NotContains disallows the string anywhere - case sensitive
	NotContains(name string, value driver.Value) Filter
	// IContains allows the string anywhere - case insensitive
	IContains(name string, value driver.Value) Filter
	// INotContains disallows the string anywhere - case insensitive
	NotIContains(name string, value driver.Value) Filter
	// StartsWith allows the string at the start - case sensitive
	StartsWith(name string, value driver.Value) Filter
	// NotStartsWith disallows the string at the start - case sensitive
	NotStartsWith(name string, value driver.Value) Filter
	// IStartsWith allows the string at the start - case insensitive
	IStartsWith(name string, value driver.Value) Filter
	// NotIStartsWith disallows the string att the start - case insensitive
	NotIStartsWith(name string, value driver.Value) Filter
	// EndsWith allows the string at the end - case sensitive
	EndsWith(name string, value driver.Value) Filter
	// NotEndsWith disallows the string at the end - case sensitive
	NotEndsWith(name string, value driver.Value) Filter
	// IEndsWith allows the string at the end - case insensitive
	IEndsWith(name string, value driver.Value) Filter
	// NotIEndsWith disallows the string att the end - case insensitive
	NotIEndsWith(name string, value driver.Value) Filter
}

// NullBehavior specifies whether to sort nulls first or last in a query
type NullBehavior int

const (
	NullsDefault NullBehavior = iota
	NullsFirst
	NullsLast
)

// SortField is field+direction for sorting
type SortField struct {
	Field      string
	Descending bool
	Nulls      NullBehavior
}

// FilterInfo is the structure returned by Finalize to the plugin, to serialize this filter
// into the underlying database mechanism's filter language
type FilterInfo struct {
	GroupBy        []string
	RequiredFields []string
	Sort           []*SortField
	Skip           uint64
	Limit          uint64
	Count          bool
	CountExpr      string
	Field          string
	FieldMods      []FieldMod
	Op             FilterOp
	Values         []FieldSerialization
	Value          FieldSerialization
	Children       []*FilterInfo
}

// FilterResult is has additional info if requested on the query - currently only the total count
type FilterResult struct {
	TotalCount *int64
}

func ValueString(f FieldSerialization) string {
	v, _ := f.Value()
	switch tv := v.(type) {
	case nil:
		return fftypes.NullString
	case []byte:
		return fmt.Sprintf("'%s'", tv)
	case int64:
		return strconv.FormatInt(tv, 10)
	case bool:
		return fmt.Sprintf("%t", tv)
	default:
		return fmt.Sprintf("'%s'", tv)
	}
}

func (f *FilterInfo) filterString() string {
	fieldName := f.Field
	for _, fm := range f.FieldMods {
		if fm == FieldModLower {
			fieldName = fmt.Sprintf("lower(%s)", fieldName)
		}
	}
	switch f.Op {
	case FilterOpAnd, FilterOpOr:
		cs := make([]string, len(f.Children))
		for i, c := range f.Children {
			cs[i] = fmt.Sprintf("( %s )", c.filterString())
		}
		return strings.Join(cs, fmt.Sprintf(" %s ", f.Op))
	case FilterOpIn, FilterOpNotIn:
		strValues := make([]string, len(f.Values))
		for i, v := range f.Values {
			strValues[i] = ValueString(v)
		}
		return fmt.Sprintf("%s %s [%s]", fieldName, f.Op, strings.Join(strValues, ","))
	default:
		return fmt.Sprintf("%s %s %s", fieldName, f.Op, ValueString(f.Value))
	}
}

func (f *FilterInfo) String() string {

	var val strings.Builder

	val.WriteString(f.filterString())

	if len(f.GroupBy) > 0 {
		val.WriteString(fmt.Sprintf(" groupBy=%s", strings.Join(f.GroupBy, ",")))
	}
	if len(f.RequiredFields) > 0 {
		val.WriteString(fmt.Sprintf(" requiredFields=%s", strings.Join(f.RequiredFields, ",")))
	}
	if len(f.Sort) > 0 {
		fields := make([]string, len(f.Sort))
		for i, s := range f.Sort {
			if s.Descending {
				fields[i] = "-"
			}
			fields[i] += s.Field
		}
		val.WriteString(fmt.Sprintf(" sort=%s", strings.Join(fields, ",")))
	}
	if f.Skip > 0 {
		val.WriteString(fmt.Sprintf(" skip=%d", f.Skip))
	}
	if f.Limit > 0 {
		val.WriteString(fmt.Sprintf(" limit=%d", f.Limit))
	}
	if f.Count {
		val.WriteString(" count=true")
	}

	return val.String()
}

func (fb *filterBuilder) Fields() []string {
	keys := make([]string, len(fb.queryFields))
	i := 0
	for k := range fb.queryFields {
		keys[i] = k
		i++
	}
	return keys
}

type filterBuilder struct {
	ctx             context.Context
	queryFields     QueryFields
	sort            []*SortField
	groupBy         []string
	requiredFields  []string
	skip            uint64
	limit           uint64
	count           bool
	forceAscending  bool
	forceDescending bool
}

type baseFilter struct {
	fb       *filterBuilder
	children []Filter
	op       FilterOp
	field    string
	value    interface{}
}

func (f *baseFilter) Builder() FilterBuilder {
	return f.fb
}

func fieldMods(f Field) []FieldMod {
	if hfm, ok := f.(HasFieldMods); ok {
		return hfm.FieldMods()
	}
	return nil
}

func (f *baseFilter) ValueFilter() ValueFilter {
	return f
}

func (f *baseFilter) Op() FilterOp {
	return f.op
}

func (f *baseFilter) Field() string {
	return f.field
}

func (f *baseFilter) Value() interface{} {
	return f.value
}

func (f *baseFilter) SetOp(op FilterOp) {
	f.op = op
}

func (f *baseFilter) SetField(field string) {
	f.field = field
}

func (f *baseFilter) SetValue(value interface{}) {
	f.value = value
}

func (f *baseFilter) Finalize() (fi *FilterInfo, err error) {
	var children []*FilterInfo
	var value FieldSerialization
	var values []FieldSerialization
	var mods []FieldMod

	switch f.op {
	case FilterOpAnd, FilterOpOr:
		children = make([]*FilterInfo, len(f.children))
		for i, c := range f.children {
			if children[i], err = c.Finalize(); err != nil {
				return nil, err
			}
		}
	case FilterOpIn, FilterOpNotIn:
		fValues := f.value.([]driver.Value)
		values = make([]FieldSerialization, len(fValues))
		name := strings.ToLower(f.field)
		field, ok := f.fb.queryFields[name]
		if !ok {
			return nil, i18n.NewError(f.fb.ctx, i18n.MsgInvalidFilterField, name)
		}
		mods = fieldMods(field)
		for i, fv := range fValues {
			values[i] = field.GetSerialization()
			if err = values[i].Scan(fv); err != nil {
				return nil, i18n.WrapError(f.fb.ctx, err, i18n.MsgInvalidValueForFilterField, name)
			}
		}
	default:
		name := strings.ToLower(f.field)
		field, ok := f.fb.queryFields[name]
		if !ok {
			return nil, i18n.NewError(f.fb.ctx, i18n.MsgInvalidFilterField, name)
		}
		mods = fieldMods(field)
		skipScan := false
		switch f.value.(type) {
		case nil:
			if filterCannotAcceptNull(f.op) {
				return nil, i18n.NewError(f.fb.ctx, i18n.MsgFieldMatchNoNull, f.op, name)
			}
			value = &nullField{}
			skipScan = true
		case string:
			switch {
			case field.FilterAsString():
				// We need to have a stringField for the filter serialization, but in the case of StringFieldLower
				// (or other modified stringField types) we might still need to run transformations. So...
				normalFilter := field.GetSerialization()
				switch normalFilter.(type) {
				case *stringField:
					value = normalFilter
				default:
					// ... only switch to a default &stringField{} if the Serialization is not already a string
					value = &stringField{}
				}
			case filterOpIsStringMatch(f.op):
				return nil, i18n.NewError(f.fb.ctx, i18n.MsgFieldTypeNoStringMatching, name, field.Description())
			default:
				value = field.GetSerialization()
			}
		default:
			value = field.GetSerialization()
		}
		if !skipScan {
			if err = value.Scan(f.value); err != nil {
				return nil, i18n.WrapError(f.fb.ctx, err, i18n.MsgInvalidValueForFilterField, name)
			}
		}
	}

	if f.fb.forceDescending {
		for _, sf := range f.fb.sort {
			sf.Descending = true
		}
	} else if f.fb.forceAscending {
		for _, sf := range f.fb.sort {
			sf.Descending = false
		}
	}

	return &FilterInfo{
		Children:       children,
		Op:             f.op,
		Field:          f.field,
		FieldMods:      mods,
		Values:         values,
		Value:          value,
		Sort:           f.fb.sort,
		GroupBy:        f.fb.groupBy,
		RequiredFields: f.fb.requiredFields,
		Skip:           f.fb.skip,
		Limit:          f.fb.limit,
		Count:          f.fb.count,
	}, nil
}

func (fb *filterBuilder) Sort(fields ...string) FilterBuilder {
	for _, field := range fields {
		descending := false
		if strings.HasPrefix(field, "-") {
			field = strings.TrimPrefix(field, "-")
			descending = true
		}
		if _, ok := fb.queryFields[field]; ok {
			fb.sort = append(fb.sort, &SortField{
				Field:      field,
				Descending: descending,
			})
		}
	}
	return fb
}

func (f *baseFilter) Sort(fields ...string) Filter {
	_ = f.fb.Sort(fields...)
	return f
}

func (fb *filterBuilder) GroupBy(fields ...string) FilterBuilder {
	for _, field := range fields {
		if _, ok := fb.queryFields[field]; ok {
			fb.groupBy = append(fb.groupBy, field)
		}
	}
	return fb
}

func (f *baseFilter) GroupBy(fields ...string) Filter {
	_ = f.fb.GroupBy(fields...)
	return f
}

func (fb *filterBuilder) RequiredFields(fields ...string) FilterBuilder {
	for _, field := range fields {
		if _, ok := fb.queryFields[field]; ok {
			fb.requiredFields = append(fb.requiredFields, field)
		}
	}
	return fb
}

func (f *baseFilter) RequiredFields(fields ...string) Filter {
	_ = f.fb.RequiredFields(fields...)
	return f
}

func (fb *filterBuilder) Skip(skip uint64) FilterBuilder {
	fb.skip = skip
	return fb
}

func (f *baseFilter) Skip(skip uint64) Filter {
	_ = f.fb.Skip(skip)
	return f
}

func (fb *filterBuilder) Limit(limit uint64) FilterBuilder {
	fb.limit = limit
	return fb
}

func (f *baseFilter) Limit(limit uint64) Filter {
	_ = f.fb.Limit(limit)
	return f
}

func (fb *filterBuilder) Count(c bool) FilterBuilder {
	fb.count = c
	return fb
}

func (f *baseFilter) Count(c bool) Filter {
	_ = f.fb.Count(c)
	return f
}

func (fb *filterBuilder) Ascending() FilterBuilder {
	fb.forceAscending = true
	return fb
}

func (f *baseFilter) Ascending() Filter {
	_ = f.fb.Ascending()
	return f
}

func (f *baseFilter) Descending() Filter {
	_ = f.fb.Descending()
	return f
}

func (fb *filterBuilder) Descending() FilterBuilder {
	fb.forceDescending = true
	return fb
}

type andFilter struct {
	baseFilter
}

func (fb *andFilter) GetConditions() []Filter {
	return fb.children
}

func (fb *andFilter) Condition(children ...Filter) MultiConditionFilter {
	fb.children = append(fb.children, children...)
	return fb
}

func (fb *filterBuilder) And(and ...Filter) AndFilter {
	return &andFilter{
		baseFilter: baseFilter{
			fb:       fb,
			op:       FilterOpAnd,
			children: and,
		},
	}
}

type orFilter struct {
	baseFilter
}

func (fb *orFilter) GetConditions() []Filter {
	return fb.children
}

func (fb *orFilter) Condition(children ...Filter) MultiConditionFilter {
	fb.children = append(fb.children, children...)
	return fb
}

func (fb *filterBuilder) Or(or ...Filter) OrFilter {
	return &orFilter{
		baseFilter: baseFilter{
			fb:       fb,
			op:       FilterOpOr,
			children: or,
		},
	}
}

func (fb *filterBuilder) Eq(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpEq, name, value)
}

func (fb *filterBuilder) Neq(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpNeq, name, value)
}

func (fb *filterBuilder) IEq(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpIEq, name, value)
}

func (fb *filterBuilder) NIeq(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpNIeq, name, value)
}

func (fb *filterBuilder) In(name string, values []driver.Value) Filter {
	return fb.fieldFilter(FilterOpIn, name, values)
}

func (fb *filterBuilder) NotIn(name string, values []driver.Value) Filter {
	return fb.fieldFilter(FilterOpNotIn, name, values)
}

func (fb *filterBuilder) Lt(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpLt, name, value)
}

func (fb *filterBuilder) Gt(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpGt, name, value)
}

func (fb *filterBuilder) Gte(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpGte, name, value)
}

func (fb *filterBuilder) Lte(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpLte, name, value)
}

func (fb *filterBuilder) Contains(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpCont, name, value)
}

func (fb *filterBuilder) NotContains(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpNotCont, name, value)
}

func (fb *filterBuilder) IContains(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpICont, name, value)
}

func (fb *filterBuilder) NotIContains(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpNotICont, name, value)
}

func (fb *filterBuilder) StartsWith(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpStartsWith, name, value)
}

func (fb *filterBuilder) NotStartsWith(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpNotStartsWith, name, value)
}

func (fb *filterBuilder) IStartsWith(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpIStartsWith, name, value)
}

func (fb *filterBuilder) NotIStartsWith(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpNotIStartsWith, name, value)
}

func (fb *filterBuilder) EndsWith(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpEndsWith, name, value)
}

func (fb *filterBuilder) NotEndsWith(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpNotEndsWith, name, value)
}

func (fb *filterBuilder) IEndsWith(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpIEndsWith, name, value)
}

func (fb *filterBuilder) NotIEndsWith(name string, value driver.Value) Filter {
	return fb.fieldFilter(FilterOpNotIEndsWith, name, value)
}

func (fb *filterBuilder) fieldFilter(op FilterOp, name string, value interface{}) Filter {
	return &fieldFilter{
		baseFilter: baseFilter{
			fb:    fb,
			op:    op,
			field: name,
			value: value,
		},
	}
}

type fieldFilter struct {
	baseFilter
}
