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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/hyperledger-firefly/common/pkg/i18n"
)

var allMods = []string{"not", "caseInsensitive"}
var justCaseInsensitive = []string{"caseInsensitive"}

// Note if ItemsResultTyped below might be preferred for new APIs (if you are able to adopt always-return {items:[]} style)
type FilterResultsWithCount struct {
	Count int64       `json:"count"`
	Total *int64      `json:"total,omitempty"` // omitted if a count was not calculated (AlwaysPaginate enabled, and count not specified)
	Items interface{} `json:"items"`
}

type ItemsResultTyped[T any] struct {
	Count int    `ffstruct:"CollectionResults" json:"count"`
	Total *int64 `ffstruct:"CollectionResults" json:"total,omitempty"` // omitted if a count was not calculated (AlwaysPaginate enabled, and count not specified)
	Items []T    `ffstruct:"CollectionResults" json:"items"`
}

type filterModifiers struct {
	negate          bool
	caseInsensitive bool
	emptyIsNull     bool
	andCombine      bool
}

type FilterJSONBase struct {
	Not             bool   `ffstruct:"FilterJSON" json:"not,omitempty"`
	CaseInsensitive bool   `ffstruct:"FilterJSON" json:"caseInsensitive,omitempty"`
	Field           string `ffstruct:"FilterJSON" json:"field,omitempty"`
}

type FilterJSONKeyValue struct {
	FilterJSONBase
	Value SimpleFilterValue `ffstruct:"FilterJSON" json:"value,omitempty"`
}

type FilterJSONKeyValues struct {
	FilterJSONBase
	Values []SimpleFilterValue `ffstruct:"FilterJSON" json:"values,omitempty"`
}

type FilterJSON struct {
	Or  []*FilterJSON `ffstruct:"FilterJSON" json:"or,omitempty"`
	And []*FilterJSON `ffstruct:"FilterJSON" json:"and,omitempty"`
	FilterJSONOps
}

type FilterJSONOps struct {
	Equal              []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"equal,omitempty"`
	Eq                 []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"eq,omitempty"`  // short name
	NEq                []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"neq,omitempty"` // negated short name
	Contains           []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"contains,omitempty"`
	StartsWith         []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"startsWith,omitempty"`
	EndsWith           []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"endsWith,omitempty"`
	LessThan           []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"lessThan,omitempty"`
	LT                 []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"lt,omitempty"` // short name
	LessThanOrEqual    []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"lessThanOrEqual,omitempty"`
	LTE                []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"lte,omitempty"` // short name
	GreaterThan        []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"greaterThan,omitempty"`
	GT                 []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"gt,omitempty"` // short name
	GreaterThanOrEqual []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"greaterThanOrEqual,omitempty"`
	GTE                []*FilterJSONKeyValue  `ffstruct:"FilterJSON" json:"gte,omitempty"` // short name
	In                 []*FilterJSONKeyValues `ffstruct:"FilterJSON" json:"in,omitempty"`
	NIn                []*FilterJSONKeyValues `ffstruct:"FilterJSON" json:"nin,omitempty"` // negated short name
	Null               []*FilterJSONBase      `ffstruct:"FilterJSON" json:"null,omitempty"`
}

type QueryJSON struct {
	FilterJSON
	Skip   *uint64  `ffstruct:"FilterJSON" json:"skip,omitempty"`
	Limit  *uint64  `ffstruct:"FilterJSON" json:"limit,omitempty"`
	Sort   []string `ffstruct:"FilterJSON" json:"sort,omitempty"`
	Count  *bool    `ffstruct:"FilterJSON" json:"count,omitempty"`
	Fields []string `ffstruct:"FilterJSON" json:"fields,omitempty"`
}

type SimpleFilterValue string

type resolveCtx struct {
	ctx                 context.Context
	jsonFilter          *FilterJSON
	valueResolver       ValueResolverFn
	fieldResolver       FieldResolverFn
	skipFieldValidation bool
	err                 error
}

type ValueResolverFn func(ctx context.Context, level *FilterJSON, fieldName, suppliedValue string) (driver.Value, error)

type FieldResolverFn func(ctx context.Context, fieldName string) (resolvedFieldName string, err error)

type JSONBuildFilterOpt struct {
	valueResolver       ValueResolverFn
	fieldResolver       FieldResolverFn
	skipFieldValidation bool
}

// Option to add a handler that will be called at each OR level, before performing the normal
// processing on each
func ValueResolver(fn ValueResolverFn) *JSONBuildFilterOpt {
	return &JSONBuildFilterOpt{
		valueResolver: fn,
	}
}

// If you want to know the full set of fields used in the field, and build a dynamic
// map of fields that might need JOIN relationships (such as labels)
func FieldResolver(fn FieldResolverFn) *JSONBuildFilterOpt {
	return &JSONBuildFilterOpt{
		fieldResolver: fn,
	}
}

func SkipFieldValidation() *JSONBuildFilterOpt {
	return &JSONBuildFilterOpt{
		skipFieldValidation: true,
	}
}

func (js *SimpleFilterValue) UnmarshalJSON(b []byte) error {
	var v interface{}
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}
	switch vi := v.(type) {
	case float64:
		*js = (SimpleFilterValue)(strconv.FormatFloat(vi, 'f', -1, 64))
		return nil
	case string:
		*js = (SimpleFilterValue)(vi)
		return nil
	case bool:
		*js = (SimpleFilterValue)(fmt.Sprintf("%t", vi))
		return nil
	default:
		return i18n.NewError(context.Background(), i18n.MsgJSONQueryValueUnsupported, string(b))
	}
}

func (js SimpleFilterValue) String() string {
	return (string)(js)
}

func (jq *QueryJSON) BuildFilter(ctx context.Context, qf QueryFactory, options ...*JSONBuildFilterOpt) (Filter, error) {
	fb := qf.NewFilter(ctx)
	if jq.Count != nil {
		fb = fb.Count(*jq.Count)
	}
	if jq.Skip != nil {
		fb = fb.Skip(*jq.Skip)
	}
	if jq.Limit != nil {
		fb = fb.Limit(*jq.Limit)
	}
	if len(jq.Fields) > 0 {
		fb = fb.RequiredFields(jq.Fields...)
	}
	if len(jq.Sort) > 0 {
		rv := buildResolveCtx(ctx, &jq.FilterJSON, options...)
		for _, field := range jq.Sort {
			field, isDesc := strings.CutPrefix(field, "-")
			field, err := validateFilterField(ctx, fb, field, rv)
			if err != nil {
				return nil, err
			}
			if isDesc {
				field = "-" + field
			}
			fb = fb.Sort(field)
		}
	}
	return (&jq.FilterJSON).BuildSubFilter(ctx, fb, options...)
}

func validateFilterField(ctx context.Context, fb FilterBuilder, fieldAnyCase string, rv *resolveCtx) (string, error) {
	if rv.skipFieldValidation {
		return fieldAnyCase, nil
	}
	if rv.fieldResolver != nil {
		return rv.fieldResolver(ctx, fieldAnyCase)
	}
	for _, f := range fb.Fields() {
		if strings.EqualFold(fieldAnyCase, f) {
			return f, nil
		}
	}
	return "", i18n.NewError(ctx, i18n.MsgInvalidFilterField, fieldAnyCase)
}

func (jf *FilterJSON) addSimpleFilters(ctx context.Context, fb FilterBuilder, andFilter AndFilter, rv *resolveCtx) (AndFilter, error) {
	for _, e := range joinShortNames(jf.Equal, jf.Eq, jf.NEq) {
		field, err := validateFilterField(ctx, fb, e.Field, rv)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive {
			if e.Not {
				andFilter = andFilter.Condition(fb.NIeq(field, rv.resolve(field, e.Value.String())))
			} else {
				andFilter = andFilter.Condition(fb.IEq(field, rv.resolve(field, e.Value.String())))
			}
		} else {
			if e.Not {
				andFilter = andFilter.Condition(fb.Neq(field, rv.resolve(field, e.Value.String())))
			} else {
				andFilter = andFilter.Condition(fb.Eq(field, rv.resolve(field, e.Value.String())))
			}
		}
	}
	for _, e := range jf.Contains {
		field, err := validateFilterField(ctx, fb, e.Field, rv)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotIContains(field, rv.resolve(field, e.Value.String())))
			} else {
				andFilter = andFilter.Condition(fb.IContains(field, rv.resolve(field, e.Value.String())))
			}
		} else {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotContains(field, rv.resolve(field, e.Value.String())))
			} else {
				andFilter = andFilter.Condition(fb.Contains(field, rv.resolve(field, e.Value.String())))
			}
		}
	}
	for _, e := range jf.StartsWith {
		field, err := validateFilterField(ctx, fb, e.Field, rv)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotIStartsWith(field, rv.resolve(field, e.Value.String())))
			} else {
				andFilter = andFilter.Condition(fb.IStartsWith(field, rv.resolve(field, e.Value.String())))
			}
		} else {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotStartsWith(field, rv.resolve(field, e.Value.String())))
			} else {
				andFilter = andFilter.Condition(fb.StartsWith(field, rv.resolve(field, e.Value.String())))
			}
		}
	}
	for _, e := range jf.EndsWith {
		field, err := validateFilterField(ctx, fb, e.Field, rv)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotIEndsWith(field, rv.resolve(field, e.Value.String())))
			} else {
				andFilter = andFilter.Condition(fb.IEndsWith(field, rv.resolve(field, e.Value.String())))
			}
		} else {
			if e.Not {
				andFilter = andFilter.Condition(fb.NotEndsWith(field, rv.resolve(field, e.Value.String())))
			} else {
				andFilter = andFilter.Condition(fb.EndsWith(field, rv.resolve(field, e.Value.String())))
			}
		}
	}
	for _, e := range jf.Null {
		field, err := validateFilterField(ctx, fb, e.Field, rv)
		if err != nil {
			return nil, err
		}
		if e.Not {
			andFilter = andFilter.Condition(fb.Neq(field, nil))
		} else {
			andFilter = andFilter.Condition(fb.Eq(field, nil))
		}
	}
	return andFilter, nil
}

func joinShortNames(long, short, negated []*FilterJSONKeyValue) []*FilterJSONKeyValue {
	res := make([]*FilterJSONKeyValue, len(long)+len(short)+len(negated))
	copy(res, long)
	copy(res[len(long):], short)
	negs := res[len(short)+len(long):]
	copy(negs, negated)
	for _, n := range negs {
		n.Not = true
	}
	return res
}

func joinInAndNin(in, nin []*FilterJSONKeyValues) []*FilterJSONKeyValues {
	res := make([]*FilterJSONKeyValues, len(in)+len(nin))
	copy(res, in)
	negs := res[len(in):]
	copy(negs, nin)
	for _, n := range negs {
		n.Not = true
	}
	return res
}

func (rv *resolveCtx) resolve(fieldName string, v string) driver.Value {
	if rv.valueResolver == nil {
		return v
	}
	resolved, err := rv.valueResolver(rv.ctx, rv.jsonFilter, fieldName, v)
	if err != nil {
		rv.err = err
		return ""
	}
	return resolved
}

func (rv *resolveCtx) resolveMany(fieldName string, values []SimpleFilterValue) []driver.Value {
	driverValues := make([]driver.Value, len(values))
	for i, v := range values {
		driverValues[i] = rv.resolve(fieldName, v.String())
	}
	return driverValues
}

func buildResolveCtx(ctx context.Context, jsonFilter *FilterJSON, options ...*JSONBuildFilterOpt) *resolveCtx {
	rv := &resolveCtx{ctx: ctx, jsonFilter: jsonFilter}
	for _, o := range options {
		if o.valueResolver != nil {
			rv.valueResolver = o.valueResolver
		}
		if o.fieldResolver != nil {
			rv.fieldResolver = o.fieldResolver
		}
		if o.skipFieldValidation {
			rv.skipFieldValidation = true
		}
	}
	return rv
}

func (jf *FilterJSON) BuildSubFilter(ctx context.Context, fb FilterBuilder, options ...*JSONBuildFilterOpt) (Filter, error) {
	andFilter, err := jf.BuildAndFilter(ctx, fb, options...)
	if err != nil {
		return nil, err
	}
	if len(andFilter.GetConditions()) == 1 {
		return andFilter.GetConditions()[0], nil
	}
	return andFilter, nil
}

func (jf *FilterJSON) BuildAndFilter(ctx context.Context, fb FilterBuilder, options ...*JSONBuildFilterOpt) (AndFilter, error) {
	rv := buildResolveCtx(ctx, jf, options...)
	andFilter, err := jf.addSimpleFilters(ctx, fb, fb.And(), rv)
	if err != nil {
		return nil, err
	}
	for _, e := range joinShortNames(jf.LessThan, jf.LT, nil) {
		field, err := validateFilterField(ctx, fb, e.Field, rv)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive || e.Not {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "lessThan", allMods)
		}
		andFilter = andFilter.Condition(fb.Lt(field, rv.resolve(field, e.Value.String())))
	}
	for _, e := range joinShortNames(jf.LessThanOrEqual, jf.LTE, nil) {
		field, err := validateFilterField(ctx, fb, e.Field, rv)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive || e.Not {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "lessThanOrEqual", allMods)
		}
		andFilter = andFilter.Condition(fb.Lte(field, rv.resolve(field, e.Value.String())))
	}
	for _, e := range joinShortNames(jf.GreaterThan, jf.GT, nil) {
		field, err := validateFilterField(ctx, fb, e.Field, rv)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive || e.Not {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "greaterThan", allMods)
		}
		andFilter = andFilter.Condition(fb.Gt(field, rv.resolve(field, e.Value.String())))
	}
	for _, e := range joinShortNames(jf.GreaterThanOrEqual, jf.GTE, nil) {
		field, err := validateFilterField(ctx, fb, e.Field, rv)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive || e.Not {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "greaterThanOrEqual", allMods)
		}
		andFilter = andFilter.Condition(fb.Gte(field, rv.resolve(field, e.Value.String())))
	}
	for _, e := range joinInAndNin(jf.In, jf.NIn) {
		field, err := validateFilterField(ctx, fb, e.Field, rv)
		if err != nil {
			return nil, err
		}
		if e.CaseInsensitive {
			return nil, i18n.NewError(ctx, i18n.MsgJSONQueryOpUnsupportedMod, "in", justCaseInsensitive)
		}
		if e.Not {
			andFilter = andFilter.Condition(fb.NotIn(field, rv.resolveMany(field, e.Values)))
		} else {
			andFilter = andFilter.Condition(fb.In(field, rv.resolveMany(field, e.Values)))
		}
	}
	for _, child := range jf.And {
		subFilter, err := child.BuildSubFilter(ctx, fb, options...)
		if err != nil {
			return nil, err
		}
		andFilter.Condition(subFilter)
	}
	if len(jf.Or) > 0 {
		childFilter := fb.Or()
		for _, child := range jf.Or {
			subFilter, err := child.BuildSubFilter(ctx, fb, options...)
			if err != nil {
				return nil, err
			}
			childFilter.Condition(subFilter)
		}
		if len(childFilter.GetConditions()) == 1 {
			andFilter.Condition(childFilter.GetConditions()[0])
		} else {
			andFilter.Condition(childFilter)
		}
	}
	// Any error that occurred as part of the resolver plugin, need to be reconciled
	if rv.err != nil {
		return nil, rv.err
	}
	return andFilter, nil
}

// Converts to a builder - which will add to the underlying query structure (cannot remove existing elements)
func (jq *QueryJSON) ToBuilder() QueryBuilder {
	return &queryBuilderImpl{
		rootQuery:  jq,
		statements: &jq.FilterJSON,
	}
}
