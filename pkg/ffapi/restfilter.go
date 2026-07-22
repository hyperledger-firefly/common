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

package ffapi

import (
	"context"
	"database/sql/driver"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
)

func (hs *HandlerFactory) getValues(values url.Values, key string) (results []string) {
	for queryName, queryValues := range values {
		// We choose to be case insensitive for our filters, so protocolID and protocolid can be used interchangeably
		if strings.EqualFold(queryName, key) {
			results = append(results, queryValues...)
		}
	}
	return results
}

func ParseFilterParam(ctx context.Context, fb FilterBuilder, field string, values []string) (Filter, error) {
	if len(values) == 1 {
		_, cond, err := getCondition(ctx, fb, field, values[0])
		if err != nil {
			return nil, err
		}
		return cond, nil
	}

	if len(values) > 0 {
		sort.Strings(values)
		fs := make([]Filter, len(values))
		andCombine := false
		for i, value := range values {
			mods, cond, err := getCondition(ctx, fb, field, value)
			if err != nil {
				return nil, err
			}
			andCombine = andCombine || mods.andCombine
			fs[i] = cond
		}
		if andCombine {
			return fb.And(fs...), nil
		}
		return fb.Or(fs...), nil
	}

	return nil, nil
}

func (hs *HandlerFactory) buildFilter(req *http.Request, ff QueryFactory) (AndFilter, error) {
	ctx := req.Context()
	log.L(ctx).Debugf("Query: %s", req.URL.RawQuery)
	fb := ff.NewFilterLimit(ctx, hs.DefaultFilterLimit)
	possibleFields := fb.Fields()
	sort.Strings(possibleFields)
	filter := fb.And()
	_ = req.ParseForm()
	for _, field := range possibleFields {
		f, err := ParseFilterParam(ctx, fb, field, hs.getValues(req.Form, field))
		if err != nil {
			return nil, err
		}
		if f != nil {
			filter.Condition(f)
		}
	}
	skipVals := hs.getValues(req.Form, "skip")
	if len(skipVals) > 0 {
		s, _ := strconv.ParseUint(skipVals[0], 10, 64)
		if hs.MaxFilterSkip != 0 && s > hs.MaxFilterSkip {
			return nil, i18n.NewError(req.Context(), i18n.MsgMaxFilterSkip, hs.MaxFilterSkip)
		}
		filter.Skip(s)
	}
	limitVals := hs.getValues(req.Form, "limit")
	if len(limitVals) > 0 {
		l, _ := strconv.ParseUint(limitVals[0], 10, 64)
		if hs.MaxFilterLimit != 0 && l > hs.MaxFilterLimit {
			return nil, i18n.NewError(req.Context(), i18n.MsgMaxFilterLimit, hs.MaxFilterLimit)
		}
		filter.Limit(l)
	}
	sortVals := hs.getValues(req.Form, "sort")
	for _, sv := range sortVals {
		subSortVals := strings.Split(sv, ",")
		for _, ssv := range subSortVals {
			ssv = strings.TrimSpace(ssv)
			if ssv != "" {
				filter.Sort(ssv)
			}
		}
	}
	if hs.SupportFieldRedaction {
		requiredFieldVals := hs.getValues(req.Form, "fields")
		for _, rf := range requiredFieldVals {
			subRequiredFieldVals := strings.Split(rf, ",")
			for _, srf := range subRequiredFieldVals {
				srf = strings.TrimSpace(srf)
				if srf != "" {
					filter.RequiredFields(srf)
				}
			}
		}
	}
	descendingVals := hs.getValues(req.Form, "descending")
	ascendingVals := hs.getValues(req.Form, "ascending")
	if len(descendingVals) > 0 && (descendingVals[0] == "" || strings.EqualFold(descendingVals[0], "true")) {
		filter.Descending()
	} else if len(ascendingVals) > 0 && (ascendingVals[0] == "" || strings.EqualFold(ascendingVals[0], "true")) {
		filter.Ascending()
	}
	countVals := hs.getValues(req.Form, "count")
	filter.Count(len(countVals) > 0 && (countVals[0] == "" || strings.EqualFold(countVals[0], "true")))
	return filter, nil
}

func checkNoModsExceptAnd(ctx context.Context, mods filterModifiers, field, op string, filter Filter) (Filter, error) {
	if mods.caseInsensitive || mods.emptyIsNull || mods.negate {
		return nil, i18n.NewError(ctx, i18n.MsgQueryOpUnsupportedMod, op, field)
	}
	return filter, nil
}

func getCondition(ctx context.Context, fb FilterBuilder, field, value string) (mods filterModifiers, filter Filter, err error) {

	mods = filterModifiers{}
	operator := make([]rune, 0, 2)
	prefixLength := 0
opFinder:
	for _, r := range value {
		switch r {
		case '!':
			mods.negate = true
			prefixLength++
		case ':':
			mods.caseInsensitive = true
			prefixLength++
		case '?':
			mods.emptyIsNull = true
			prefixLength++
		case '[':
			mods.andCombine = true
			prefixLength++
		case ']':
			mods.andCombine = false
			prefixLength++
		case '>', '<':
			// Terminates the opFinder if it's the second character
			if len(operator) == 1 && operator[0] != r {
				// Detected "><" or "<>" - which is a single char operator, followed by beginning of match string
				break opFinder
			}
			operator = append(operator, r)
			prefixLength++
			if len(operator) > 1 {
				// Detected ">>" or "<<" full operators
				break opFinder
			}
		case '=', '@', '^', '$':
			// Always terminates the opFinder
			// Could be ">=" or "<=" (due to above logic continuing on '>' or '<' first char)
			operator = append(operator, r)
			prefixLength++
			break opFinder
		default:
			// Found a normal character
			break opFinder
		}
	}

	var matchString driver.Value = value[prefixLength:]
	if mods.emptyIsNull && prefixLength == len(value) {
		matchString = nil
	}
	filter, err = mapOperation(ctx, fb, field, matchString, string(operator), mods)
	return mods, filter, err
}

func mapOperation(ctx context.Context, fb FilterBuilder, field string, matchString driver.Value, op string, mods filterModifiers) (filter Filter, err error) {

	switch op {
	case ">=":
		return checkNoModsExceptAnd(ctx, mods, field, op, fb.Gte(field, matchString))
	case "<=":
		return checkNoModsExceptAnd(ctx, mods, field, op, fb.Lte(field, matchString))
	case ">", ">>":
		return checkNoModsExceptAnd(ctx, mods, field, op, fb.Gt(field, matchString))
	case "<", "<<":
		return checkNoModsExceptAnd(ctx, mods, field, op, fb.Lt(field, matchString))
	case "@":
		if mods.caseInsensitive {
			if mods.negate {
				return fb.NotIContains(field, matchString), nil
			}
			return fb.IContains(field, matchString), nil
		}
		if mods.negate {
			return fb.NotContains(field, matchString), nil
		}
		return fb.Contains(field, matchString), nil
	case "^":
		if mods.caseInsensitive {
			if mods.negate {
				return fb.NotIStartsWith(field, matchString), nil
			}
			return fb.IStartsWith(field, matchString), nil
		}
		if mods.negate {
			return fb.NotStartsWith(field, matchString), nil
		}
		return fb.StartsWith(field, matchString), nil
	case "$":
		if mods.caseInsensitive {
			if mods.negate {
				return fb.NotIEndsWith(field, matchString), nil
			}
			return fb.IEndsWith(field, matchString), nil
		}
		if mods.negate {
			return fb.NotEndsWith(field, matchString), nil
		}
		return fb.EndsWith(field, matchString), nil
	default:
		if mods.caseInsensitive {
			if mods.negate {
				return fb.NIeq(field, matchString), nil
			}
			return fb.IEq(field, matchString), nil
		}
		if mods.negate {
			return fb.Neq(field, matchString), nil
		}
		return fb.Eq(field, matchString), nil
	}
}
