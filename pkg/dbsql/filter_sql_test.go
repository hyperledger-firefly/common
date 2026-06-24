// Copyright © 2021 Kaleido, Inc.
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
	"sort"
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
)

var TestQueryFactory = &ffapi.QueryFields{
	"author":   &ffapi.StringField{},
	"cid":      &ffapi.UUIDField{},
	"created":  &ffapi.TimeField{},
	"hash":     &ffapi.Bytes32Field{},
	"id":       &ffapi.UUIDField{},
	"masked":   &ffapi.BoolField{},
	"output":   &ffapi.JSONField{},
	"sequence": &ffapi.Int64Field{},
	"tag":      &ffapi.StringField{},
	"topics":   &ffapi.FFStringArrayField{},
	"type":     &ffapi.StringField{},
	"address":  &ffapi.StringFieldLower{},
}

func TestSQLQueryFactoryIgnoreInvalidFilterFields(t *testing.T) {
	s, _ := NewMockProvider().UTInit()
	s.IndividualSort = true
	fb := TestQueryFactory.NewFilter(context.Background())
	f := fb.And(
		fb.Eq("tag", "tag1"),
	).
		Sort("-notvalid").
		Sort("notvalid").
		GroupBy("notvalid")

	sel := squirrel.Select("*").From("mytable")
	sel, _, _, err := s.FilterSelect(context.Background(), "", sel, f, map[string]string{
		"namespace": "ns",
	}, []interface{}{"sequence"})
	assert.NoError(t, err)

	sqlFilter, args, err := sel.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM mytable WHERE (tag = ?) ORDER BY seq DESC", sqlFilter)
	assert.Equal(t, "tag1", args[0])
}

func TestSQLQueryFactory(t *testing.T) {
	s, _ := NewMockProvider().UTInit()
	s.IndividualSort = true
	fb := TestQueryFactory.NewFilter(context.Background())
	f := fb.And(
		fb.Eq("tag", "tag1"),
		fb.Or(
			fb.Eq("id", "35c11cba-adff-4a4d-970a-02e3a0858dc8"),
			fb.Eq("id", "caefb9d1-9fc9-4d6a-a155-514d3139adf7"),
		),
		fb.Gt("sequence", "12345"),
		fb.Eq("created", nil),
	).
		Skip(50).
		Limit(25).
		Sort("-id").
		Sort("tag").
		Sort("-sequence").
		GroupBy("type")

	sel := squirrel.Select("*").From("mytable")
	sel, _, _, err := s.FilterSelect(context.Background(), "", sel, f, map[string]string{
		"namespace": "ns",
	}, []interface{}{"sequence"})
	assert.NoError(t, err)

	sqlFilter, args, err := sel.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM mytable WHERE (tag = ? AND (id = ? OR id = ?) AND seq > ? AND created IS NULL) GROUP BY type ORDER BY id DESC, tag, seq DESC LIMIT 25 OFFSET 50", sqlFilter)
	assert.Equal(t, "tag1", args[0])
	assert.Equal(t, "35c11cba-adff-4a4d-970a-02e3a0858dc8", args[1])
	assert.Equal(t, "caefb9d1-9fc9-4d6a-a155-514d3139adf7", args[2])
	assert.Equal(t, int64(12345), args[3])
}

func TestSQLQueryFactoryExtraOps(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	fb := TestQueryFactory.NewFilter(context.Background())
	u := fftypes.MustParseUUID("4066ABDC-8BBD-4472-9D29-1A55B467F9B9")
	f := fb.And(
		fb.In("created", []driver.Value{1, 2, 3}),
		fb.NotIn("created", []driver.Value{1, 2, 3}),
		fb.Eq("id", u),
		fb.In("id", []driver.Value{*u}),
		fb.Neq("id", nil),
		fb.Lt("created", "0"),
		fb.Lte("created", "0"),
		fb.Gte("created", "0"),
		fb.Neq("created", "0"),
		fb.Gt("sequence", 12345),
		fb.Contains("topics", "abc"),
		fb.NotContains("topics", "def"),
		fb.IContains("topics", "ghi"),
		fb.NotIContains("topics", "jkl"),
	).
		Descending()

	sel := squirrel.Select("*").From("mytable AS mt")
	sel, _, _, err := s.FilterSelect(context.Background(), "mt", sel, f, nil, []interface{}{"sequence"})
	assert.NoError(t, err)

	sqlFilter, _, err := sel.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM mytable AS mt WHERE (mt.created IN (?,?,?) AND mt.created NOT IN (?,?,?) AND mt.id = ? AND mt.id IN (?) AND mt.id IS NOT NULL AND mt.created < ? AND mt.created <= ? AND mt.created >= ? AND mt.created <> ? AND mt.seq > ? AND mt.topics LIKE ? ESCAPE '[' AND mt.topics NOT LIKE ? ESCAPE '[' AND mt.topics ILIKE ? ESCAPE '[' AND mt.topics NOT ILIKE ? ESCAPE '[') ORDER BY mt.seq DESC", sqlFilter)
}

func TestSQLQueryFactoryEvenMoreOps(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	fb := TestQueryFactory.NewFilter(context.Background())
	u := fftypes.MustParseUUID("4066ABDC-8BBD-4472-9D29-1A55B467F9B9")
	f := fb.And(
		fb.IEq("id", u),
		fb.NIeq("id", nil),
		fb.StartsWith("topics", "abc_"),
		fb.NotStartsWith("topics", "def%"),
		fb.IStartsWith("topics", "ghi"),
		fb.NotIStartsWith("topics", "jkl"),
		fb.EndsWith("topics", "mno"),
		fb.NotEndsWith("topics", "pqr"),
		fb.IEndsWith("topics", "sty"),
		fb.NotIEndsWith("topics", "vwx"),
	).
		Descending()

	sel := squirrel.Select("*").From("mytable AS mt")
	sel, _, _, err := s.FilterSelect(context.Background(), "mt", sel, f, nil, []interface{}{"sequence"})
	assert.NoError(t, err)

	sqlFilter, args, err := sel.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM mytable AS mt WHERE (mt.id ILIKE ? ESCAPE '[' AND mt.id NOT ILIKE ? ESCAPE '[' AND mt.topics LIKE ? ESCAPE '[' AND mt.topics NOT LIKE ? ESCAPE '[' AND mt.topics ILIKE ? ESCAPE '[' AND mt.topics NOT ILIKE ? ESCAPE '[' AND mt.topics LIKE ? ESCAPE '[' AND mt.topics NOT LIKE ? ESCAPE '[' AND mt.topics ILIKE ? ESCAPE '[' AND mt.topics NOT ILIKE ? ESCAPE '[') ORDER BY mt.seq DESC", sqlFilter)
	assert.Equal(t, []interface{}{
		"4066abdc-8bbd-4472-9d29-1a55b467f9b9",
		"",
		"abc[_%",
		"def[%%",
		"ghi%",
		"jkl%",
		"%mno",
		"%pqr",
		"%sty",
		"%vwx",
	}, args)
}

func TestSQLQueryFactoryLowerCaseIndexSearch(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	fb := TestQueryFactory.NewFilter(context.Background())
	addr1 := "0xf698D78272a0bCD63A3feb097B24a866f6b8a5a0"
	addr2 := "0xb9B919763dBC54D4D634150446Bf3991A9ef5eD7"
	f := fb.And(
		fb.IEq("address", addr1),
		fb.In("address", []driver.Value{addr1, addr2}),
	)

	sel := squirrel.Select("*").From("mytable AS mt")
	sel, _, _, err := s.FilterSelect(context.Background(), "mt", sel, f, nil, []interface{}{"sequence"})
	assert.NoError(t, err)

	sqlFilter, _, err := sel.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM mytable AS mt WHERE (lower(mt.address) ILIKE ? ESCAPE '[' AND lower(mt.address) IN (?,?)) ORDER BY mt.seq DESC", sqlFilter)
}

func TestSQLQueryFactoryEscapeLike(t *testing.T) {

	sel := squirrel.Select("*").From("mytable AS mt").
		Where(LikeEscape{"a": 1, "b": 2}).
		Where(NotLikeEscape{"a": 1, "b": 2}).
		Where(ILikeEscape{"a": 1, "b": 2}).
		Where(NotILikeEscape{"a": 1, "b": 2})

	sql, args, err := sel.ToSql()
	assert.NoError(t, err)
	assert.Regexp(t, `SELECT \* FROM mytable AS mt WHERE \([ab] LIKE \? ESCAPE '\[' AND [ab] LIKE \? ESCAPE '\['\) AND \([ab] NOT LIKE \? ESCAPE '\[' AND [ab] NOT LIKE \? ESCAPE '\['\) AND \([ab] ILIKE \? ESCAPE '\[' AND [ab] ILIKE \? ESCAPE '\['\) AND \([ab] NOT ILIKE \? ESCAPE '\[' AND [ab] NOT ILIKE \? ESCAPE '\['\)`, sql)
	assert.Len(t, args, 8)
	sort.Slice(args, func(i, j int) bool { return args[i].(int) < args[j].(int) })
	assert.Equal(t, []interface{}{1, 1, 1, 1, 2, 2, 2, 2}, args)
}

func TestSQLQueryFactoryFinalizeFail(t *testing.T) {
	s, _ := NewMockProvider().UTInit()
	fb := TestQueryFactory.NewFilter(context.Background())
	sel := squirrel.Select("*").From("mytable")
	_, _, _, err := s.FilterSelect(context.Background(), "ns", sel, fb.Eq("tag", map[bool]bool{true: false}), nil, []interface{}{"sequence"})
	assert.Regexp(t, "FF00143.*tag", err)
}

func TestSQLQueryFactoryBadOp(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	_, err := s.refineQuery(context.Background(), "", &ffapi.FilterInfo{
		Op: ffapi.FilterOp("wrong"),
	}, nil)
	assert.Regexp(t, "FF00190.*wrong", err)
}

func TestSQLQueryFactoryBadOpInOr(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	_, err := s.refineQuery(context.Background(), "", &ffapi.FilterInfo{
		Op: ffapi.FilterOpOr,
		Children: []*ffapi.FilterInfo{
			{Op: ffapi.FilterOp("wrong")},
		},
	}, nil)
	assert.Regexp(t, "FF00190.*wrong", err)
}

func TestSQLQueryFactoryBadOpInAnd(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	_, err := s.refineQuery(context.Background(), "", &ffapi.FilterInfo{
		Op: ffapi.FilterOpAnd,
		Children: []*ffapi.FilterInfo{
			{Op: ffapi.FilterOp("wrong")},
		},
	}, nil)
	assert.Regexp(t, "FF00190.*wrong", err)
}

func TestSQLQueryFactoryDefaultSort(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	sel := squirrel.Select("*").From("mytable")
	fb := TestQueryFactory.NewFilter(context.Background())
	f := fb.And(
		fb.Eq("tag", "tag1"),
	)
	sel, _, _, err := s.FilterSelect(context.Background(), "", sel, f, nil, []interface{}{
		&ffapi.SortField{
			Field:      "sequence",
			Descending: true,
			Nulls:      ffapi.NullsLast,
		},
	})
	assert.NoError(t, err)

	sqlFilter, args, err := sel.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM mytable WHERE (tag = ?) ORDER BY seq DESC NULLS LAST", sqlFilter)
	assert.Equal(t, "tag1", args[0])
}

func TestSQLQueryFactoryNullsLastPreconditions(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	sel := squirrel.Select("*").From("mytable")
	fb := TestQueryFactory.NewFilter(context.Background())
	f := fb.And(
		fb.Eq("tag", "tag1"),
	)
	sel, _, _, err := s.FilterSelect(context.Background(), "", sel, f, map[string]string{
		"tag": "another",
	}, []interface{}{
		&ffapi.SortField{
			Field:      "sequence",
			Descending: true,
			Nulls:      ffapi.NullsFirst,
		},
	}, squirrel.Eq{"a": "b"})
	assert.NoError(t, err)

	sqlFilter, args, err := sel.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM mytable WHERE (a = ? AND (another = ?)) ORDER BY seq DESC NULLS FIRST", sqlFilter)
	assert.Equal(t, "b", args[0])
	assert.Equal(t, "tag1", args[1])
}

func TestSQLQueryFactoryDefaultSortBadType(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	sel := squirrel.Select("*").From("mytable")
	fb := TestQueryFactory.NewFilter(context.Background())
	f := fb.And(
		fb.Eq("tag", "tag1"),
	)
	assert.PanicsWithValue(t, "unknown sort type: 100", func() {
		s.FilterSelect(context.Background(), "", sel, f, nil, []interface{}{100})
	})
}

func TestILIKE(t *testing.T) {
	s, _ := NewMockProvider().UTInit()

	s.features.UseILIKE = true
	q := s.newILike("test", "value")
	sqlString, _, _ := q.ToSql()
	assert.Regexp(t, "ILIKE", sqlString)

	s.features.UseILIKE = false
	q = s.newILike("test", "value")
	sqlString, _, _ = q.ToSql()
	assert.Regexp(t, "lower\\(test\\)", sqlString)
}

func TestNotILIKE(t *testing.T) {
	s, _ := NewMockProvider().UTInit()

	s.features.UseILIKE = true
	q := s.newNotILike("test", "value")
	sqlString, _, _ := q.ToSql()
	assert.Regexp(t, "ILIKE", sqlString)

	s.features.UseILIKE = false
	q = s.newNotILike("test", "value")
	sqlString, _, _ = q.ToSql()
	assert.Regexp(t, "lower\\(test\\)", sqlString)
}

func TestBuildUpdateExampleFail(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	ub := TestQueryFactory.NewUpdate(context.Background())
	q := squirrel.Update("table1")
	_, err := s.BuildUpdate(q, ub.Set("wrong", "1"), nil)
	assert.Regexp(t, "FF00142", err)
}

func TestBuildUpdateExample(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	ub := TestQueryFactory.NewUpdate(context.Background())
	q := squirrel.Update("table1")
	updateQuery, err := s.BuildUpdate(q, ub.Set("tag", "tag1"), nil)
	assert.NoError(t, err)

	sqlFilter, args, err := updateQuery.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE table1 SET tag = ?", sqlFilter)
	v, err := args[0].(driver.Valuer).Value()
	assert.NoError(t, err)
	assert.Equal(t, "tag1", v)
}

func TestBuildUpdateSetNull(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	ub := TestQueryFactory.NewUpdate(context.Background())
	q := squirrel.Update("table1")
	updateQuery, err := s.BuildUpdate(q, ub.SetNull("tag"), nil)
	assert.NoError(t, err)

	sqlFilter, args, err := updateQuery.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE table1 SET tag = NULL", sqlFilter)
	assert.Empty(t, args)
}

func TestFilterUpdateOk(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	q := squirrel.Update("table1").Set("name", "bob")

	fb := TestQueryFactory.NewFilter(context.Background())
	f := fb.And(
		fb.Eq("tag", "tag1"),
	)

	updateQuery, err := s.FilterUpdate(context.Background(), q, f, nil)
	assert.NoError(t, err)

	sqlFilter, _, err := updateQuery.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE table1 SET name = ? WHERE (tag = ?)", sqlFilter)
}

func TestFilterUpdateErr(t *testing.T) {

	s, _ := NewMockProvider().UTInit()
	q := squirrel.Update("table1").Set("name", "bob")

	fb := TestQueryFactory.NewFilter(context.Background())
	f := fb.And(
		fb.Eq("wrong", "1"),
	)

	_, err := s.FilterUpdate(context.Background(), q, f, nil)
	assert.Regexp(t, "FF00142", err)
}
