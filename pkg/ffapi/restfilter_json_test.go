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

package ffapi

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildQueryJSONNestedAndOr(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"fields": ["tag","masked","sequence","cid"],
		"sort": [
			"tag",
			"-sequence"
		],
		"equal": [
			{
				"field": "tag",
				"value": "a"
			}
		],
		"eq": [
			{
				"field": "masked",
				"value": true
			}
		],
		"neq": [
			{
				"field": "sequence",
				"value": 999
			}
		],
		"null": [
			{
				"field": "cid"
			}
		],
		"greaterThan": [
			{
				"field": "sequence",
				"value": 10
			}
		],
		"or": [
			{
				"equal": [
					{
						"field": "masked",
						"value": true
					}
				],
				"in": [
					{
						"field": "tag",
						"values": ["a","b","c"]
					}
				],
				"nin": [
					{
						"field": "tag",
						"values": ["x","y"]
					},
					{
						"field": "tag",
						"values": ["z"]
					}
				]
			},
			{
				"equal": [
					{
						"field": "masked",
						"value": false
					}
				]
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( tag == 'a' ) && ( masked == true ) && ( sequence != 999 ) && ( cid == null ) && ( sequence >> 10 ) && ( ( ( masked == true ) && ( tag IN ['a','b','c'] ) && ( tag NI ['x','y'] ) && ( tag NI ['z'] ) ) || ( masked == false ) ) requiredFields=tag,masked,sequence,cid sort=tag,-sequence skip=5 limit=10", fi.String())
}

func TestBuildQuerySingleNestedOr(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"or": [
			{
				"equal": [
					{
						"field": "tag",
						"value": "a"
					}
				]		
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "tag == 'a'", fi.String())
}

func TestBuildQueryAndWithNestedOr(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
	  "and": [
	  	{
			"or": [
				{
					"equal": [
						{
							"field": "tag",
							"value": "a"
						}
					]		
				},
				{
					"equal": [
						{
							"field": "tag",
							"value": "b"
						}
					]		
				}			
			]
		},
	  	{
			"equal": [
				{
					"field": "cid",
					"value": "12345"
				}
			]		
		}		
	  ]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( ( tag == 'a' ) || ( tag == 'b' ) ) && ( cid == '12345' )", fi.String())
}

func TestBuildQuerySkipFieldValidation(t *testing.T) {

	var jf *FilterJSON
	err := json.Unmarshal([]byte(`{
		"equal": [
			{
				"field": "anything at all",
				"value": "a"
			}
		]		
	}`), &jf)
	assert.NoError(t, err)

	fb := TestQueryFactory.NewFilter(context.Background())
	andFilter, err := jf.BuildAndFilter(context.Background(), fb, SkipFieldValidation())
	assert.NoError(t, err)
	conditions := andFilter.GetConditions()
	assert.Len(t, conditions, 1)

	cond0 := conditions[0].ValueFilter()
	assert.Equal(t, FilterOpEq, cond0.Op())
	assert.Equal(t, "anything at all", cond0.Field())
	assert.Equal(t, "a", cond0.Value())

}

func TestBuildQuerySingleNestedWithResolverOk(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
	    "sort": ["-created"],
		"or": [
			{
				"equal": [
					{
						"field": "tag",
						"value": "a"
					}
				]		
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	tqf := TestQueryFactory.Clone()
	filter, err := qf.BuildFilter(context.Background(), tqf,
		FieldResolver(func(ctx context.Context, fieldName string) (resolvedFieldName string, err error) {
			resolvedFieldName = fieldName + ".resolved"
			tqf[resolvedFieldName] = &StringField{}
			return
		}),
		ValueResolver(func(ctx context.Context, level *FilterJSON, fieldName, suppliedValue string) (driver.Value, error) {
			assert.Equal(t, "tag.resolved", fieldName)
			assert.Equal(t, "a", suppliedValue)
			assert.Len(t, level.Equal, 1)
			return "b", nil
		}),
	)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "tag.resolved == 'b' sort=-created.resolved", fi.String())
}

func TestBuildQuerySingleNestedWithResolverError(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"or": [
			{
				"in": [
					{
						"field": "tag",
						"values": ["a"]
					}
				]		
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	_, err = qf.BuildFilter(context.Background(), TestQueryFactory, ValueResolver(func(ctx context.Context, level *FilterJSON, fieldName, suppliedValue string) (driver.Value, error) {
		assert.Equal(t, "tag", fieldName)
		assert.Equal(t, "a", suppliedValue)
		assert.Len(t, level.In, 1)
		return "", fmt.Errorf("pop")
	}))
	assert.Regexp(t, "pop", err)
}

func TestBuildQuerySingleNestedWithFieldResolverError(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"sort": ["wrong"]
	}`), &qf)
	assert.NoError(t, err)

	_, err = qf.BuildFilter(context.Background(), TestQueryFactory, FieldResolver(func(ctx context.Context, fieldName string) (resolvedFieldName string, err error) {
		require.Equal(t, "wrong", fieldName)
		return "", fmt.Errorf("pop")
	}))
	assert.Regexp(t, "pop", err)
}

func TestBuildQueryJSONEqual(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"count": true,
		"sort": [
			"tag",
			"sequence"
		],
		"equal": [
			{
				"field": "created",
				"value": 0
			},
			{
				"not": true,
				"field": "tag",
				"value": "abc"
			},
			{
				"caseInsensitive": true,
				"field": "tag",
				"value": "ABC"
			},
			{
				"caseInsensitive": true,
				"not": true,
				"field": "tag",
				"value": "abc"
			}
		],
		"null": [
			{
				"not": true,
				"field": "cid"
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( created == 0 ) && ( tag != 'abc' ) && ( tag := 'ABC' ) && ( tag ;= 'abc' ) && ( cid != null ) sort=tag,sequence skip=5 limit=10 count=true", fi.String())
}

func TestBuildQueryJSONContains(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"contains": [
			{
				"field": "tag",
				"value": 0
			},
			{
				"not": true,
				"field": "tag",
				"value": "abc"
			},
			{
				"caseInsensitive": true,
				"field": "tag",
				"value": "ABC"
			},
			{
				"caseInsensitive": true,
				"not": true,
				"field": "tag",
				"value": "abc"
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( tag %= '0' ) && ( tag !% 'abc' ) && ( tag :% 'ABC' ) && ( tag ;% 'abc' ) skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONStartsWith(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"startsWith": [
			{
				"field": "tag",
				"value": 0
			},
			{
				"not": true,
				"field": "tag",
				"value": "abc"
			},
			{
				"caseInsensitive": true,
				"field": "tag",
				"value": "ABC"
			},
			{
				"caseInsensitive": true,
				"not": true,
				"field": "tag",
				"value": true
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( tag ^= '0' ) && ( tag !^ 'abc' ) && ( tag :^ 'ABC' ) && ( tag ;^ 'true' ) skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONEndsWith(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"endsWith": [
			{
				"field": "tag",
				"value": 0
			},
			{
				"not": true,
				"field": "tag",
				"value": "abc"
			},
			{
				"caseInsensitive": true,
				"field": "tag",
				"value": "ABC"
			},
			{
				"caseInsensitive": true,
				"not": true,
				"field": "tag",
				"value": true
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( tag $= '0' ) && ( tag !$ 'abc' ) && ( tag :$ 'ABC' ) && ( tag ;$ 'true' ) skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONGreaterThan(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"greaterThan": [
			{
				"field": "sequence",
				"value": 0
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "sequence >> 0 skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONLessThan(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"lessThan": [
			{
				"field": "sequence",
				"value": "12345"
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "sequence << 12345 skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONGreaterThanOrEqual(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"greaterThanOrEqual": [
			{
				"field": "sequence",
				"value": 0
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "sequence >= 0 skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONLessThanOrEqual(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"lessThanOrEqual": [
			{
				"field": "sequence",
				"value": "12345"
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "sequence <= 12345 skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONIn(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
		"skip": 5,
		"limit": 10,
		"in": [
			{
				"field": "tag",
				"values": ["a","b","c"]
			},
			{
				"not": true,
				"field": "tag",
				"values": ["x","y","z"]
			}
		]
	}`), &qf)
	assert.NoError(t, err)

	filter, err := qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( tag IN ['a','b','c'] ) && ( tag NI ['x','y','z'] ) skip=5 limit=10", fi.String())
}

func TestBuildQueryJSONBadModifiers(t *testing.T) {

	var qf1 QueryJSON
	err := json.Unmarshal([]byte(`{"lessThan": [{"not": true, "field": "tag"}]}`), &qf1)
	assert.NoError(t, err)
	_, err = qf1.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00240", err)

	var qf2 QueryJSON
	err = json.Unmarshal([]byte(`{"lessThanOrEqual": [{"not": true, "field": "tag"}]}`), &qf2)
	assert.NoError(t, err)
	_, err = qf2.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00240", err)

	var qf3 QueryJSON
	err = json.Unmarshal([]byte(`{"greaterThan": [{"not": true, "field": "tag"}]}`), &qf3)
	assert.NoError(t, err)
	_, err = qf3.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00240", err)

	var qf4 QueryJSON
	err = json.Unmarshal([]byte(`{"greaterThanOrEqual": [{"not": true, "field": "tag"}]}`), &qf4)
	assert.NoError(t, err)
	_, err = qf4.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00240", err)

	var qf5 QueryJSON
	err = json.Unmarshal([]byte(`{"in": [{"caseInsensitive": true, "field": "tag"}]}`), &qf5)
	assert.NoError(t, err)
	_, err = qf5.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00240", err)

	var qf6 QueryJSON
	err = json.Unmarshal([]byte(`{"or": [{"in": [{"caseInsensitive": true, "field": "tag"}]}] }`), &qf6)
	assert.NoError(t, err)
	_, err = qf6.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00240", err)

}

func TestStringableParseFail(t *testing.T) {

	var js SimpleFilterValue
	err := js.UnmarshalJSON([]byte(`{!!! not json`))
	assert.Error(t, err)

	err = js.UnmarshalJSON([]byte(`{"this": "is an object"}`))
	assert.Error(t, err)

}

func TestBuildQueryJSONBadFields(t *testing.T) {

	var qf1 QueryJSON
	err := json.Unmarshal([]byte(`{"equal": [{"field": "wrong"}]}`), &qf1)
	assert.NoError(t, err)
	_, err = qf1.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf2 QueryJSON
	err = json.Unmarshal([]byte(`{"contains": [{"field": "wrong"}]}`), &qf2)
	assert.NoError(t, err)
	_, err = qf2.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf3 QueryJSON
	err = json.Unmarshal([]byte(`{"startsWith": [{"field": "wrong"}]}`), &qf3)
	assert.NoError(t, err)
	_, err = qf3.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf4 QueryJSON
	err = json.Unmarshal([]byte(`{"endsWith": [{"field": "wrong"}]}`), &qf4)
	assert.NoError(t, err)
	_, err = qf4.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf5 QueryJSON
	err = json.Unmarshal([]byte(`{"lessThan": [{"field": "wrong"}]}`), &qf5)
	assert.NoError(t, err)
	_, err = qf5.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf6 QueryJSON
	err = json.Unmarshal([]byte(`{"lessThanOrEqual": [{"field": "wrong"}]}`), &qf6)
	assert.NoError(t, err)
	_, err = qf6.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf7 QueryJSON
	err = json.Unmarshal([]byte(`{"greaterThan": [{"field": "wrong"}]}`), &qf7)
	assert.NoError(t, err)
	_, err = qf7.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf8 QueryJSON
	err = json.Unmarshal([]byte(`{"greaterThanOrEqual": [{"field": "wrong"}]}`), &qf8)
	assert.NoError(t, err)
	_, err = qf8.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf9 QueryJSON
	err = json.Unmarshal([]byte(`{"in": [{"field": "wrong"}]}`), &qf9)
	assert.NoError(t, err)
	_, err = qf9.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)

	var qf10 QueryJSON
	err = json.Unmarshal([]byte(`{"null": [{"field": "wrong"}]}`), &qf10)
	assert.NoError(t, err)
	_, err = qf10.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142", err)
}

func TestBuildQueryJSONDocumented(t *testing.T) {
	CheckObjectDocumented(&QueryJSON{})
}

func TestBuildQueryJSONContainsShortNames(t *testing.T) {

	var qf1 QueryJSON
	err := json.Unmarshal([]byte(`{
		"eq": [
			{
				"field": "sequence",
				"value": "12345"
			}
		]
	}`), &qf1)
	assert.NoError(t, err)

	filter, err := qf1.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "sequence == 12345", fi.String())

	var qf2 QueryJSON
	err = json.Unmarshal([]byte(`{
		"gt": [
			{
				"field": "sequence",
				"value": "12345"
			}
		],
		"lte": [
			{
				"field": "sequence",
				"value": "12345"
			}
		]
	}`), &qf2)
	assert.NoError(t, err)

	filter, err = qf2.BuildFilter(context.Background(), TestQueryFactory)
	assert.NoError(t, err)

	fi, err = filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( sequence <= 12345 ) && ( sequence >> 12345 )", fi.String())
}

func TestBuildQueryAndFail(t *testing.T) {

	var qf QueryJSON
	err := json.Unmarshal([]byte(`{
	  "and": [
	  	{
			"or": [
				{
					"equal": [
						{
							"field": "color",
							"value": "b"
						}
					]		
				}			
			]
		}	
	  ]
	}`), &qf)
	assert.NoError(t, err)

	_, err = qf.BuildFilter(context.Background(), TestQueryFactory)
	assert.Regexp(t, "FF00142.*color", err)
}

func TestSimpleFilterValueNumbers(t *testing.T) {
	for in, expected := range map[string]string{
		`9007199254740993`: "9007199254740993", // 2^53+1, not representable as a float64
		`115792089237316195423570985008687907853269984665640564039457584007913129639935`: "115792089237316195423570985008687907853269984665640564039457584007913129639935", // 2^256-1
		`-57896044618658097711785492504343953926634992332820282019728792003956564819968`: "-57896044618658097711785492504343953926634992332820282019728792003956564819968", // -2^255
		`-12345678901234567890123`: "-12345678901234567890123",
		`42`:                       "42",
		`5.0`:                      "5",
		`1e3`:                      "1000",
		`1.5`:                      "1.5",
		`-0.000123`:                "-0.000123",
		`1.25e-2`:                  "0.0125",
		`0.0009765625`:             "0.0009765625", // 1/1024 - more places than denominator digits
		`9007199254740993e0`:       "9007199254740993",
		`123456789012345678901.5`:  "123456789012345678901.5",
		`1e30`:                     "1000000000000000000000000000000",
		// 2^-255, the most decimal places a value within 256 bits can need
		`0.000000000000000000000000000000000000000000000000000000000000000000000000000017272337110188889250772703725600799142232000728872562770047406940337183606324854115943015006944576453121094587892299327193990197893663893387306007554116149549372494220733642578125`: "0.000000000000000000000000000000000000000000000000000000000000000000000000000017272337110188889250772703725600799142232000728872562770047406940337183606324854115943015006944576453121094587892299327193990197893663893387306007554116149549372494220733642578125",
		`true`:                     "true",
		`"str"`:                    "str",
	} {
		var js SimpleFilterValue
		err := json.Unmarshal([]byte(in), &js)
		assert.NoError(t, err, in)
		assert.Equal(t, expected, js.String(), in)
	}

	var js SimpleFilterValue
	err := js.UnmarshalJSON([]byte(`115792089237316195423570985008687907853269984665640564039457584007913129639936`)) // 2^256
	assert.Regexp(t, "FF00241", err)
	err = js.UnmarshalJSON([]byte(`1e999999`))
	assert.Regexp(t, "FF00241", err)
	err = js.UnmarshalJSON([]byte(`1e99999999999999999999`)) // exponent overflows an int
	assert.Regexp(t, "FF00241", err)
	err = js.UnmarshalJSON([]byte(`1e80`)) // within the exponent bound, but over 256 bits
	assert.Regexp(t, "FF00241", err)
	err = js.UnmarshalJSON([]byte(strings.Repeat("9", 1_000_000))) // too long to parse
	assert.Regexp(t, "FF00241", err)
	err = js.UnmarshalJSON([]byte(`1e-400`))
	assert.Regexp(t, "FF00241", err)
}
