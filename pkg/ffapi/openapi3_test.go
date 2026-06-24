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
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/ghodss/yaml"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

type TestEnum fftypes.FFEnum

var (
	TestEnumVal1 = fftypes.FFEnumValue("ut", "val1")
	TestEnumVal2 = fftypes.FFEnumValue("ut", "val2")
)

type TestStruct1 struct {
	TestEmbedded1
	ID     *fftypes.UUID     `ffstruct:"ut1" json:"id"`
	Time   *fftypes.FFTime   `ffstruct:"ut1" json:"time"`
	Hash   *fftypes.Bytes32  `ffstruct:"ut1" json:"hash"`
	Amount *fftypes.FFBigInt `ffstruct:"ut1" json:"amount"`
}

type TestEmbedded1 struct {
	Enum1    TestEnum         `ffstruct:"ut1" json:"enum1" ffenum:"ut"`
	String1  string           `ffstruct:"ut1" json:"string1"`
	String2  string           `ffstruct:"ut1" json:"string2"`
	JSONAny1 *fftypes.JSONAny `ffstruct:"ut1" json:"jsonAny1,omitempty"`
	Number1  int64            `ffstruct:"ut1" json:"number1"`
	Struct2  TestStruct2      `ffstruct:"ut1" json:"struct2"`
}

type TestStruct2 struct {
	Enum1    TestEnum         `ffstruct:"ut1" json:"enum1" ffenum:"ut"`
	String1  string           `ffstruct:"ut1" json:"string1"`
	String2  string           `ffstruct:"ut1" json:"string2"`
	JSONAny1 *fftypes.JSONAny `ffstruct:"ut1" json:"jsonAny1,omitempty"`
}

type TestExtensions struct {
	String1 string `ffstruct:"ut1" json:"string1" ffschemaext:"x-key1=value1"`
	String2 string `ffstruct:"ut1" json:"string2" ffschemaext:"x-key1=value1&x-key1=value2&x-key2=value2%26value3"`
	String3 string `ffstruct:"ut1" json:"string3" ffschemaext:""`
}

type TestExtensionsBadKey struct {
	String1 string `ffstruct:"ut1" json:"string1" ffschemaext:"x-=value1"`
}

type TestExtensionsBadEncoding struct {
	String1 string `ffstruct:"ut1" json:"string1" ffschemaext:"x-key1=value1%"`
}

var ExampleDesc = i18n.FFM(language.AmericanEnglish, "TestKey", "Test Description")

var example2TagName = "Example 2"
var tagsMetadata = openapi3.Tags{
	{
		Name:        example2TagName,
		Description: "This is an example description of Example 2",
		ExternalDocs: &openapi3.ExternalDocs{
			Description: "Find out more about Example 2",
			URL:         "https://example2.com/docs",
		},
	},
}

var externalDocsMetadata = openapi3.ExternalDocs{
	Description: "Find out more about the Unit Test API",
	URL:         "https://unittest.com/docs",
}

var testRoutes = []*Route{
	{
		Name:   "op1",
		Path:   "namespaces/{ns}/example1/{id}/things",
		Method: http.MethodGet,
		PathParams: []*PathParam{
			{Name: "lang", ExampleFromConf: config.Lang, Description: ExampleDesc},
			{Name: "id", Example: "id12345", Description: ExampleDesc},
		},
		FilterFactory:   TestQueryFactory,
		QueryParams:     nil,
		Description:     ExampleDesc,
		JSONInputValue:  func() interface{} { return &TestStruct1{} },
		JSONOutputValue: func() interface{} { return &TestStruct2{} },
		JSONOutputCodes: []int{http.StatusOK},
	},
	{
		Name:           "op2",
		Path:           "example2",
		Method:         http.MethodPatch,
		PathParams:     nil,
		QueryParams:    nil,
		Description:    ExampleDesc,
		JSONInputValue: func() interface{} { return nil },
		JSONInputSchema: func(ctx context.Context, schemaGen SchemaGenerator) (*openapi3.SchemaRef, error) {
			s1, _ := schemaGen(&TestStruct1{})
			s2, _ := schemaGen(&TestStruct2{})
			return &openapi3.SchemaRef{
				Value: openapi3.NewAnyOfSchema(s1.Value, s2.Value),
			}, nil
		},
		JSONOutputSchema: func(ctx context.Context, schemaGen SchemaGenerator) (*openapi3.SchemaRef, error) {
			return schemaGen(&TestStruct1{})
		},
		JSONOutputCodes: []int{http.StatusOK},
		Tag:             example2TagName,
	},
	{
		Name:       "op3",
		Path:       "example2",
		Method:     http.MethodPut,
		PathParams: nil,
		QueryParams: []*QueryParam{
			{Name: "lang", ExampleFromConf: config.Lang, Description: ExampleDesc},
			{Name: "myfield", Default: "val1", Description: ExampleDesc},
		},
		Description:     ExampleDesc,
		JSONInputValue:  func() interface{} { return &TestStruct1{} },
		JSONOutputValue: func() interface{} { return nil },
		JSONOutputCodes: []int{http.StatusNoContent},
		FormParams: []*FormParam{
			{Name: "metadata", Description: ExampleDesc},
		},
		FormUploadHandler: func(r *APIRequest) (output interface{}, err error) { return nil, nil },
		Tag:               example2TagName,
	},
	{
		Name:   "op4",
		Path:   "example2/{id}",
		Method: http.MethodDelete,
		PathParams: []*PathParam{
			{Name: "id", Description: ExampleDesc},
		},
		QueryParams:     nil,
		Description:     ExampleDesc,
		JSONInputValue:  func() interface{} { return nil },
		JSONOutputValue: func() interface{} { return nil },
		JSONOutputCodes: []int{http.StatusNoContent},
		Tag:             example2TagName,
	},
	{
		Name:            "op5",
		Path:            "example2",
		Method:          http.MethodPost,
		PathParams:      nil,
		QueryParams:     nil,
		Description:     ExampleDesc,
		JSONInputValue:  func() interface{} { return &TestStruct1{} },
		JSONOutputValue: func() interface{} { return &TestStruct1{} },
		JSONOutputCodes: []int{http.StatusOK},
		Tag:             example2TagName,
	},
	{
		Name:       "op6",
		Path:       "example3",
		Method:     http.MethodGet,
		PathParams: nil,
		QueryParams: []*QueryParam{
			{Name: "scope", IsArray: true},
		},
		Description:     ExampleDesc,
		JSONInputValue:  func() interface{} { return &TestStruct1{} },
		JSONOutputValue: func() interface{} { return &TestStruct1{} },
		JSONOutputCodes: []int{http.StatusOK},
	},
	{
		Name:            "op7",
		Path:            "example7",
		Method:          http.MethodPut,
		PathParams:      nil,
		QueryParams:     nil,
		Description:     ExampleDesc,
		JSONInputValue:  func() interface{} { return &TestStruct1{} },
		JSONOutputValue: func() interface{} { return nil },
		JSONOutputCodes: []int{http.StatusNoContent},
		FormParams: []*FormParam{
			{Name: "metadata", Description: ExampleDesc},
		},
		Tag: example2TagName,
	},
	{
		Name:            "op8",
		Path:            "example8",
		Method:          http.MethodGet,
		PathParams:      nil,
		QueryParams:     nil,
		Description:     ExampleDesc,
		JSONInputValue:  func() interface{} { return nil },
		JSONOutputValue: func() interface{} { return &TestExtensions{} },
		JSONOutputCodes: []int{http.StatusOK},
	},
}

type TestInOutType struct {
	Length           float64          `ffstruct:"TestInOutType" json:"length"`
	Width            float64          `ffstruct:"TestInOutType" json:"width"`
	Height           float64          `ffstruct:"TestInOutType" json:"height" ffexcludeoutput:"true"`
	Volume           fftypes.FFBigInt `ffstruct:"TestInOutType" json:"volume" ffexcludeinput:"true"`
	Secret           string           `ffstruct:"TestInOutType" json:"secret" ffexclude:"true"`
	Conditional      string           `ffstruct:"TestInOutType" json:"conditional" ffexclude:"PostTagTest"`
	ConditionalInput string           `ffstruct:"TestInOutType" json:"conditionalInput" ffexcludeinput:"PostTagTest"`
}

type TestNonTaggedType struct {
	NoFFStructTag string `json:"noFFStructTag"`
}

func TestOpenAPI3SwaggerGen(t *testing.T) {
	doc := NewSwaggerGen(&SwaggerGenOptions{
		Title:                 "UnitTest",
		Version:               "1.0",
		SupportFieldRedaction: true,
		BaseURL:               "http://localhost:12345/api/v1",
		RouteCustomizations: func(ctx context.Context, sg *SwaggerGen, route *Route, op *openapi3.Operation) {
			sg.AddParam(ctx, op, "header", "x-my-param", "thing", "stuff", ExampleDesc, false)
		},
		Tags:         tagsMetadata,
		ExternalDocs: &externalDocsMetadata,
	}).Generate(context.Background(), testRoutes)
	err := doc.Validate(context.Background())
	assert.NoError(t, err)

	b, err := yaml.Marshal(doc)
	assert.NoError(t, err)
	fmt.Print(string(b))
}

func TestBadCustomInputSchemaFail(t *testing.T) {

	routes := []*Route{
		{
			Name:            "op6",
			Path:            "namespaces/{ns}/example1/{id}",
			Method:          http.MethodPost,
			JSONInputValue:  func() interface{} { return &TestStruct1{} },
			JSONInputMask:   []string{"id"},
			JSONOutputCodes: []int{http.StatusOK},
			JSONInputSchema: func(ctx context.Context, schemaGen SchemaGenerator) (*openapi3.SchemaRef, error) {
				return nil, fmt.Errorf("pop")
			},
		},
	}
	assert.Panics(t, func() {
		_ = NewSwaggerGen(&SwaggerGenOptions{
			Title:   "UnitTest",
			Version: "1.0",
			BaseURL: "http://localhost:12345/api/v1",
		}).Generate(context.Background(), routes)
	})
}

func TestBadCustomOutputSchemaFail(t *testing.T) {
	routes := []*Route{
		{
			Name:           "op7",
			Path:           "namespaces/{ns}/example1/{id}",
			Method:         http.MethodGet,
			JSONInputValue: func() interface{} { return &TestStruct1{} },
			JSONInputMask:  []string{"id"},
			JSONOutputSchema: func(ctx context.Context, schemaGen SchemaGenerator) (*openapi3.SchemaRef, error) {
				return nil, fmt.Errorf("pop")
			},
		},
	}
	assert.Panics(t, func() {
		_ = NewSwaggerGen(&SwaggerGenOptions{
			Title:   "UnitTest",
			Version: "1.0",
			BaseURL: "http://localhost:12345/api/v1",
		}).Generate(context.Background(), routes)
	})
}

func TestDuplicateOperationIDCheck(t *testing.T) {
	routes := []*Route{
		{Name: "op1"}, {Name: "op1"},
	}
	assert.PanicsWithValue(t, "Duplicate/invalid name (used as operation ID in swagger): op1", func() {
		_ = NewSwaggerGen(&SwaggerGenOptions{
			Title:   "UnitTest",
			Version: "1.0",
		}).Generate(context.Background(), routes)
	})
}

func TestWildcards(t *testing.T) {
	routes := []*Route{
		{
			Name:            "op1",
			Path:            "namespaces/{ns}/example1/{id:.*wildcard.*}",
			Method:          http.MethodPost,
			JSONInputValue:  func() interface{} { return &TestStruct1{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}
	swagger := NewSwaggerGen(&SwaggerGenOptions{

		Title:   "UnitTest",
		Version: "1.0",

		BaseURL: "http://localhost:12345/api/v1",
	}).Generate(context.Background(), routes)
	assert.NotNil(t, swagger.Paths.Find("/namespaces/{ns}/example1/{id}"))
	assert.NotNil(t, swagger.Paths.Value("/namespaces/{ns}/example1/{id}"))
}

func TestSamePathWithDifferentValues(t *testing.T) {
	routes := []*Route{
		{
			Name:            "op1",
			Path:            "namespaces/{ns}/example1/{id}",
			Method:          http.MethodPost,
			JSONInputValue:  func() interface{} { return &TestStruct1{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
		{
			Name:            "op2",
			Path:            "namespaces/{ns}/example1/{did}",
			Method:          http.MethodPost,
			JSONInputValue:  func() interface{} { return &TestStruct1{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}
	swagger := NewSwaggerGen(&SwaggerGenOptions{
		Title:   "UnitTest",
		Version: "1.0",
		BaseURL: "http://localhost:12345/api/v1",
	}).Generate(context.Background(), routes)
	assert.Equal(t, 2, swagger.Paths.Len())
	assert.NotNil(t, swagger.Paths.Find("/namespaces/{ns}/example1/{id}"))
	assert.NotNil(t, swagger.Paths.Value("/namespaces/{ns}/example1/{id}"))
	assert.NotNil(t, swagger.Paths.Find("/namespaces/{ns}/example1/{did}"))
	assert.NotNil(t, swagger.Paths.Value("/namespaces/{ns}/example1/{did}"))
}
func TestFFExcludeTag(t *testing.T) {
	routes := []*Route{
		{
			Name:            "PostTagTest",
			Path:            "namespaces/{ns}/example1/test",
			Method:          http.MethodPost,
			JSONInputValue:  func() interface{} { return &TestInOutType{} },
			JSONOutputValue: func() interface{} { return &TestInOutType{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}
	swagger := NewSwaggerGen(&SwaggerGenOptions{
		Title:   "UnitTest",
		Version: "1.0",
		BaseURL: "http://localhost:12345/api/v1",
	}).Generate(context.Background(), routes)
	assert.NotNil(t, swagger.Paths.Find("/namespaces/{ns}/example1/test").Post.RequestBody.Value)
	length, err := swagger.Paths.Find("/namespaces/{ns}/example1/test").Post.RequestBody.Value.Content.Get("application/json").Schema.Value.Properties.JSONLookup("length")
	assert.NoError(t, err)
	assert.NotNil(t, length)
	width, err := swagger.Paths.Find("/namespaces/{ns}/example1/test").Post.RequestBody.Value.Content.Get("application/json").Schema.Value.Properties.JSONLookup("width")
	assert.NoError(t, err)
	assert.NotNil(t, width)
	_, err = swagger.Paths.Find("/namespaces/{ns}/example1/test").Post.RequestBody.Value.Content.Get("application/json").Schema.Value.Properties.JSONLookup("secret")
	assert.Regexp(t, "no schema", err)
	_, err = swagger.Paths.Find("/namespaces/{ns}/example1/test").Post.RequestBody.Value.Content.Get("application/json").Schema.Value.Properties.JSONLookup("conditional")
	assert.Regexp(t, "no schema", err)
	_, err = swagger.Paths.Find("/namespaces/{ns}/example1/test").Post.RequestBody.Value.Content.Get("application/json").Schema.Value.Properties.JSONLookup("conditionalInput")
	assert.Regexp(t, "no schema", err)
}

func TestCustomResponseRefs(t *testing.T) {
	routes := []*Route{
		{
			Name:   "CustomResponseRefTest",
			Path:   "/test",
			Method: http.MethodGet,
			CustomResponseRefs: map[string]*openapi3.ResponseRef{
				"200": {
					Value: &openapi3.Response{
						Content: openapi3.Content{
							"text/plain": &openapi3.MediaType{},
						},
					},
				},
			},
		},
	}
	swagger := NewSwaggerGen(&SwaggerGenOptions{
		Title:   "UnitTest",
		Version: "1.0",

		BaseURL: "http://localhost:12345/api/v1",
	}).Generate(context.Background(), routes)
	assert.Nil(t, swagger.Paths.Find("/test").Get.RequestBody)
	require.NotEmpty(t, swagger.Paths.Find("/test").Get.Responses)
	require.NotNil(t, swagger.Paths.Find("/test").Get.Responses.Value("200"))
	require.NotNil(t, swagger.Paths.Find("/test").Get.Responses.Value("200").Value)
	assert.NotNil(t, swagger.Paths.Find("/test").Get.Responses.Value("200").Value.Content.Get("text/plain"))
	assert.Nil(t, swagger.Paths.Find("/test").Get.Responses.Value("201"))
}

func TestPanicOnMissingDescription(t *testing.T) {
	routes := []*Route{
		{
			Name:            "PostPanicOnMissingDescription",
			Path:            "namespaces/{ns}/example1/test",
			Method:          http.MethodPost,
			Description:     "this is fine",
			JSONInputValue:  func() interface{} { return &TestInOutType{} },
			JSONOutputValue: func() interface{} { return &TestInOutType{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}
	assert.PanicsWithValue(t, "invalid schema: FF00158: Field description missing for 'TestInOutType.conditional' on route 'PostPanicOnMissingDescription'", func() {
		_ = NewSwaggerGen(&SwaggerGenOptions{
			Title:                     "UnitTest",
			Version:                   "1.0",
			PanicOnMissingDescription: true,
			BaseURL:                   "http://localhost:12345/api/v1",
		}).Generate(context.Background(), routes)
	})
}

func TestPanicOnMissingFFStructTag(t *testing.T) {
	routes := []*Route{
		{
			Name:            "GetPanicOnMissingFFStructTag",
			Path:            "namespaces/{ns}/example1/test",
			Method:          http.MethodGet,
			Description:     "this is fine",
			JSONOutputValue: func() interface{} { return &TestNonTaggedType{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}
	assert.PanicsWithValue(t, "invalid schema: FF00160: ffstruct tag is missing for 'noFFStructTag' on route 'GetPanicOnMissingFFStructTag'", func() {
		_ = NewSwaggerGen(&SwaggerGenOptions{
			Title:                     "UnitTest",
			Version:                   "1.0",
			PanicOnMissingDescription: true,
			BaseURL:                   "http://localhost:12345/api/v1",
		}).Generate(context.Background(), routes)
	})
}

func TestPanicOnMissingRouteDescription(t *testing.T) {
	routes := []*Route{
		{
			Name:            "GetPanicOnMissingRouteDescription",
			Path:            "namespaces/{ns}/example1/test",
			Method:          http.MethodGet,
			JSONOutputValue: func() interface{} { return &TestNonTaggedType{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}
	assert.PanicsWithValue(t, "FF00159: API route description missing for route 'GetPanicOnMissingRouteDescription'", func() {
		_ = NewSwaggerGen(&SwaggerGenOptions{
			Title:                     "UnitTest",
			Version:                   "1.0",
			PanicOnMissingDescription: true,
			BaseURL:                   "http://localhost:12345/api/v1",
		}).Generate(context.Background(), routes)
	})
}

func TestPreTranslatedRouteDescription(t *testing.T) {
	routes := []*Route{
		{
			Name:                     "PostTagTest",
			Path:                     "namespaces/{ns}/example1/test",
			Method:                   http.MethodPost,
			JSONInputValue:           func() interface{} { return &TestInOutType{} },
			JSONOutputValue:          func() interface{} { return &TestInOutType{} },
			JSONOutputCodes:          []int{http.StatusOK},
			PreTranslatedDescription: "this is a description",
		},
	}
	swagger := NewSwaggerGen(&SwaggerGenOptions{
		Title:   "UnitTest",
		Version: "1.0",
		BaseURL: "http://localhost:12345/api/v1",
	}).Generate(context.Background(), routes)
	assert.NotNil(t, swagger.Paths.Find("/namespaces/{ns}/example1/test").Post.RequestBody.Value)
	description := swagger.Paths.Find("/namespaces/{ns}/example1/test").Post.Description
	assert.NotNil(t, swagger.Paths.Value("/namespaces/{ns}/example1/test").Post.RequestBody.Value)
	description = swagger.Paths.Value("/namespaces/{ns}/example1/test").Post.Description
	assert.Equal(t, "this is a description", description)
}

func TestBaseURLVariables(t *testing.T) {
	doc := NewSwaggerGen(&SwaggerGenOptions{
		Title:   "UnitTest",
		Version: "1.0",
		BaseURL: "http://localhost:12345/api/v1/{param}",
		BaseURLVariables: map[string]BaseURLVariable{
			"param": {
				Default: "default-value",
			},
		},
	}).Generate(context.Background(), testRoutes)
	err := doc.Validate(context.Background())
	assert.NoError(t, err)

	server := doc.Servers[0]
	if assert.Contains(t, server.Variables, "param") {
		assert.Equal(t, "default-value", server.Variables["param"].Default)
	}

	b, err := yaml.Marshal(doc)
	assert.NoError(t, err)
	fmt.Print(string(b))
}

func TestCheckObjectDocumented(t *testing.T) {
	type Undocumented struct {
		Field1 string `ffstruct:"ThisDoesNotHaveDocs" json:"field1"`
	}
	defer func() {
		assert.Regexp(t, "FF00158.*ThisDoesNotHaveDocs.field1", recover())
	}()
	CheckObjectDocumented(&Undocumented{})
}

func TestExcludeFromOpenAPI(t *testing.T) {
	routes := []*Route{
		{
			Name:               "IncludedRoute",
			Path:               "included",
			Method:             http.MethodGet,
			Description:        ExampleDesc,
			JSONInputValue:     func() interface{} { return &TestStruct1{} },
			JSONOutputValue:    func() interface{} { return &TestStruct1{} },
			JSONOutputCodes:    []int{http.StatusOK},
			ExcludeFromOpenAPI: false,
		},
		{
			Name:               "ExcludedRoute",
			Path:               "excluded",
			Method:             http.MethodPost,
			Description:        ExampleDesc,
			JSONInputValue:     func() interface{} { return &TestStruct1{} },
			JSONOutputValue:    func() interface{} { return &TestStruct1{} },
			JSONOutputCodes:    []int{http.StatusOK},
			ExcludeFromOpenAPI: true,
		},
		{
			Name:               "AnotherIncludedRoute",
			Path:               "another-included",
			Method:             http.MethodPut,
			Description:        ExampleDesc,
			JSONInputValue:     func() interface{} { return &TestStruct1{} },
			JSONOutputValue:    func() interface{} { return &TestStruct1{} },
			JSONOutputCodes:    []int{http.StatusOK},
			ExcludeFromOpenAPI: false,
		},
	}

	doc := NewSwaggerGen(&SwaggerGenOptions{
		Title:   "ExcludeFromOpenAPITest",
		Version: "1.0",
		BaseURL: "http://localhost:12345/api/v1",
	}).Generate(context.Background(), routes)

	assert.NotNil(t, doc.Paths)

	// Check that included routes are present
	includedPath := doc.Paths.Find("/included")
	assert.NotNil(t, includedPath)
	assert.NotNil(t, includedPath.Get) // GET method should be present

	anotherIncludedPath := doc.Paths.Find("/another-included")
	assert.NotNil(t, anotherIncludedPath)
	assert.NotNil(t, anotherIncludedPath.Put) // PUT method should be present

	// Check that excluded route is not present
	excludedPath := doc.Paths.Find("/excluded")
	assert.Nil(t, excludedPath) // Should be nil because route was excluded

	// Verify the document is valid
	err := doc.Validate(context.Background())
	assert.NoError(t, err)
}

func TestExtensionsBadEncodingFail(t *testing.T) {
	routes := []*Route{
		{
			Name:            "badEncoding",
			Path:            "extensions",
			Method:          http.MethodGet,
			JSONInputValue:  func() interface{} { return nil },
			JSONOutputValue: func() interface{} { return &TestExtensionsBadEncoding{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}

	assert.PanicsWithValue(t, "invalid schema: FF00258: Invalid extension 'x-key1=value1%' - extensions should be RFC 3986 compliant query parameter format (e.g. x-name=value with percent-encoding for special characters): invalid URL escape \"%\"", func() {
		_ = NewSwaggerGen(&SwaggerGenOptions{
			Title:   "UnitTest",
			Version: "1.0",
			BaseURL: "http://localhost:12345/api/v1",
		}).Generate(context.Background(), routes)
	})
}

func TestExtensionsBadKeyFail(t *testing.T) {
	routes := []*Route{
		{
			Name:            "bad3",
			Path:            "extensions",
			Method:          http.MethodGet,
			JSONInputValue:  func() interface{} { return nil },
			JSONOutputValue: func() interface{} { return &TestExtensionsBadKey{} },
			JSONOutputCodes: []int{http.StatusOK},
		},
	}

	assert.PanicsWithValue(t, "invalid schema: FF00259: Invalid extension key 'x-' - extension keys must follow the format 'x-<name>'", func() {
		_ = NewSwaggerGen(&SwaggerGenOptions{
			Title:   "UnitTest",
			Version: "1.0",
			BaseURL: "http://localhost:12345/api/v1",
		}).Generate(context.Background(), routes)
	})
}

func TestOpenAPIVersion(t *testing.T) {
	doc := NewSwaggerGen(&SwaggerGenOptions{
		Title:          "UnitTest",
		Version:        "1.0",
		OpenAPIVersion: "3.1.1",
		BaseURL:        "http://localhost:12345/api/v1",
	}).Generate(context.Background(), testRoutes)
	err := doc.Validate(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "3.1.1", doc.OpenAPI)
}
