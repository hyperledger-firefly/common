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
	"log"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3gen"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
)

type SwaggerGenOptions struct {
	// ---------- options that applies to all API versions ----------

	// attributes of openapi3.Info
	Title       string
	Version     string
	Description string

	OpenAPIVersion string

	// descriptions for the route parameters are parsed from the ffstruct tags
	// when set this flag to true, if a description is not found, the generator will panic
	// this is useful to ensure that all fields are documented
	PanicOnMissingDescription bool

	SupportFieldRedaction bool
	DefaultRequestTimeout time.Duration

	// ----------- API version specific options ------------
	// options to generate the base URL
	BaseURL          string
	BaseURLVariables map[string]BaseURLVariable

	// options of controlling query parameters values
	APIMaxFilterSkip      uint
	APIDefaultFilterLimit string
	APIMaxFilterLimit     uint

	// a custom function to modify the generated OpenAPI spec
	RouteCustomizations func(ctx context.Context, sg *SwaggerGen, route *Route, op *openapi3.Operation)

	// OpenAPI 3.0.x specific options
	Tags                       openapi3.Tags
	ExternalDocs               *openapi3.ExternalDocs
	ExportComponentOpts        *openapi3gen.ExportComponentSchemasOptions
	AdditionalSchemaCustomizer func(name string, t reflect.Type, tag reflect.StructTag, schema *openapi3.Schema) error
}

type BaseURLVariable struct {
	Default     string
	Description string
}

var (
	customRegexRemoval = regexp.MustCompile(`{(\w+)\:[^}]+}`)
	// Check ffExtension key starts with "x-"
	ffExtensionKeyRegexp = regexp.MustCompile(`^x-.+$`)
)

type SwaggerGen struct {
	options      *SwaggerGenOptions
	ffTagHandler *OpenAPITagHandler
}

func NewSwaggerGen(options *SwaggerGenOptions) *SwaggerGen {
	return &SwaggerGen{
		options: options,
		ffTagHandler: &OpenAPITagHandler{
			PanicOnMissingDescription: options.PanicOnMissingDescription,
		},
	}
}

func (sg *SwaggerGen) Generate(ctx context.Context, routes []*Route) *openapi3.T {

	server := &openapi3.Server{
		URL: sg.options.BaseURL,
	}

	if sg.options.BaseURLVariables != nil {
		server.Variables = map[string]*openapi3.ServerVariable{}
		for variableName, variable := range sg.options.BaseURLVariables {
			server.Variables[variableName] = &openapi3.ServerVariable{
				Default:     variable.Default,
				Description: variable.Description,
			}
		}
	}

	openAPIVersion := "3.0.2"
	if sg.options.OpenAPIVersion != "" {
		openAPIVersion = sg.options.OpenAPIVersion
	}

	doc := &openapi3.T{
		OpenAPI: openAPIVersion,
		Servers: openapi3.Servers{
			server,
		},
		Info: &openapi3.Info{
			Title:       sg.options.Title,
			Version:     sg.options.Version,
			Description: sg.options.Description,
		},
		Components: &openapi3.Components{
			Schemas: make(openapi3.Schemas),
		},
		Tags:         sg.options.Tags,
		ExternalDocs: sg.options.ExternalDocs,
	}
	opIDs := make(map[string]bool)
	for _, route := range routes {
		// Skip routes that are excluded from OpenAPI generation
		if route.ExcludeFromOpenAPI {
			continue
		}
		if route.Name == "" || opIDs[route.Name] {
			log.Panicf("Duplicate/invalid name (used as operation ID in swagger): %s", route.Name)
		}
		sg.addRoute(ctx, doc, route)
		opIDs[route.Name] = true
	}
	return doc
}

func (sg *SwaggerGen) getPathItem(doc *openapi3.T, path string) *openapi3.PathItem {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	path = customRegexRemoval.ReplaceAllString(path, `{$1}`)
	if doc.Paths == nil {
		doc.Paths = &openapi3.Paths{}
	}
	pi := doc.Paths.Value(path)
	if pi != nil {
		return pi
	}
	pi = &openapi3.PathItem{}
	doc.Paths.Set(path, pi)
	return pi
}

func (sg *SwaggerGen) initInput(op *openapi3.Operation) {
	op.RequestBody = &openapi3.RequestBodyRef{
		Value: &openapi3.RequestBody{
			Required: true,
			Content:  openapi3.Content{},
		},
	}
}

func isTrue(str string) bool {
	return strings.EqualFold(str, "true")
}

func (sg *SwaggerGen) ffInputTagHandler(ctx context.Context, route *Route, name string, tag reflect.StructTag, schema *openapi3.Schema) error {
	if isTrue(tag.Get("ffexcludeinput")) {
		return &openapi3gen.ExcludeSchemaSentinel{}
	}
	if taggedRoutes, ok := tag.Lookup("ffexcludeinput"); ok {
		for _, r := range strings.Split(taggedRoutes, ",") {
			if route.Name == r {
				return &openapi3gen.ExcludeSchemaSentinel{}
			}
		}
	}
	return sg.ffTagHandler.HandleFFTags(ctx, route, name, tag, schema)
}

func (sg *SwaggerGen) ffOutputTagHandler(ctx context.Context, route *Route, name string, tag reflect.StructTag, schema *openapi3.Schema) error {
	if isTrue(tag.Get("ffexcludeoutput")) {
		return &openapi3gen.ExcludeSchemaSentinel{}
	}
	return sg.ffTagHandler.HandleFFTags(ctx, route, name, tag, schema)
}

func applyFFExtensionsTag(ctx context.Context, schema *openapi3.Schema, tag string) error {
	if tag == "" {
		return nil
	}
	q, err := url.ParseQuery(tag)
	if err != nil {
		return i18n.WrapError(ctx, err, i18n.MsgFFExtensionsInvalid, tag)
	}
	for extension, values := range q {
		for _, value := range values {
			if !ffExtensionKeyRegexp.MatchString(extension) {
				return i18n.NewError(ctx, i18n.MsgFFExtensionsInvalidEncoding, extension)
			}
			if schema.Extensions == nil {
				schema.Extensions = make(map[string]interface{})
			}
			schema.Extensions[extension] = value
		}
	}
	return nil
}

type OpenAPITagHandler struct {
	PanicOnMissingDescription bool
}

func (th *OpenAPITagHandler) HandleFFTags(ctx context.Context, route *Route, name string, tag reflect.StructTag, schema *openapi3.Schema) error {
	if ffEnum := tag.Get("ffenum"); ffEnum != "" {
		// For a container field (e.g. []FFEnum or map[string]FFEnum) the openapi3gen library
		// invokes this customizer with the same field tag for both the container schema and its
		// element schema (array items / map additionalProperties). FFEnum values are always
		// strings, so the enum belongs only on the string leaf - applying it to the "array" or
		// "object" container node too produces invalid OpenAPI that strict SDK generators reject.
		if schema.Type.Is(openapi3.TypeString) {
			schema.Enum = fftypes.FFEnumValues(ffEnum)
		}
	}
	if ffExtensions := tag.Get("ffschemaext"); ffExtensions != "" {
		if err := applyFFExtensionsTag(ctx, schema, ffExtensions); err != nil {
			return err
		}
	}
	if isTrue(tag.Get("ffexclude")) {
		return &openapi3gen.ExcludeSchemaSentinel{}
	}
	if taggedRoutes, ok := tag.Lookup("ffexclude"); ok {
		for _, r := range strings.Split(taggedRoutes, ",") {
			if route.Name == r {
				return &openapi3gen.ExcludeSchemaSentinel{}
			}
		}
	}
	if name != "_root" {
		if structName, ok := tag.Lookup("ffstruct"); ok {
			key := fmt.Sprintf("%s.%s", structName, name)
			description := i18n.Expand(ctx, i18n.MessageKey(key))
			if description == key && th.PanicOnMissingDescription {
				return i18n.NewError(ctx, i18n.MsgFieldDescriptionMissing, key, route.Name)
			}
			schema.Description = description
		} else if th.PanicOnMissingDescription {
			return i18n.NewError(ctx, i18n.MsgFFStructTagMissing, name, route.Name)
		}
	}
	return nil
}

func (sg *SwaggerGen) addCustomType(t reflect.Type, schema *openapi3.Schema) {
	switch t.Name() {
	case "UUID":
		schema.Type = &openapi3.Types{openapi3.TypeString}
		schema.Format = "uuid"
	case "FFTime":
		schema.Type = &openapi3.Types{openapi3.TypeString}
		schema.Format = "date-time"
	case "Bytes32":
		schema.Type = &openapi3.Types{openapi3.TypeString}
		schema.Format = "byte"
	case "FFBigInt":
		schema.Type = &openapi3.Types{openapi3.TypeString}
	case "JSONAny":
		schema.Type = &openapi3.Types{openapi3.TypeObject}
		True := true
		schema.AdditionalProperties = openapi3.AdditionalProperties{Has: &True}
	}

	if schema.Items != nil && schema.Items.Value != nil {
		schema.Items.Value.Nullable = false
	}
}

func (sg *SwaggerGen) schemaRefOptions(ctx context.Context, route *Route, tagHandler func(ctx context.Context, route *Route, name string, tag reflect.StructTag, schema *openapi3.Schema) error) []openapi3gen.Option {
	schemaCustomizer := func(name string, t reflect.Type, tag reflect.StructTag, schema *openapi3.Schema) error {
		sg.addCustomType(t, schema)
		err := tagHandler(ctx, route, name, tag, schema)
		if err == nil && sg.options != nil && sg.options.AdditionalSchemaCustomizer != nil {
			err = sg.options.AdditionalSchemaCustomizer(name, t, tag, schema)
		}
		return err
	}
	options := []openapi3gen.Option{openapi3gen.SchemaCustomizer(schemaCustomizer)}
	if sg.options != nil && sg.options.ExportComponentOpts != nil {
		options = append(options, openapi3gen.CreateComponentSchemas(*sg.options.ExportComponentOpts))
	}
	return options
}

func (sg *SwaggerGen) addInput(ctx context.Context, doc *openapi3.T, route *Route, op *openapi3.Operation) {
	var schemaRef *openapi3.SchemaRef
	var err error
	switch {
	case route.JSONInputSchema != nil:
		schemaRef, err = route.JSONInputSchema(ctx, func(obj interface{}) (*openapi3.SchemaRef, error) {
			return openapi3gen.NewSchemaRefForValue(obj, doc.Components.Schemas, sg.schemaRefOptions(ctx, route, sg.ffInputTagHandler)...)
		})
		if err != nil {
			panic(fmt.Sprintf("invalid schema: %s", err))
		}
	case route.JSONInputValue != nil:
		schemaRef, err = openapi3gen.NewSchemaRefForValue(route.JSONInputValue(), doc.Components.Schemas, sg.schemaRefOptions(ctx, route, sg.ffInputTagHandler)...)
		if err != nil {
			panic(fmt.Sprintf("invalid schema: %s", err))
		}
	}
	op.RequestBody.Value.Content["application/json"] = &openapi3.MediaType{
		Schema: schemaRef,
	}
}

func (sg *SwaggerGen) addUploadFormInput(ctx context.Context, op *openapi3.Operation, formParams []*FormParam) {
	props := openapi3.Schemas{
		"filename.ext": &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Type:   &openapi3.Types{openapi3.TypeString},
				Format: "binary",
			},
		},
	}
	for _, fp := range formParams {
		props[fp.Name] = &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Description: i18n.Expand(ctx, i18n.APISuccessResponse),
				Type:        &openapi3.Types{openapi3.TypeString},
			},
		}
	}

	op.RequestBody.Value.Content["multipart/form-data"] = &openapi3.MediaType{
		Schema: &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Type:       &openapi3.Types{openapi3.TypeObject},
				Properties: props,
			},
		},
	}
}

func (sg *SwaggerGen) addURLEncodedFormInput(ctx context.Context, op *openapi3.Operation, formParams []*FormParam) {
	props := openapi3.Schemas{}
	for _, fp := range formParams {
		props[fp.Name] = &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Description: i18n.Expand(ctx, i18n.APISuccessResponse),
				Type:        &openapi3.Types{openapi3.TypeString},
			},
		}

		if fp.Description != "" {
			props[fp.Name].Value.Description = i18n.Expand(ctx, fp.Description)
		}
	}

	op.RequestBody.Value.Content["application/x-www-form-urlencoded"] = &openapi3.MediaType{
		Schema: &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Type:       &openapi3.Types{openapi3.TypeObject},
				Properties: props,
			},
		},
	}
}

// CheckObjectDocumented lets unit tests on individual structures validate that all the ffstruct tags are set,
// without having to build their own swagger.
func CheckObjectDocumented(example interface{}) {
	(NewSwaggerGen(&SwaggerGenOptions{PanicOnMissingDescription: true})).Generate(context.Background(), []*Route{{
		Name:           "doctest",
		Path:           "doctest",
		Method:         http.MethodPost,
		Description:    "Test route to verify object is fully documented",
		JSONInputValue: func() interface{} { return example },
	}})
}

func (sg *SwaggerGen) addOutput(ctx context.Context, doc *openapi3.T, route *Route, op *openapi3.Operation) {
	var schemaRef *openapi3.SchemaRef
	var err error
	s := i18n.Expand(ctx, i18n.APISuccessResponse)
	switch {
	case route.JSONOutputSchema != nil:
		schemaRef, err = route.JSONOutputSchema(ctx, func(obj interface{}) (*openapi3.SchemaRef, error) {
			return openapi3gen.NewSchemaRefForValue(obj, doc.Components.Schemas, sg.schemaRefOptions(ctx, route, sg.ffOutputTagHandler)...)
		})
		if err != nil {
			panic(fmt.Sprintf("invalid schema: %s", err))
		}
	case route.JSONOutputValue != nil:
		outputValue := route.JSONOutputValue()
		if outputValue != nil {
			schemaRef, err = openapi3gen.NewSchemaRefForValue(outputValue, doc.Components.Schemas, sg.schemaRefOptions(ctx, route, sg.ffOutputTagHandler)...)
			if err != nil {
				panic(fmt.Sprintf("invalid schema: %s", err))
			}
		}
	}
	for _, code := range route.JSONOutputCodes {
		op.Responses.Set(strconv.FormatInt(int64(code), 10), &openapi3.ResponseRef{
			Value: &openapi3.Response{
				Description: &s,
				Content: openapi3.Content{
					"application/json": &openapi3.MediaType{
						Schema: schemaRef,
					},
				},
			},
		})
	}
	for code, res := range route.CustomResponseRefs {
		if res.Value != nil && res.Value.Description == nil {
			res.Value.Description = &s
		}
		op.Responses.Set(code, res)
	}
}

func (sg *SwaggerGen) AddParam(ctx context.Context, op *openapi3.Operation, in, name, def, example string, description i18n.MessageKey, deprecated bool, msgArgs ...interface{}) {
	sg.addParamInternal(ctx, op, in, name, def, example, false, description, deprecated, msgArgs...)
}

func (sg *SwaggerGen) addParamInternal(ctx context.Context, op *openapi3.Operation, in, name, def, example string, isArray bool, description i18n.MessageKey, deprecated bool, msgArgs ...interface{}) {
	required := false
	if in == "path" {
		required = true
	}
	var defValue interface{}
	if def != "" {
		defValue = def
	}
	var exampleValue interface{}
	if example != "" {
		exampleValue = example
	}
	value := &openapi3.Schema{
		Type:    &openapi3.Types{openapi3.TypeString},
		Default: defValue,
		Example: exampleValue,
	}
	if isArray {
		value.Type = &openapi3.Types{openapi3.TypeArray}
		value.Items = &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Type:    &openapi3.Types{openapi3.TypeString},
				Default: defValue,
				Example: exampleValue,
			},
		}
	}
	op.Parameters = append(op.Parameters, &openapi3.ParameterRef{
		Value: &openapi3.Parameter{
			In:          in,
			Name:        name,
			Required:    required,
			Deprecated:  deprecated,
			Description: i18n.Expand(ctx, description, msgArgs...),
			Schema: &openapi3.SchemaRef{
				Value: value,
			},
		},
	})
}

func (sg *SwaggerGen) addFilters(ctx context.Context, route *Route, op *openapi3.Operation) {
	if route.FilterFactory != nil {
		fields := route.FilterFactory.NewFilter(ctx).Fields()
		sort.Strings(fields)
		for _, field := range fields {
			sg.AddParam(ctx, op, "query", field, "", "", i18n.APIFilterParamDesc, false)
		}
		sg.AddParam(ctx, op, "query", "sort", "", "", i18n.APIFilterSortDesc, false)
		sg.AddParam(ctx, op, "query", "ascending", "", "", i18n.APIFilterAscendingDesc, false)
		sg.AddParam(ctx, op, "query", "descending", "", "", i18n.APIFilterDescendingDesc, false)
		sg.AddParam(ctx, op, "query", "skip", "", "", i18n.APIFilterSkipDesc, false, sg.options.APIMaxFilterSkip)
		sg.AddParam(ctx, op, "query", "limit", "", sg.options.APIDefaultFilterLimit, i18n.APIFilterLimitDesc, false, sg.options.APIMaxFilterLimit)
		sg.AddParam(ctx, op, "query", "count", "", "", i18n.APIFilterCountDesc, false)
		if sg.options.SupportFieldRedaction {
			sg.AddParam(ctx, op, "query", "fields", "", "", i18n.APIFilterFieldsDesc, false)
		}
	}
}

func (sg *SwaggerGen) addRoute(ctx context.Context, doc *openapi3.T, route *Route) {
	var routeDescription string
	pi := sg.getPathItem(doc, route.Path)
	if route.PreTranslatedDescription != "" {
		routeDescription = route.PreTranslatedDescription
	} else {
		routeDescription = i18n.Expand(ctx, route.Description)
		if routeDescription == "" && sg.options.PanicOnMissingDescription {
			log.Panic(i18n.NewError(ctx, i18n.MsgRouteDescriptionMissing, route.Name).Error())
		}
	}
	op := &openapi3.Operation{
		Description: routeDescription,
		OperationID: route.Name,
		Responses:   openapi3.NewResponses(),
		Deprecated:  route.Deprecated,
		Tags:        []string{route.Tag},
	}
	if route.Method != http.MethodGet && route.Method != http.MethodDelete {
		sg.initInput(op)
		switch {
		case route.FormUploadHandler != nil:
			// add a mix of JSON and upload form input (though we don't support JSON in the form)
			sg.addInput(ctx, doc, route, op)
			sg.addUploadFormInput(ctx, op, route.FormParams)
		case route.FormParams != nil:
			// we only want form input and not JSON input
			sg.addURLEncodedFormInput(ctx, op, route.FormParams)
		default:
			// for all other handlers/inputs just add the standard JSON input
			sg.addInput(ctx, doc, route, op)
		}
	}
	sg.addOutput(ctx, doc, route, op)
	for _, p := range route.PathParams {
		example := p.Example
		if p.ExampleFromConf != "" {
			example = config.GetString(p.ExampleFromConf)
		}
		sg.AddParam(ctx, op, "path", p.Name, p.Default, example, p.Description, false)
	}
	for _, q := range route.QueryParams {
		example := q.Example
		if q.ExampleFromConf != "" {
			example = config.GetString(q.ExampleFromConf)
		}
		sg.addParamInternal(ctx, op, "query", q.Name, q.Default, example, q.IsArray, q.Description, q.Deprecated)
	}
	sg.AddParam(ctx, op, "header", "Request-Timeout", sg.options.DefaultRequestTimeout.String(), "", i18n.APIRequestTimeoutDesc, false)

	sg.addFilters(ctx, route, op)

	if sg.options.RouteCustomizations != nil {
		sg.options.RouteCustomizations(ctx, sg, route, op)
	}
	switch route.Method {
	case http.MethodGet:
		pi.Get = op
	case http.MethodPut:
		pi.Put = op
	case http.MethodPost:
		pi.Post = op
	case http.MethodDelete:
		pi.Delete = op
	case http.MethodPatch:
		pi.Patch = op
	}
}
