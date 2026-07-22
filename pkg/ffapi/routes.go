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
	"io"
	"net/http"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/i18n"
)

// SchemaGenerator is passed into the JSONInputSchema advanced customization function, to give
// access to tools like object schema generation, for an anyOf schema for example
type SchemaGenerator func(o interface{}) (*openapi3.SchemaRef, error)

// Route defines each API operation on the REST API of Firefly
// Having a standard pluggable layer here on top of Gorilla allows us to autmoatically
// maintain the OpenAPI specification in-line with the code, while retaining the
// power of the Gorilla mux without a deep abstraction layer.
type Route struct {
	// Name is the operation name that will go into the Swagger definition, and prefix input/output schema
	Name string
	// Path is a Gorilla mux path spec
	Path string
	// PathParams is a list of documented path parameters
	PathParams []*PathParam
	// QueryParams is a list of documented query parameters
	QueryParams []*QueryParam
	// FormParams is a list of documented multi-part form parameters - combine with FormUploadHandler
	FormParams []*FormParam
	// Method is the HTTP method
	Method string
	// Description is a message key to a translatable description of the operation
	Description i18n.MessageKey
	// PreTranslatedDescription is a string describing the operation - used for programmatically generated routes where a built-in string translation is not available
	PreTranslatedDescription string
	// FilterFactory models the filter fields that can be specified on the API, and will automatically be parsed
	FilterFactory QueryFactory
	// JSONInputDecoder is a function that does the decoding completely - needed (as this was written pre-generics) for handling arrays
	JSONInputDecoder func(req *http.Request, body io.Reader) (interface{}, error)
	// JSONInputValue is a function that returns a pointer to a structure to take JSON input
	JSONInputValue func() interface{}
	// JSONInputMask are fields that aren't available for users to supply on input
	JSONInputMask []string
	// JSONInputSchema is a custom schema definition, for the case where the auto-gen + mask isn't good enough
	JSONInputSchema func(ctx context.Context, schemaGen SchemaGenerator) (*openapi3.SchemaRef, error)
	// JSONOutputSchema is a custom schema definition, for the case where the auto-gen + mask isn't good enough
	JSONOutputSchema func(ctx context.Context, schemaGen SchemaGenerator) (*openapi3.SchemaRef, error)
	// JSONOutputValue is a function that returns a pointer to a structure to take JSON output
	JSONOutputValue func() interface{}
	// JSONOutputCodes is the success response codes that could be returned by the API. Error codes are explicitly not supported by the framework since they could be subject to change by the errors thrown or how errors are handled.
	JSONOutputCodes []int
	// JSONHandler is a function for handling JSON content type input. Input/Output objects are returned by JSONInputValue/JSONOutputValue funcs
	JSONHandler func(r *APIRequest) (output interface{}, err error)
	// FormUploadHandler takes a single file upload, and returns a JSON object
	FormUploadHandler func(r *APIRequest) (output interface{}, err error)
	// StreamHandler allows for custom request handling with explicit stream (io.ReadCloser) responses
	StreamHandler func(r *APIRequest) (output io.ReadCloser, err error)
	// CustomResponseRefs allows for specifying custom responses for a route
	CustomResponseRefs map[string]*openapi3.ResponseRef
	// Deprecated whether this route is deprecated
	Deprecated bool
	// Tag a category identifier for this route in the generated OpenAPI spec
	Tag string
	// ExcludeFromOpenAPI if true, this route will not be included in the generated OpenAPI specification
	ExcludeFromOpenAPI bool
	// Extensions allows extension of the route struct by individual microservices
	Extensions interface{}
}

// APIVersion defines a version of the API
// it contains a list of routes and associated metadata for the OpenAPI spec generation
type APIVersion struct {
	Routes       []*Route               // the routes
	Tags         openapi3.Tags          // (OPTIONAL) description and external documentation of tags used in the routes, all tags in this array will need to be used by at least one of routes
	ExternalDocs *openapi3.ExternalDocs // (OPTIONAL) external documentation of this API version
}

// PathParam is a description of a path parameter
type PathParam struct {
	// Name is the name of the parameter, from the Gorilla path mux
	Name string
	// Default is the value that will be used in the case no value is supplied
	Default string
	// Example is a field to fill in, in the helper UI
	Example string
	// ExampleFromConf is a field to fill in, in the helper UI, from the runtime configuration
	ExampleFromConf config.RootKey
	// Description is a message key to a translatable description of the parameter
	Description i18n.MessageKey
}

// QueryParam is a description of a path parameter
type QueryParam struct {
	// Name is the name of the parameter, from the Gorilla path mux
	Name string
	// IsBool if this is a boolean query
	IsBool bool
	// IsArray if this is an array parameter (can be specified multiple times)
	IsArray bool
	// Default is the value that will be used in the case no value is supplied
	Default string
	// Example is a field to fill in, in the helper UI
	Example string
	// ExampleFromConf is a field to fill in, in the helper UI, from the runtime configuration
	ExampleFromConf config.RootKey
	// Description is a message key to a translatable description of the parameter
	Description i18n.MessageKey
	// Deprecated whether this param is deprecated
	Deprecated bool
}

// FormParam is a description of a multi-part form parameter
type FormParam struct {
	// Name is the name of the parameter, from the Gorilla path mux
	Name string
	// Description is a message key to a translatable description of the parameter
	Description i18n.MessageKey
}
