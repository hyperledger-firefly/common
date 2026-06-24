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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/ghodss/yaml"
	"github.com/gorilla/mux"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/httpserver"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/sirupsen/logrus"
)

const DefaultRequestIDHeader = "X-FireFly-Request-ID"

var (
	requestIDHeader = DefaultRequestIDHeader
)

// RequestIDHeader returns the header name used to pass the request ID for
// ffapi server and ffresty client requests.
func RequestIDHeader() string {
	return requestIDHeader
}

// SetRequestIDHeader configures the header used to pass the request ID between
// ffapi server and ffresty client requests. Should be set at initialization of
// a process.
func SetRequestIDHeader(header string) {
	requestIDHeader = header
}

type (
	CtxHeadersKey     struct{}
	CtxFFRequestIDKey struct{}
)

type HandlerFunction func(res http.ResponseWriter, req *http.Request) (status int, err error)

type HandlerFactory struct {
	DefaultRequestTimeout time.Duration
	MaxTimeout            time.Duration
	DefaultFilterLimit    uint64
	MaxFilterSkip         uint64
	MaxFilterLimit        uint64
	HandleYAML            bool
	PassthroughHeaders    []string
	AlwaysPaginate        bool
	SupportFieldRedaction bool
	BasePath              string
	BasePathParams        []*PathParam

	apiEntryLoggingLevel logrus.Level // the log level at which entry/exit logging is enabled at all (does not affect trace logging)
}

var ffMsgCodeExtractor = regexp.MustCompile(`^(FF\d+):`)

type multipartState struct {
	mpr        *multipart.Reader
	formParams map[string]string
	part       *Multipart
	close      func()
}

func (hs *HandlerFactory) SetAPIEntryLoggingLevel(logLevel logrus.Level) {
	hs.apiEntryLoggingLevel = logLevel
}

func (hs *HandlerFactory) getFilePart(req *http.Request) (*multipartState, error) {
	formParams := make(map[string]string)
	ctx := req.Context()
	l := log.L(ctx)
	mpr, err := req.MultipartReader()
	if err != nil {
		return nil, i18n.WrapError(ctx, err, i18n.MsgMultiPartFormReadError)
	}
	for {
		part, err := mpr.NextPart()
		if err != nil {
			return nil, i18n.WrapError(ctx, err, i18n.MsgMultiPartFormReadError)
		}
		if part.FileName() == "" {
			value, _ := io.ReadAll(part)
			formParams[part.FormName()] = string(value)
		} else {
			l.Debugf("Processing multi-part upload. Field='%s' Filename='%s'", part.FormName(), part.FileName())
			mp := &Multipart{
				Data:     part,
				Filename: part.FileName(),
				Mimetype: part.Header.Get("Content-Disposition"),
			}
			return &multipartState{
				mpr:        mpr,
				formParams: formParams,
				part:       mp,
				close:      func() { _ = part.Close() },
			}, nil
		}
	}
}

func (hs *HandlerFactory) getFormParams(req *http.Request) (map[string]string, error) {
	if err := req.ParseForm(); err != nil {
		return nil, i18n.WrapError(req.Context(), err, i18n.MsgParseFormError)
	}
	form := make(map[string]string, len(req.Form))
	for k, v := range req.Form {
		if len(v) < 1 {
			continue
		}

		if len(v) > 1 {
			return nil, i18n.WrapError(req.Context(), fmt.Errorf("multi-value form parameters for '%s' are not currently supported", k), i18n.MsgParseFormError)
		}

		form[k] = v[0]
	}
	return form, nil
}

func (hs *HandlerFactory) getParams(req *http.Request, route *Route) (queryParams, pathParams map[string]string, queryArrayParams map[string][]string) {
	queryParams = make(map[string]string)
	pathParams = make(map[string]string)
	queryArrayParams = make(map[string][]string)
	v := mux.Vars(req)
	for _, pp := range route.PathParams {
		paramUnescaped, err := url.QueryUnescape(v[pp.Name]) // Gorilla mux assures this works
		if err == nil {
			pathParams[pp.Name] = paramUnescaped
		}
	}
	for _, pp := range hs.BasePathParams {
		paramUnescaped, err := url.QueryUnescape(v[pp.Name])
		if err == nil {
			pathParams[pp.Name] = paramUnescaped
		}
	}

	for _, qp := range route.QueryParams {
		val, exists := req.URL.Query()[qp.Name]
		if qp.IsBool {
			if exists && (len(val) == 0 || val[0] == "" || strings.EqualFold(val[0], "true")) {
				val = []string{"true"}
			} else {
				val = []string{"false"}
			}
		}
		if exists && len(val) > 0 {
			if qp.IsArray {
				queryArrayParams[qp.Name] = val
			} else {
				queryParams[qp.Name] = val[0]
			}
		}
	}
	return queryParams, pathParams, queryArrayParams
}

func (hs *HandlerFactory) RoutePath(route *Route) string {
	return path.Join("/", hs.BasePath, route.Path)
}

func (hs *HandlerFactory) RouteHandler(route *Route) http.HandlerFunc {
	// Check the mandatory parts are ok at startup time
	return hs.APIWrapper(func(res http.ResponseWriter, req *http.Request) (int, error) {

		var jsonInput interface{}
		if route.JSONInputValue != nil {
			jsonInput = route.JSONInputValue()
		}
		var queryParams, pathParams, formParams map[string]string
		var queryArrayParams map[string][]string
		var multipart *multipartState
		contentType := req.Header.Get("Content-Type")
		var err error
		if req.Method != http.MethodGet && req.Method != http.MethodDelete {
			switch {
			case strings.HasPrefix(strings.ToLower(contentType), "multipart/form-data") && route.FormUploadHandler != nil:
				multipart, err = hs.getFilePart(req)
				if err != nil {
					return 400, err
				}
				defer multipart.close()
			case hs.HandleYAML && strings.HasPrefix(req.Header.Get("Content-Type"), "application/x-yaml"):
				var jsonBytes []byte
				yamlBytes, err := io.ReadAll(req.Body)
				if err == nil {
					jsonBytes, err = yaml.YAMLToJSON(yamlBytes)
				}
				if err != nil {
					return 400, i18n.NewError(req.Context(), i18n.MsgRequestYAMLInvalid, err)
				}
				req.Body = io.NopCloser(bytes.NewBuffer(jsonBytes))
				req.Header.Set("Content-Type", "application/json; charset=utf8")
				fallthrough
			case strings.HasPrefix(strings.ToLower(contentType), "application/json"):
				if route.JSONInputDecoder != nil {
					jsonInput, err = route.JSONInputDecoder(req, req.Body)
				} else if jsonInput != nil {
					d := json.NewDecoder(req.Body)
					d.UseNumber()
					err = d.Decode(&jsonInput)
				}
			case strings.HasPrefix(strings.ToLower(contentType), "application/x-www-form-urlencoded"):
				formParams, err = hs.getFormParams(req)
				if err != nil {
					return 400, err
				}
			case strings.HasPrefix(strings.ToLower(contentType), "text/plain"):
			default:
				return 415, i18n.NewError(req.Context(), i18n.MsgInvalidContentType, contentType)
			}
		}

		status := 400 // if fail parsing input
		var output interface{}
		if err == nil {
			queryParams, pathParams, queryArrayParams = hs.getParams(req, route)
		}

		var filter AndFilter
		if err == nil && route.FilterFactory != nil {
			filter, err = hs.buildFilter(req, route.FilterFactory)
		}

		if err == nil {
			r := &APIRequest{
				Req:            req,
				PP:             pathParams,
				QP:             queryParams,
				QAP:            queryArrayParams,
				Filter:         filter,
				Input:          jsonInput,
				SuccessStatus:  http.StatusOK,
				AlwaysPaginate: hs.AlwaysPaginate,

				// res.Header() returns a map which is a ref type so handler header edits are persisted
				ResponseHeaders: res.Header(),
			}
			if len(route.JSONOutputCodes) > 0 {
				r.SuccessStatus = route.JSONOutputCodes[0]
			}
			switch {
			case multipart != nil:
				r.FP = multipart.formParams
				r.Part = multipart.part
				output, err = route.FormUploadHandler(r)
			case route.StreamHandler != nil:
				output, err = route.StreamHandler(r)
			case formParams != nil:
				r.FP = formParams
				fallthrough
			default:
				output, err = route.JSONHandler(r)
			}
			status = r.SuccessStatus // Can be updated by the route
		}
		if err == nil && multipart != nil {
			// Catch the case that someone puts form fields after the file in a multi-part body.
			// We don't support that, so that we can stream through the core rather than having
			// to hold everything in memory.
			trailing, expectEOF := multipart.mpr.NextPart()
			if expectEOF == nil {
				err = i18n.NewError(req.Context(), i18n.MsgFieldsAfterFile, trailing.FormName())
			}
		}
		if err == nil {
			status, err = hs.handleOutput(req.Context(), res, status, output)
		}
		return status, err
	})
}

func (hs *HandlerFactory) handleOutput(ctx context.Context, res http.ResponseWriter, status int, output interface{}) (int, error) {
	vOutput := reflect.ValueOf(output)
	outputKind := vOutput.Kind()
	isPointer := outputKind == reflect.Ptr
	invalid := outputKind == reflect.Invalid
	isNil := output == nil || invalid || (isPointer && vOutput.IsNil())
	var reader io.ReadCloser
	var marshalErr error
	if !isNil && vOutput.CanInterface() {
		reader, _ = vOutput.Interface().(io.ReadCloser)
	}
	switch {
	case isNil:
		if status != 204 {
			return 404, i18n.NewError(ctx, i18n.Msg404NoResult)
		}
		res.WriteHeader(204)
	case reader != nil:
		defer reader.Close()
		if res.Header().Get("Content-Type") == "" {
			res.Header().Add("Content-Type", "application/octet-stream")
		}
		res.WriteHeader(status)
		_, marshalErr = io.Copy(res, reader)
	default:
		res.Header().Add("Content-Type", "application/json")
		res.WriteHeader(status)
		marshalErr = json.NewEncoder(res).Encode(output)
	}
	if marshalErr != nil {
		err := i18n.WrapError(ctx, marshalErr, i18n.MsgResponseMarshalError)
		log.L(ctx).Error(err.Error())
		return 500, err
	}
	return status, nil
}

func CalcRequestTimeout(req *http.Request, defaultTimeout, maxTimeout time.Duration) time.Duration {
	// Configure a server-side timeout on each request, to try and avoid cases where the API requester
	// times out, and we continue to churn indefinitely processing the request.
	// Long-running processes should be dispatched asynchronously (API returns 202 Accepted asap),
	// and the caller can either listen on the websocket for updates, or poll the status of the affected object.
	// This is dependent on the context being passed down through to all blocking operations down the stack
	// (while avoiding passing the context to asynchronous tasks that are dispatched as a result of the request)
	reqTimeout := defaultTimeout
	reqTimeoutHeader := req.Header.Get("Request-Timeout")
	if reqTimeoutHeader != "" {
		customTimeout, err := fftypes.ParseDurationString(reqTimeoutHeader, time.Second /* default is seconds */)
		if err != nil {
			log.L(req.Context()).Warnf("Invalid Request-Timeout header '%s': %s", reqTimeoutHeader, err)
		} else {
			reqTimeout = time.Duration(customTimeout)
			if reqTimeout > maxTimeout {
				reqTimeout = maxTimeout
			}
		}
	}
	return reqTimeout
}

func (hs *HandlerFactory) getTimeout(req *http.Request) time.Duration {
	return CalcRequestTimeout(req, hs.DefaultRequestTimeout, hs.MaxTimeout)
}

func (hs *HandlerFactory) APIWrapper(handler HandlerFunction) http.HandlerFunc {
	if hs.apiEntryLoggingLevel == 0 {
		hs.SetAPIEntryLoggingLevel(logrus.InfoLevel)
	}
	return func(res http.ResponseWriter, req *http.Request) {
		reqTimeout := hs.getTimeout(req)
		ctx, cancel := context.WithTimeout(req.Context(), reqTimeout)
		httpReqID := req.Header.Get(RequestIDHeader())
		if httpReqID == "" {
			httpReqID = fftypes.ShortID()
		}
		ctx = withRequestID(ctx, httpReqID)
		ctx = withPassthroughHeaders(ctx, req, hs.PassthroughHeaders)
		ctx = log.WithLogFields(ctx, "httpreq", httpReqID)

		req = req.WithContext(ctx)
		defer cancel()

		// Wrap the request itself in a log wrapper, that gives minimal request/response and timing info
		addrFields := map[string]string{}
		if remoteAddr := httpserver.RemoteAddr(ctx); remoteAddr != nil {
			addrFields["remote"] = remoteAddr.String()
		}
		if localAddr := httpserver.LocalAddr(ctx); localAddr != nil {
			addrFields["local"] = localAddr.String()
		}
		l := log.L(log.WithLogFieldsMap(ctx, addrFields))
		l.Logf(hs.apiEntryLoggingLevel, "--> %s %s", req.Method, req.URL.Path)
		startTime := time.Now()
		status, err := handler(res, req)
		durationMS := float64(time.Since(startTime)) / float64(time.Millisecond)
		if err != nil {
			if ffe, ok := (interface{}(err)).(i18n.FFError); ok {
				if logrus.IsLevelEnabled(logrus.DebugLevel) {
					l.Debugf("%s:\n%s", ffe.Error(), ffe.StackTrace())
				}
				status = ffe.HTTPStatus()
			} else {
				// Routers don't need to tweak the status code when sending errors.
				// .. either the FF12345 error they raise is mapped to a status hint
				ffMsgCodeExtract := ffMsgCodeExtractor.FindStringSubmatch(err.Error())
				if len(ffMsgCodeExtract) >= 2 {
					if statusHint, ok := i18n.GetStatusHint(ffMsgCodeExtract[1]); ok {
						status = statusHint
					}
				}
			}

			// If the context is done, we wrap in 408
			if status != http.StatusRequestTimeout {
				select {
				case <-ctx.Done():
					l.Errorf("Request failed and context is closed. Returning %d (overriding %d): %s", http.StatusRequestTimeout, status, err)
					status = http.StatusRequestTimeout
					err = i18n.WrapError(ctx, err, i18n.MsgRequestTimeout, httpReqID, durationMS)
				default:
				}
			}

			// ... or we default to 500
			if status < 300 {
				status = 500
			}
			l.Logf(hs.apiEntryLoggingLevel, "<-- %s %s [%d] (%.2fms): %s", req.Method, req.URL.Path, status, durationMS, err)
			res.Header().Add("Content-Type", "application/json")
			res.WriteHeader(status)
			_ = json.NewEncoder(res).Encode(&fftypes.RESTError{
				Error: err.Error(),
			})
		} else {
			l.Logf(hs.apiEntryLoggingLevel, "<-- %s %s [%d] (%.2fms)", req.Method, req.URL.Path, status, durationMS)
		}
	}
}

func withPassthroughHeaders(ctx context.Context, req *http.Request, passthroughHeaders []string) context.Context {
	headers := http.Header{}
	for _, key := range passthroughHeaders {
		headers.Set(key, req.Header.Get(key))
	}
	return context.WithValue(ctx, CtxHeadersKey{}, headers)
}

func withRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, CtxFFRequestIDKey{}, requestID)
}
