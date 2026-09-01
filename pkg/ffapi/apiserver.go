// Copyright © 2026 Kaleido, Inc.
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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3gen"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gorilla/mux"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/fftls"
	"github.com/hyperledger-firefly/common/pkg/httpserver"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/sirupsen/logrus"
)

const APIServerMetricsSubSystemName = "api_server_rest"

// APIServer is an opinionated use of the HTTP Server facilities in common, to provide
// an API server with:
// - a set of routes defining API endpoints
// - a swagger endpoint on /api
// - optional metrics endpoint on /metrics (configurable)
type APIServer interface {
	Serve(ctx context.Context) error
	Started() <-chan struct{}
	MuxRouter(ctx context.Context) (*mux.Router, error)
	APIPublicURL() string // valid to call after server is successfully started
	MonitoringPublicURL() string
}

type apiServer[T any] struct {
	started chan struct{}

	defaultFilterLimit        uint64
	maxFilterLimit            uint64
	maxFilterSkip             uint64
	requestTimeout            time.Duration
	requestMaxTimeout         time.Duration
	apiPublicURL              string
	apiDynamicPublicURLHeader string
	alwaysPaginate            bool
	handleYAML                bool
	monitoringEnabled         bool
	metricsPath               string
	livenessPath              string
	loggingPath               string
	monitoringPublicURL       string
	mux                       *mux.Router
	oah                       *OpenAPIHandlerFactory
	baseSwaggerGenOptions     SwaggerGenOptions

	APIServerOptions[T]
}

type APIServerOptions[T any] struct {
	MetricsRegistry                   metric.MetricsRegistry
	MetricsSubsystemName              string
	Routes                            []*Route // move to use VersionedAPIs for support of Tags and ExternalDocs
	VersionedAPIs                     *VersionedAPIs
	MonitoringRoutes                  []*Route
	EnrichRequest                     func(r *APIRequest) (T, error)
	Description                       string
	APIConfig                         config.Section
	MonitoringConfig                  config.Section
	CORSConfig                        config.Section
	FavIcon16                         []byte
	FavIcon32                         []byte
	PanicOnMissingDescription         bool
	SupportFieldRedaction             bool
	HandleYAML                        bool
	SwaggerExportComponentOpts        *openapi3gen.ExportComponentSchemasOptions
	SwaggerAdditionalSchemaCustomizer func(name string, t reflect.Type, tag reflect.StructTag, schema *openapi3.Schema) error
}

type VersionedAPIs struct {
	DefaultVersion string                 // must be set to a version string if there are more than 1 API versions provided
	APIVersions    map[string]*APIVersion // a list of APIVersions, with the key being the version string
}

type APIServerRouteExt[T any] struct {
	JSONHandler   func(*APIRequest, T) (output interface{}, err error)
	UploadHandler func(*APIRequest, T) (output interface{}, err error)
	StreamHandler func(*APIRequest, T) (output io.ReadCloser, err error)
}

// NewAPIServer makes a new server, with the specified configuration, and
// the supplied wrapper function - which will inject
func NewAPIServer[T any](ctx context.Context, options APIServerOptions[T]) APIServer {
	if options.APIConfig == nil {
		panic("APIConfig is required")
	}

	if options.MonitoringConfig == nil {
		panic("MonitoringConfig is required")
	}

	as := &apiServer[T]{
		defaultFilterLimit: options.APIConfig.GetUint64(ConfAPIDefaultFilterLimit),
		maxFilterLimit:     options.APIConfig.GetUint64(ConfAPIMaxFilterLimit),
		maxFilterSkip:      options.APIConfig.GetUint64(ConfAPIMaxFilterSkip),
		requestTimeout:     options.APIConfig.GetDuration(ConfAPIRequestTimeout),
		requestMaxTimeout:  options.APIConfig.GetDuration(ConfAPIRequestMaxTimeout),
		monitoringEnabled:  options.MonitoringConfig.GetBool(ConfMonitoringServerEnabled),
		metricsPath:        options.MonitoringConfig.GetString(ConfMonitoringServerMetricsPath),
		livenessPath:       options.MonitoringConfig.GetString(ConfMonitoringServerLivenessPath),
		loggingPath:        options.MonitoringConfig.GetString(ConfMonitoringServerLoggingPath),

		alwaysPaginate:            options.APIConfig.GetBool(ConfAPIAlwaysPaginate),
		handleYAML:                options.HandleYAML,
		apiDynamicPublicURLHeader: options.APIConfig.GetString(ConfAPIDynamicPublicURLHeader),
		APIServerOptions:          options,
		started:                   make(chan struct{}),
		baseSwaggerGenOptions: SwaggerGenOptions{
			Title:                      options.Description,
			Version:                    "1.0",
			PanicOnMissingDescription:  options.PanicOnMissingDescription,
			DefaultRequestTimeout:      options.APIConfig.GetDuration(ConfAPIRequestTimeout),
			SupportFieldRedaction:      options.SupportFieldRedaction,
			ExportComponentOpts:        options.SwaggerExportComponentOpts,
			AdditionalSchemaCustomizer: options.SwaggerAdditionalSchemaCustomizer,
			APIDefaultFilterLimit:      options.APIConfig.GetString(ConfAPIDefaultFilterLimit),
			APIMaxFilterLimit:          options.APIConfig.GetUint(ConfAPIMaxFilterLimit),
			APIMaxFilterSkip:           options.APIConfig.GetUint(ConfAPIMaxFilterSkip),
		},
	}
	if as.FavIcon16 == nil {
		as.FavIcon16 = ffLogo16
	}
	if as.FavIcon32 == nil {
		as.FavIcon32 = ffLogo16
	}

	_ = as.MetricsRegistry.NewHTTPMetricsInstrumentationsForSubsystem(
		ctx,
		as.metricsSubsystemName(),
		true,
		prometheus.DefBuckets,
		map[string]string{},
	)
	return as
}

func (as *apiServer[T]) metricsSubsystemName() string {
	metricsSubsystemName := APIServerMetricsSubSystemName
	if as.MetricsSubsystemName != "" {
		metricsSubsystemName = as.MetricsSubsystemName
	}
	return metricsSubsystemName
}

// Can be called before Serve, but MUST use the background context if so
func (as *apiServer[T]) MuxRouter(ctx context.Context) (*mux.Router, error) {
	if as.mux == nil {
		var err error
		if as.mux, err = as.createMuxRouter(ctx); err != nil {
			return nil, err
		}

	}
	return as.mux, nil
}

// Serve is the main entry point for the API Server
func (as *apiServer[T]) Serve(ctx context.Context) (err error) {
	started := false
	defer func() {
		// Ensure we don't leave the Started() channel indefinitely in the case of an error
		if !started {
			close(as.started)
		}
	}()

	httpErrChan := make(chan error)
	monitoringErrChan := make(chan error)

	apiMux, err := as.MuxRouter(ctx)
	if err != nil {
		return err
	}
	apiHTTPServer, err := httpserver.NewHTTPServer(ctx, "api", apiMux, httpErrChan, as.APIConfig, as.CORSConfig, &httpserver.ServerOptions{
		MaximumRequestTimeout: as.requestMaxTimeout,
	})
	if err != nil {
		return err
	}
	as.apiPublicURL = buildPublicURL(as.APIConfig, apiHTTPServer.Addr())
	as.oah.StaticPublicURL = as.apiPublicURL
	go apiHTTPServer.ServeHTTP(ctx)

	if as.monitoringEnabled {
		monitoringMux, err := as.createMonitoringMuxRouter(ctx)
		if err != nil {
			return err
		}
		monitoringHTTPServer, err := httpserver.NewHTTPServer(ctx, "monitoring", monitoringMux, monitoringErrChan, as.MonitoringConfig, as.CORSConfig, &httpserver.ServerOptions{
			MaximumRequestTimeout: as.requestMaxTimeout,
		})
		if err != nil {
			return err
		}
		as.monitoringPublicURL = buildPublicURL(as.MonitoringConfig, monitoringHTTPServer.Addr())
		go monitoringHTTPServer.ServeHTTP(ctx)
	}

	started = true
	close(as.started)
	return as.waitForServerStop(httpErrChan, monitoringErrChan)
}

func (as *apiServer[T]) Started() <-chan struct{} {
	return as.started
}

func (as *apiServer[T]) APIPublicURL() string {
	return as.apiPublicURL
}

func (as *apiServer[T]) MonitoringPublicURL() string {
	return as.monitoringPublicURL
}

func (as *apiServer[T]) waitForServerStop(httpErrChan, monitoringErrChan chan error) error {
	select {
	case err := <-httpErrChan:
		return err
	case err := <-monitoringErrChan:
		return err
	}
}

func buildPublicURL(conf config.Section, a net.Addr) string {
	publicURL := conf.GetString(httpserver.HTTPConfPublicURL)
	if publicURL == "" {
		proto := "https"
		tlsConfig := conf.SubSection("tls")
		if !tlsConfig.GetBool(fftls.HTTPConfTLSEnabled) {
			proto = "http"
		}
		publicURL = fmt.Sprintf("%s://%s", proto, a.String())
	}
	return publicURL
}

func (as *apiServer[T]) routeHandler(hf *HandlerFactory, route *Route) http.HandlerFunc {
	// We extend the base ffapi functionality, with standardized DB filter support for all core resources.
	// We also pass the Orchestrator context through
	ext := route.Extensions.(*APIServerRouteExt[T])
	switch {
	case ext.StreamHandler != nil:
		route.StreamHandler = func(r *APIRequest) (output io.ReadCloser, err error) {
			er, err := as.EnrichRequest(r)
			if err != nil {
				return nil, err
			}
			return ext.StreamHandler(r, er)
		}
	case ext.JSONHandler != nil:
		route.JSONHandler = func(r *APIRequest) (output interface{}, err error) {
			er, err := as.EnrichRequest(r)
			if err != nil {
				return nil, err
			}
			return ext.JSONHandler(r, er)
		}
	}

	return hf.RouteHandler(route)
}

func (as *apiServer[T]) handlerFactory(logLevel logrus.Level) *HandlerFactory {
	hf := &HandlerFactory{
		DefaultRequestTimeout: as.requestTimeout,
		MaxTimeout:            as.requestMaxTimeout,
		DefaultFilterLimit:    as.defaultFilterLimit,
		MaxFilterSkip:         as.maxFilterSkip,
		MaxFilterLimit:        as.maxFilterLimit,
		SupportFieldRedaction: as.SupportFieldRedaction,
		AlwaysPaginate:        as.alwaysPaginate,
		HandleYAML:            as.handleYAML,
	}
	hf.SetAPIEntryLoggingLevel(logLevel)
	return hf
}

func (as *apiServer[T]) createMuxRouter(ctx context.Context) (*mux.Router, error) {
	r := mux.NewRouter().UseEncodedPath()
	hf := as.handlerFactory(logrus.InfoLevel)

	as.oah = &OpenAPIHandlerFactory{
		BaseSwaggerGenOptions: as.baseSwaggerGenOptions,
		StaticPublicURL:       as.apiPublicURL, // this is most likely not yet set, we'll ensure its set later on
	}

	if as.monitoringEnabled {
		h, _ := as.MetricsRegistry.GetHTTPMetricsInstrumentationsMiddlewareForSubsystem(ctx, as.metricsSubsystemName())
		r.Use(h)
	}

	defaultAPIVersionObject := &APIVersion{
		Routes: as.Routes,
	}
	defaultAPIVersion := "v1"
	if as.VersionedAPIs != nil {
		if len(as.Routes) > 0 {
			return nil, i18n.NewError(ctx, i18n.MsgCannotUseRouteAndVersionedAPI)
		}
		if len(as.VersionedAPIs.APIVersions) == 0 {
			return nil, i18n.NewError(ctx, i18n.MsgMissingVersionedAPI)
		}
		if len(as.VersionedAPIs.APIVersions) == 1 {
			for apiVersion := range as.VersionedAPIs.APIVersions {
				as.VersionedAPIs.DefaultVersion = apiVersion
			}
		}
		if as.VersionedAPIs.DefaultVersion == "" {
			return nil, i18n.NewError(ctx, i18n.MsgMissingDefaultAPIVersion)
		}
		if as.VersionedAPIs.APIVersions[as.VersionedAPIs.DefaultVersion] == nil || len(as.VersionedAPIs.APIVersions[as.VersionedAPIs.DefaultVersion].Routes) == 0 {
			return nil, i18n.NewError(ctx, i18n.MsgNonExistDefaultAPIVersion, as.VersionedAPIs.DefaultVersion)
		}
		defaultAPIVersionObject = as.VersionedAPIs.APIVersions[as.VersionedAPIs.DefaultVersion]
		defaultAPIVersion = as.VersionedAPIs.DefaultVersion
		for apiVersion, routes := range as.VersionedAPIs.APIVersions {
			if err := as.addRoutesForVersion(ctx, r, hf, apiVersion, routes); err != nil {
				return nil, err
			}
		}
	} else {
		if err := as.addRoutesForVersion(ctx, r, hf, defaultAPIVersion, defaultAPIVersionObject); err != nil {
			return nil, err
		}
	}

	r.HandleFunc(`/api/swagger.yaml`, hf.APIWrapper(as.oah.OpenAPIHandlerVersioned(fmt.Sprintf(`/api/%s`, defaultAPIVersion), OpenAPIFormatYAML, defaultAPIVersionObject)))
	r.HandleFunc(`/api/swagger.json`, hf.APIWrapper(as.oah.OpenAPIHandlerVersioned(fmt.Sprintf(`/api/%s`, defaultAPIVersion), OpenAPIFormatJSON, defaultAPIVersionObject)))
	r.HandleFunc(`/api/openapi.yaml`, hf.APIWrapper(as.oah.OpenAPIHandlerVersioned(fmt.Sprintf(`/api/%s`, defaultAPIVersion), OpenAPIFormatYAML, defaultAPIVersionObject)))
	r.HandleFunc(`/api/openapi.json`, hf.APIWrapper(as.oah.OpenAPIHandlerVersioned(fmt.Sprintf(`/api/%s`, defaultAPIVersion), OpenAPIFormatJSON, defaultAPIVersionObject)))
	r.HandleFunc(`/api`, hf.APIWrapper(as.oah.SwaggerUIHandler(`/api/openapi.yaml`)))
	r.HandleFunc(`/favicon{any:.*}.png`, favIconsHandler(as.FavIcon16, as.FavIcon32))

	r.NotFoundHandler = hf.APIWrapper(as.notFoundHandler)
	return r, nil
}

func (as *apiServer[T]) addRoutesForVersion(ctx context.Context, r *mux.Router, hf *HandlerFactory, apiVersion string, apiVersionObject *APIVersion) error {
	for _, route := range apiVersionObject.Routes {
		ce, ok := route.Extensions.(*APIServerRouteExt[T])
		if !ok {
			panic(fmt.Sprintf("invalid route extensions: %t", route.Extensions))
		}
		if ce.UploadHandler != nil {
			route.FormUploadHandler = func(r *APIRequest) (output interface{}, err error) {
				er, err := as.EnrichRequest(r)
				if err != nil {
					return nil, err
				}
				return ce.UploadHandler(r, er)
			}
		}
		if ce.JSONHandler != nil || ce.UploadHandler != nil || ce.StreamHandler != nil {
			if strings.HasPrefix(route.Path, "/") {
				return i18n.NewError(ctx, i18n.MsgRoutePathNotStartWithSlash, route.Path)
			}
			r.HandleFunc(fmt.Sprintf("/api/%s/%s", apiVersion, route.Path), as.routeHandler(hf, route)).
				Methods(route.Method)
		}
	}

	// adding open api documentation for this api version
	r.HandleFunc(fmt.Sprintf("/api/%s/openapi.yaml", apiVersion), hf.APIWrapper(as.oah.OpenAPIHandlerVersioned(fmt.Sprintf("/api/%s", apiVersion), OpenAPIFormatYAML, apiVersionObject)))
	r.HandleFunc(fmt.Sprintf("/api/%s/openapi.json", apiVersion), hf.APIWrapper(as.oah.OpenAPIHandlerVersioned(fmt.Sprintf("/api/%s", apiVersion), OpenAPIFormatJSON, apiVersionObject)))

	return nil
}

func (as *apiServer[T]) notFoundHandler(res http.ResponseWriter, req *http.Request) (status int, err error) {
	res.Header().Add("Content-Type", "application/json")
	return 404, i18n.NewError(req.Context(), i18n.Msg404NotFound)
}

func (as *apiServer[T]) noContentResponder(res http.ResponseWriter, _ *http.Request) {
	res.WriteHeader(http.StatusNoContent)
}

type loggingChanges struct {
	Level *logSettingChange `json:"level,omitempty"`
}

type logSettingChange struct {
	Previous any `json:"previous,omitempty"`
	New      any `json:"new"`
}

func LoggingSettingsHandler(res http.ResponseWriter, req *http.Request) (status int, err error) {
	if req.Method != http.MethodPut {
		return http.StatusMethodNotAllowed, i18n.NewError(req.Context(), i18n.MsgMethodNotAllowed)
	}

	changes := &loggingChanges{}

	logLevel := req.URL.Query().Get("level")
	if logLevel != "" {
		currentLogLevel := log.GetLevel()
		l := log.L(log.WithLogFieldsMap(req.Context(), map[string]string{"new_level": logLevel}))
		switch strings.ToLower(logLevel) {
		case "error":
		case "debug":
		case "trace":
		case "info":
		case "warn":
		case "warning":
			// noop - all valid levels
		default:
			l.Warn("invalid log level")
			return http.StatusBadRequest, i18n.NewError(req.Context(), i18n.MsgInvalidLogLevel, logLevel)
		}
		l.Warn("changing log level")
		log.SetLevel(logLevel)
		changes.Level = &logSettingChange{
			Previous: currentLogLevel,
			New:      logLevel,
		}
	}

	res.Header().Add("Content-Type", "application/json")
	changesJSON, _ := json.Marshal(changes)
	_, err = res.Write(changesJSON)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusAccepted, nil
}

func (as *apiServer[T]) createMonitoringMuxRouter(ctx context.Context) (*mux.Router, error) {
	r := mux.NewRouter().UseEncodedPath()
	// This ensures logs aren't polluted with monitoring API requests such as metrics or probes
	hf := as.handlerFactory(logrus.TraceLevel)

	h, err := as.MetricsRegistry.HTTPHandler(ctx, promhttp.HandlerOpts{})
	if err != nil {
		panic(err)
	}
	r.Path(as.metricsPath).Handler(h)
	r.Path(as.loggingPath).Handler(hf.APIWrapper(LoggingSettingsHandler))
	r.HandleFunc(as.livenessPath, as.noContentResponder)

	for _, route := range as.MonitoringRoutes {
		path := route.Path
		if strings.HasPrefix(route.Path, "/") {
			return nil, i18n.NewError(ctx, i18n.MsgRoutePathNotStartWithSlash, route.Path)
		}
		r.HandleFunc("/"+path, as.routeHandler(hf, route)).Methods(route.Method)
	}

	r.NotFoundHandler = hf.APIWrapper(as.notFoundHandler)
	return r, nil
}
