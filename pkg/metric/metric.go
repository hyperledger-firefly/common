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

package metric

import (
	"context"
	"net/http"
	"regexp"
	"sync"

	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	muxprom "gitlab.com/hfuss/mux-prometheus/pkg/middleware"
)

var allowedNameStringRegex = `^[a-zA-Z]+[a-zA-Z0-9_]*[a-zA-Z0-9]$`
var ffMetricsPrefix = "ff"

const compulsoryComponentLabel = "component"

/** Metrics names should follow the convention documented in https://prometheus.io/docs/practices/naming/. Below is an example breakdown of the term mapping:
* Example metric: ff                _  api_server_rest    _ requests         _ total   {component="tm"              , method       =  "Get"          ...}
                  ff                _  token              _ mint_duration    _ seconds {component=""                , status       =  "Success"      ...}
* Mapping       : <firefly prefix>  _  <subsystem>        _ <metric target>  _ <unit>  {component=<component name>  , <label name> =  <label value>  ...}
*/

// MetricsRegistry contains all metrics defined in a micro-service.
//   - All metrics will have the default "ff" prefix
//   - A component name can be provided to add a "ff_component" label to separate metrics with same names from other components
//   - Metrics are defined in subsystems, a subsystem can only use one of the two options for creating metrics:
//     1. create a metrics manager to defined custom metrics manually
//     2. use out-of-box predefined metrics for a common resource type (e.g. HTTP request)
type MetricsRegistry interface {
	// NewMetricsManagerForSubsystem returns a MetricsManager which can be used to add custom metrics
	NewMetricsManagerForSubsystem(ctx context.Context, subsystem string) (MetricsManager, error)

	// HTTPHandler returns the HTTP handler of this metrics registry
	HTTPHandler(ctx context.Context, handlerOpts promhttp.HandlerOpts) (http.Handler, error)

	// Predefined HTTP Metrics  Instrumentations
	// NewHTTPMetricsInstrumentationsForSubsystem adds predefined HTTP metrics to a subsystem
	NewHTTPMetricsInstrumentationsForSubsystem(ctx context.Context, subsystem string, useRouteTemplate bool, reqDurationBuckets []float64, labels map[string]string) error
	// GetHTTPMetricsInstrumentationsMiddlewareForSubsystem returns the HTTP middleware of a subsystem that used predefined HTTP metrics
	GetHTTPMetricsInstrumentationsMiddlewareForSubsystem(ctx context.Context, subsystem string) (func(next http.Handler) http.Handler, error)

	// MustRegisterCollector allows for registering a customer collector within the metrics registry, outside of the manager/subsystem context
	MustRegisterCollector(collector prometheus.Collector)
	// GetGatherer returns the gatherer of the metrics registry, allowing for programmatic metrics exporting
	GetGatherer() prometheus.Gatherer
}

type FireflyDefaultLabels struct {
	Namespace string
}

// MetricsManager is used to defined and emit metrics in a subsystem
// All functions in this interface should swallow errors as metrics are auxiliary
type MetricsManager interface {
	// functions for defining metrics
	NewCounterMetric(ctx context.Context, metricName string, helpText string, withDefaultLabels bool)
	NewCounterMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string, withDefaultLabels bool)
	NewGaugeMetric(ctx context.Context, metricName string, helpText string, withDefaultLabels bool)
	NewGaugeMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string, withDefaultLabels bool)
	NewHistogramMetric(ctx context.Context, metricName string, helpText string, buckets []float64, withDefaultLabels bool)
	NewHistogramMetricWithLabels(ctx context.Context, metricName string, helpText string, buckets []float64, labelNames []string, withDefaultLabels bool)
	NewSummaryMetric(ctx context.Context, metricName string, helpText string, withDefaultLabels bool)
	NewSummaryMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string, withDefaultLabels bool)

	// functions for emitting metrics
	SetGaugeMetric(ctx context.Context, metricName string, number float64, defaultLabels *FireflyDefaultLabels)
	SetGaugeMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string, defaultLabels *FireflyDefaultLabels)
	IncGaugeMetric(ctx context.Context, metricName string, defaultLabels *FireflyDefaultLabels)
	IncGaugeMetricWithLabels(ctx context.Context, metricName string, labels map[string]string, defaultLabels *FireflyDefaultLabels)
	DecGaugeMetric(ctx context.Context, metricName string, defaultLabels *FireflyDefaultLabels)
	DecGaugeMetricWithLabels(ctx context.Context, metricName string, labels map[string]string, defaultLabels *FireflyDefaultLabels)

	IncCounterMetric(ctx context.Context, metricName string, defaultLabels *FireflyDefaultLabels)
	IncCounterMetricWithLabels(ctx context.Context, metricName string, labels map[string]string, defaultLabels *FireflyDefaultLabels)
	ObserveHistogramMetric(ctx context.Context, metricName string, number float64, defaultLabels *FireflyDefaultLabels)
	ObserveHistogramMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string, defaultLabels *FireflyDefaultLabels)
	ObserveSummaryMetric(ctx context.Context, metricName string, number float64, defaultLabels *FireflyDefaultLabels)
	ObserveSummaryMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string, defaultLabels *FireflyDefaultLabels)
}

type Options struct {
	MetricsPrefix string
}

func NewPrometheusMetricsRegistry(componentName string /*component name will be added to all metrics as a label*/) MetricsRegistry {
	return NewPrometheusMetricsRegistryWithOptions(componentName, Options{})
}

func NewPrometheusMetricsRegistryWithOptions(componentName string /*component name will be added to all metrics as a label*/, opts Options) MetricsRegistry {
	registry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWith(prometheus.Labels{compulsoryComponentLabel: componentName}, registry)

	// register default cpu & go metrics by default
	registerer.MustRegister(collectors.NewGoCollector())
	registerer.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerer.MustRegister(collectors.NewBuildInfoCollector())

	metricsPrefix := ffMetricsPrefix
	if opts.MetricsPrefix != "" {
		metricsPrefix = opts.MetricsPrefix
	}

	return &prometheusMetricsRegistry{
		namespace:                      metricsPrefix,
		registry:                       registry,
		registerer:                     registerer,
		managerMap:                     make(map[string]MetricsManager),
		httpMetricsInstrumentationsMap: make(map[string]*muxprom.Instrumentation),
	}
}

type prometheusMetricsRegistry struct {
	regMux                         sync.Mutex
	registry                       *prometheus.Registry
	registerer                     prometheus.Registerer
	namespace                      string
	managerMap                     map[string]MetricsManager
	httpMetricsInstrumentationsMap map[string]*muxprom.Instrumentation
}

// Custom prometheus metrics manager
func (pmr *prometheusMetricsRegistry) NewMetricsManagerForSubsystem(ctx context.Context, subsystem string) (MetricsManager, error) {
	pmr.regMux.Lock()
	defer pmr.regMux.Unlock()
	nameRegex := regexp.MustCompile(allowedNameStringRegex)
	isValidNameString := nameRegex.MatchString(subsystem)
	if !isValidNameString {
		return nil, i18n.NewError(ctx, i18n.MsgMetricsInvalidSubsystemName, subsystem)
	}

	if _, ok := pmr.managerMap[subsystem]; ok {
		return nil, i18n.NewError(ctx, i18n.MsgMetricsDuplicateSubsystemName, subsystem)
	}
	if _, ok := pmr.httpMetricsInstrumentationsMap[subsystem]; ok {
		return nil, i18n.NewError(ctx, i18n.MsgMetricsDuplicateSubsystemName, subsystem)
	}

	pmr.managerMap[subsystem] = &prometheusMetricsManager{
		namespace:  pmr.namespace,
		subsystem:  subsystem,
		registerer: pmr.registerer,
		metricsMap: make(map[string]*prometheusMetric),
	}
	return pmr.managerMap[subsystem], nil
}

// Predefined metrics for http router
func (pmr *prometheusMetricsRegistry) NewHTTPMetricsInstrumentationsForSubsystem(ctx context.Context, subsystem string, useRouteTemplate bool, reqDurationBuckets []float64, labels map[string]string) error {
	pmr.regMux.Lock()
	defer pmr.regMux.Unlock()
	if labels == nil {
		labels = make(map[string]string)
	}

	nameRegex := regexp.MustCompile(allowedNameStringRegex)
	isValidNameString := nameRegex.MatchString(subsystem)
	if !isValidNameString {
		return i18n.NewError(ctx, i18n.MsgMetricsInvalidSubsystemName, subsystem)
	}
	if _, ok := pmr.managerMap[subsystem]; ok {
		return i18n.NewError(ctx, i18n.MsgMetricsDuplicateSubsystemName, subsystem)
	}
	if _, ok := pmr.httpMetricsInstrumentationsMap[subsystem]; ok {
		return i18n.NewError(ctx, i18n.MsgMetricsDuplicateSubsystemName, subsystem)
	}
	httpInstrumentation := muxprom.NewCustomInstrumentation(
		useRouteTemplate,
		pmr.namespace,
		subsystem,
		reqDurationBuckets,
		labels,
		pmr.registerer,
	)
	pmr.httpMetricsInstrumentationsMap[subsystem] = httpInstrumentation
	return nil
}

func (pmr *prometheusMetricsRegistry) HTTPHandler(ctx context.Context, handlerOpts promhttp.HandlerOpts) (http.Handler, error) {
	if len(pmr.managerMap) == 0 && len(pmr.httpMetricsInstrumentationsMap) == 0 {
		return nil, i18n.NewError(ctx, i18n.MsgMetricsEmptyRegistry)
	}
	return promhttp.InstrumentMetricHandler(pmr.registerer, promhttp.HandlerFor(pmr.registry, handlerOpts)), nil
}

func (pmr *prometheusMetricsRegistry) GetHTTPMetricsInstrumentationsMiddlewareForSubsystem(ctx context.Context, subsystem string) (func(next http.Handler) http.Handler, error) {
	httpInstrumentation, ok := pmr.httpMetricsInstrumentationsMap[subsystem]
	if !ok {
		return nil, i18n.NewError(ctx, i18n.MsgMetricsSubsystemHTTPInstrumentationNotFound)
	}
	return httpInstrumentation.Middleware, nil
}

func (pmr *prometheusMetricsRegistry) MustRegisterCollector(collector prometheus.Collector) {
	pmr.registerer.MustRegister(collector)
}

func (pmr *prometheusMetricsRegistry) GetGatherer() prometheus.Gatherer {
	return pmr.registry
}
