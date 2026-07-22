// Copyright © 2025 Kaleido, Inc.
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
	"regexp"
	"strings"

	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/prometheus/client_golang/prometheus"
)

const gaugeVec = "gaugeVec"

var supportedTypes = []string{"counterVec", gaugeVec, "histogramVec", "summaryVec"}

const fireflySystemLabelsPrefix = "ff_"
const namespaceLabel = fireflySystemLabelsPrefix + "namespace"

// In prometheus, there is no harm to define labels for future proofing
// as the number of time series (the ways metrics data are stored)
// is determined by the number of unique label VALUE combinations, not label name
// therefore, pre-defining a label but never emitting metrics with that label
// will not increase the number of time series
var optionalFireflySystemLabels = []string{namespaceLabel}

func checkAndUpdateLabelNames(ctx context.Context, labelNames []string, withDefaultLabels bool) []string {
	validLabelNames := []string{}

	if withDefaultLabels {
		// with default system labels
		validLabelNames = append(validLabelNames, optionalFireflySystemLabels...)
	}

	// check label names are not clashing with system prefix
	for _, labelName := range labelNames {
		if strings.HasPrefix(labelName, fireflySystemLabelsPrefix) {
			err := i18n.NewError(ctx, i18n.MsgMetricsInvalidLabel, labelName, fireflySystemLabelsPrefix)
			log.L(ctx).Error(err.Error())
		} else {
			validLabelNames = append(validLabelNames, labelName)
		}
	}

	return validLabelNames
}

func checkAndUpdateLabels(ctx context.Context, labelNames []string, labels map[string]string, defaultLabels *FireflyDefaultLabels) map[string]string {
	validLabels := make(map[string]string)
	// check label names are not clashing with system prefix
	for _, labelName := range labelNames {
		if !strings.HasPrefix(labelName, fireflySystemLabelsPrefix) {
			// fetch value of the labels when it's not a system default labels
			validLabels[labelName] = labels[labelName]
		} else if labelName == namespaceLabel {
			if defaultLabels != nil {
				validLabels[namespaceLabel] = defaultLabels.Namespace
			} else {
				log.L(ctx).Warnf("No value is provided for system label %s", namespaceLabel)
				validLabels[namespaceLabel] = ""

			}
		}

	}

	return validLabels
}

type prometheusMetricsManager struct {
	namespace  string
	subsystem  string
	registerer prometheus.Registerer
	metricsMap map[string]*prometheusMetric
}

type prometheusMetric struct {
	Metric     prometheus.Collector
	LabelNames []string
}

func (pmm *prometheusMetricsManager) NewCounterMetric(ctx context.Context, metricName string, helpText string, withDefaultLabels bool) {
	pmm.NewCounterMetricWithLabels(ctx, metricName, helpText, []string{}, withDefaultLabels)
}
func (pmm *prometheusMetricsManager) NewCounterMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string, withDefaultLabels bool) {
	pmm.registerMetrics(ctx, regInfo{
		Type:       "counterVec",
		Name:       metricName,
		HelpText:   helpText,
		LabelNames: checkAndUpdateLabelNames(ctx, labelNames, withDefaultLabels),
	})
}
func (pmm *prometheusMetricsManager) NewGaugeMetric(ctx context.Context, metricName string, helpText string, withDefaultLabels bool) {
	pmm.NewGaugeMetricWithLabels(ctx, metricName, helpText, []string{}, withDefaultLabels)
}
func (pmm *prometheusMetricsManager) NewGaugeMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string, withDefaultLabels bool) {
	pmm.registerMetrics(ctx, regInfo{
		Type:       gaugeVec,
		Name:       metricName,
		HelpText:   helpText,
		LabelNames: checkAndUpdateLabelNames(ctx, labelNames, withDefaultLabels),
	})
}
func (pmm *prometheusMetricsManager) NewHistogramMetric(ctx context.Context, metricName string, helpText string, buckets []float64, withDefaultLabels bool) {
	pmm.NewHistogramMetricWithLabels(ctx, metricName, helpText, buckets, []string{}, withDefaultLabels)
}
func (pmm *prometheusMetricsManager) NewHistogramMetricWithLabels(ctx context.Context, metricName string, helpText string, buckets []float64, labelNames []string, withDefaultLabels bool) {
	pmm.registerMetrics(ctx, regInfo{
		Type:       "histogramVec",
		Name:       metricName,
		HelpText:   helpText,
		Buckets:    buckets,
		LabelNames: checkAndUpdateLabelNames(ctx, labelNames, withDefaultLabels),
	})
}
func (pmm *prometheusMetricsManager) NewSummaryMetric(ctx context.Context, metricName string, helpText string, withDefaultLabels bool) {
	pmm.NewSummaryMetricWithLabels(ctx, metricName, helpText, []string{}, withDefaultLabels)
}
func (pmm *prometheusMetricsManager) NewSummaryMetricWithLabels(ctx context.Context, metricName string, helpText string, labelNames []string, withDefaultLabels bool) {
	pmm.registerMetrics(ctx, regInfo{
		Type:       "summaryVec",
		Name:       metricName,
		HelpText:   helpText,
		LabelNames: checkAndUpdateLabelNames(ctx, labelNames, withDefaultLabels),
	})
}

type regInfo struct {
	Type       string
	Name       string
	HelpText   string
	LabelNames []string  // should only be provided for metrics types with vectors
	Buckets    []float64 // only applicable for histogram
}

func (pmm *prometheusMetricsManager) registerMetrics(ctx context.Context, mr regInfo) {

	nameRegex := regexp.MustCompile(allowedNameStringRegex)
	isValidNameString := nameRegex.MatchString(mr.Name)
	if !isValidNameString {
		err := i18n.NewError(ctx, i18n.MsgMetricsInvalidName, mr.Name)
		log.L(ctx).Warnf("Failed to initialize metric %s due to error: %s", mr.Name, err.Error())
		return
	}

	for _, typeName := range supportedTypes {
		if _, ok := pmm.metricsMap[mr.Name+"_"+typeName]; ok {
			err := i18n.NewError(ctx, i18n.MsgMetricsDuplicateName, mr.Name)
			log.L(ctx).Warnf("Failed to initialize metric %s due to error: %s", mr.Name, err.Error())
			return

		}
	}
	internalMapIndex := mr.Name + "_" + mr.Type
	if mr.HelpText == "" {
		err := i18n.NewError(ctx, i18n.MsgMetricsHelpTextMissing)
		log.L(ctx).Warnf("Failed to initialize metric %s due to error: %s", mr.Name, err.Error())
		return
	}
	switch mr.Type {
	case "counterVec":
		pmm.metricsMap[internalMapIndex] = &prometheusMetric{
			Metric: prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: pmm.namespace,
				Subsystem: pmm.subsystem,
				Name:      mr.Name,
				Help:      mr.HelpText,
			}, mr.LabelNames),
			LabelNames: mr.LabelNames,
		}
	case gaugeVec:
		pmm.metricsMap[internalMapIndex] = &prometheusMetric{
			Metric: prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: pmm.namespace,
				Subsystem: pmm.subsystem,
				Name:      mr.Name,
				Help:      mr.HelpText,
			}, mr.LabelNames),
			LabelNames: mr.LabelNames,
		}
	case "histogramVec":
		pmm.metricsMap[internalMapIndex] = &prometheusMetric{
			Metric: prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Namespace: pmm.namespace,
				Subsystem: pmm.subsystem,
				Name:      mr.Name,
				Help:      mr.HelpText,
				Buckets:   mr.Buckets,
			}, mr.LabelNames),
			LabelNames: mr.LabelNames,
		}

	case "summaryVec":
		pmm.metricsMap[internalMapIndex] = &prometheusMetric{
			Metric: prometheus.NewSummaryVec(prometheus.SummaryOpts{
				Namespace: pmm.namespace,
				Subsystem: pmm.subsystem,
				Name:      mr.Name,
				Help:      mr.HelpText,
			}, mr.LabelNames),
			LabelNames: mr.LabelNames,
		}

	}
	pmm.registerer.MustRegister(pmm.metricsMap[internalMapIndex].Metric)
}

func (pmm *prometheusMetricsManager) SetGaugeMetric(ctx context.Context, metricName string, number float64, defaultLabels *FireflyDefaultLabels) {
	pmm.SetGaugeMetricWithLabels(ctx, metricName, number, make(map[string]string), defaultLabels)
}

func (pmm *prometheusMetricsManager) IncGaugeMetric(ctx context.Context, metricName string, defaultLabels *FireflyDefaultLabels) {
	pmm.IncGaugeMetricWithLabels(ctx, metricName, make(map[string]string), defaultLabels)
}

func (pmm *prometheusMetricsManager) DecGaugeMetric(ctx context.Context, metricName string, defaultLabels *FireflyDefaultLabels) {
	pmm.DecGaugeMetricWithLabels(ctx, metricName, make(map[string]string), defaultLabels)
}

func (pmm *prometheusMetricsManager) SetGaugeMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string, defaultLabels *FireflyDefaultLabels) {
	m, ok := pmm.metricsMap[metricName+"_"+gaugeVec]
	if !ok {
		log.L(ctx).Warnf("metric with name: '%s' and type: '%s' is not found", metricName, "gaugeVec")
	} else {
		collector := m.Metric

		collector.(*prometheus.GaugeVec).With(checkAndUpdateLabels(ctx, m.LabelNames, labels, defaultLabels)).Set(number)
	}
}

func (pmm *prometheusMetricsManager) IncGaugeMetricWithLabels(ctx context.Context, metricName string, labels map[string]string, defaultLabels *FireflyDefaultLabels) {
	m, ok := pmm.metricsMap[metricName+"_"+gaugeVec]
	if !ok {
		log.L(ctx).Warnf("metric with name: '%s' and type: '%s' is not found", metricName, "gaugeVec")
	} else {
		collector := m.Metric

		collector.(*prometheus.GaugeVec).With(checkAndUpdateLabels(ctx, m.LabelNames, labels, defaultLabels)).Inc()
	}
}

func (pmm *prometheusMetricsManager) DecGaugeMetricWithLabels(ctx context.Context, metricName string, labels map[string]string, defaultLabels *FireflyDefaultLabels) {
	m, ok := pmm.metricsMap[metricName+"_"+gaugeVec]
	if !ok {
		log.L(ctx).Warnf("metric with name: '%s' and type: '%s' is not found", metricName, "gaugeVec")
	} else {
		collector := m.Metric

		collector.(*prometheus.GaugeVec).With(checkAndUpdateLabels(ctx, m.LabelNames, labels, defaultLabels)).Dec()
	}
}

func (pmm *prometheusMetricsManager) IncCounterMetric(ctx context.Context, metricName string, defaultLabels *FireflyDefaultLabels) {
	pmm.IncCounterMetricWithLabels(ctx, metricName, make(map[string]string), defaultLabels)

}

func (pmm *prometheusMetricsManager) IncCounterMetricWithLabels(ctx context.Context, metricName string, labels map[string]string, defaultLabels *FireflyDefaultLabels) {
	m, ok := pmm.metricsMap[metricName+"_counterVec"]
	if !ok {
		log.L(ctx).Warnf("metric with name: '%s' and type: '%s' is not found", metricName, "counterVec")
	} else {
		collector := m.Metric
		collector.(*prometheus.CounterVec).With(checkAndUpdateLabels(ctx, m.LabelNames, labels, defaultLabels)).Inc()
	}
}

func (pmm *prometheusMetricsManager) ObserveHistogramMetric(ctx context.Context, metricName string, number float64, defaultLabels *FireflyDefaultLabels) {
	pmm.ObserveHistogramMetricWithLabels(ctx, metricName, number, make(map[string]string), defaultLabels)

}

func (pmm *prometheusMetricsManager) ObserveHistogramMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string, defaultLabels *FireflyDefaultLabels) {
	m, ok := pmm.metricsMap[metricName+"_histogramVec"]
	if !ok {
		log.L(ctx).Warnf("metric with name: '%s' and type: '%s' is not found", metricName, "histogramVec")
	} else {
		collector := m.Metric
		collector.(*prometheus.HistogramVec).With(checkAndUpdateLabels(ctx, m.LabelNames, labels, defaultLabels)).Observe(number)
	}
}

func (pmm *prometheusMetricsManager) ObserveSummaryMetric(ctx context.Context, metricName string, number float64, defaultLabels *FireflyDefaultLabels) {
	pmm.ObserveSummaryMetricWithLabels(ctx, metricName, number, make(map[string]string), defaultLabels)
}

func (pmm *prometheusMetricsManager) ObserveSummaryMetricWithLabels(ctx context.Context, metricName string, number float64, labels map[string]string, defaultLabels *FireflyDefaultLabels) {
	m, ok := pmm.metricsMap[metricName+"_summaryVec"]
	if !ok {
		log.L(ctx).Warnf("metric with name: '%s' and type: '%s' is not found", metricName, "summaryVec")
	} else {
		collector := m.Metric
		collector.(*prometheus.SummaryVec).With(checkAndUpdateLabels(ctx, m.LabelNames, labels, defaultLabels)).Observe(number)
	}
}
