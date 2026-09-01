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

package dbsql

import (
	"context"
	"time"

	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/metric"
)

const (
	subsystem = "dbsql"

	metricOpDuration        = "operation_duration_seconds"
	metricTxDuration        = "tx_duration_seconds"
	metricMigrationDuration = "migration_duration_seconds"
	metricTxCommit          = "tx_commit_total"
	metricTxRollback        = "tx_rollback_total"
	metricErrors            = "errors_total"
)

var (
	metricsRegistry metric.MetricsRegistry
	metricsManager  metric.MetricsManager

	// 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s
	dbOperationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	// 10ms, 50ms, 100ms, 500ms, 1s, 2.5s, 5s, 10s, 30s, 60s
	dbTxBuckets = []float64{0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60}
)

// EnableDBMetrics registers prometheus metrics for database operations.
// Must be called before any Database.Init to ensure metrics are wired up.
func EnableDBMetrics(ctx context.Context, _metricsRegistry metric.MetricsRegistry) (err error) {
	if metricsRegistry != nil {
		log.L(ctx).Warn("DB metrics are already enabled, skipping")
		return nil
	}
	metricsRegistry = _metricsRegistry

	metricsManager, err = metricsRegistry.NewMetricsManagerForSubsystem(ctx, subsystem)
	if err != nil {
		metricsRegistry = nil
		metricsManager = nil
		return err
	}

	metricsManager.NewHistogramMetricWithLabels(ctx, metricOpDuration,
		"Latency of individual DB operations", dbOperationBuckets,
		[]string{"db", "table", "operation"}, false)
	metricsManager.NewHistogramMetricWithLabels(ctx, metricTxDuration,
		"Duration of DB transactions from begin to commit/rollback", dbTxBuckets,
		[]string{"db", "outcome"}, false)
	metricsManager.NewSummaryMetricWithLabels(ctx, metricMigrationDuration,
		"Duration of DB schema migrations", []string{"db"}, false)
	metricsManager.NewCounterMetricWithLabels(ctx, metricTxCommit,
		"Total committed DB transactions", []string{"db"}, false)
	metricsManager.NewCounterMetricWithLabels(ctx, metricTxRollback,
		"Total rolled-back DB transactions", []string{"db"}, false)
	metricsManager.NewCounterMetricWithLabels(ctx, metricErrors,
		"Total DB operation errors", []string{"db", "table", "operation"}, false)

	return nil
}

func (s *Database) observeOp(ctx context.Context, table, operation string, elapsed time.Duration) {
	if metricsManager == nil {
		return
	}
	metricsManager.ObserveHistogramMetricWithLabels(ctx, metricOpDuration, elapsed.Seconds(),
		map[string]string{"db": s.dbName, "table": table, "operation": operation}, nil)
}

func (s *Database) incOpError(ctx context.Context, table, operation string) {
	if metricsManager == nil {
		return
	}
	metricsManager.IncCounterMetricWithLabels(ctx, metricErrors,
		map[string]string{"db": s.dbName, "table": table, "operation": operation}, nil)
}

func (s *Database) observeTx(ctx context.Context, elapsed time.Duration, outcome string) {
	if metricsManager == nil {
		return
	}
	metricsManager.ObserveHistogramMetricWithLabels(ctx, metricTxDuration, elapsed.Seconds(),
		map[string]string{"db": s.dbName, "outcome": outcome}, nil)
	if outcome == "commit" {
		metricsManager.IncCounterMetricWithLabels(ctx, metricTxCommit,
			map[string]string{"db": s.dbName}, nil)
	} else {
		metricsManager.IncCounterMetricWithLabels(ctx, metricTxRollback,
			map[string]string{"db": s.dbName}, nil)
	}
}

func (s *Database) observeMigration(ctx context.Context, elapsed time.Duration) {
	if metricsManager == nil {
		return
	}
	metricsManager.ObserveSummaryMetricWithLabels(ctx, metricMigrationDuration, elapsed.Seconds(),
		map[string]string{"db": s.dbName}, nil)
}
