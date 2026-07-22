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
	"errors"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/mocks/metricmocks"
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestEnableDBMetrics(t *testing.T) {
	defer func() { metricsRegistry = nil; metricsManager = nil }()

	ctx := context.Background()
	mr := metric.NewPrometheusMetricsRegistry("ut")

	err := EnableDBMetrics(ctx, mr)
	assert.NoError(t, err)

	// second call is a no-op
	err = EnableDBMetrics(ctx, mr)
	assert.NoError(t, err)

	// test error from NewMetricsManagerForSubsystem
	metricsRegistry = nil
	metricsManager = nil
	mmr := metricmocks.NewMetricsRegistry(t)
	mmr.On("NewMetricsManagerForSubsystem", mock.Anything, mock.Anything).Return(nil, errors.New("pop"))
	err = EnableDBMetrics(ctx, mmr)
	assert.Error(t, err)
	assert.Regexp(t, "pop", err)
	assert.Nil(t, metricsRegistry)
	assert.Nil(t, metricsManager)
}

func TestObserveOpNoManager(t *testing.T) {
	defer func() { metricsRegistry = nil; metricsManager = nil }()

	db := &Database{dbName: "testdb"}
	// should not panic when metricsManager is nil
	db.observeOp(context.Background(), "things", "query", time.Millisecond)
	db.incOpError(context.Background(), "things", "query")
	db.observeTx(context.Background(), time.Millisecond, "commit")
	db.observeMigration(context.Background(), time.Millisecond)
}

func TestObserveOpWithManager(t *testing.T) {
	defer func() { metricsRegistry = nil; metricsManager = nil }()

	ctx := context.Background()
	mr := metric.NewPrometheusMetricsRegistry("ut_observe")
	err := EnableDBMetrics(ctx, mr)
	assert.NoError(t, err)

	db := &Database{dbName: "testdb"}

	db.observeOp(ctx, "things", "query", 10*time.Millisecond)
	db.observeOp(ctx, "things", "insert", 5*time.Millisecond)
	db.observeOp(ctx, "things", "update", 3*time.Millisecond)
	db.observeOp(ctx, "things", "delete", 2*time.Millisecond)
	db.observeOp(ctx, "things", "count", 1*time.Millisecond)
	db.observeOp(ctx, "things", "exec", 1*time.Millisecond)

	db.incOpError(ctx, "things", "query")
	db.incOpError(ctx, "things", "insert")

	db.observeTx(ctx, 50*time.Millisecond, "commit")
	db.observeTx(ctx, 20*time.Millisecond, "rollback")

	db.observeMigration(ctx, 100*time.Millisecond)
}
