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

package eventstreams

import (
	"context"
	"crypto/tls"
	"database/sql/driver"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/wsserver"
)

type DistributionMode = fftypes.FFEnum

var (
	DistributionModeBroadcast   = fftypes.FFEnumValue("distmode", "broadcast")
	DistributionModeLoadBalance = fftypes.FFEnumValue("distmode", "load_balance")
)

type WebSocketConfig struct {
	DistributionMode *DistributionMode `ffstruct:"wsconfig" json:"distributionMode,omitempty"`
}

type webSocketDispatcherFactory[CT EventStreamSpec, DT any] struct {
	esm *esManager[CT, DT]
}

// Store in DB as JSON
func (wc *WebSocketConfig) Scan(src interface{}) error {
	return fftypes.JSONScan(src, wc)
}

// Store in DB as JSON
func (wc *WebSocketConfig) Value() (driver.Value, error) {
	if wc == nil {
		return nil, nil
	}
	return fftypes.JSONValue(wc)
}

func (wsf *webSocketDispatcherFactory[CT, DT]) Validate(ctx context.Context, conf *Config[CT, DT], spec CT, _ map[string]*tls.Config, phase LifecyclePhase) error {
	wsc := spec.WebSocketConf()
	setDefaults := phase == LifecyclePhaseStarting
	return checkSet(ctx, setDefaults, "distributionMode", &wsc.DistributionMode, conf.Defaults.WebSocketDefaults.DefaultDistributionMode, func(v fftypes.FFEnum) bool { return fftypes.FFEnumValid(ctx, "distmode", v) })
}

type webSocketAction[DT any] struct {
	topic      string
	spec       *WebSocketConfig
	wsProtocol wsserver.Protocol
}

func (wsf *webSocketDispatcherFactory[CT, DT]) NewDispatcher(_ context.Context, _ *Config[CT, DT], spec CT) Dispatcher[DT] {
	return &webSocketAction[DT]{
		spec:       spec.WebSocketConf(),
		wsProtocol: wsf.esm.wsProtocol,
		topic:      *spec.ESFields().Name,
	}
}

func (w *webSocketAction[DT]) AttemptDispatch(ctx context.Context, attempt int, batch *EventBatch[DT]) error {
	var err error

	isBroadcast := *w.spec.DistributionMode == DistributionModeBroadcast

	if isBroadcast {
		w.wsProtocol.Broadcast(ctx, w.topic, batch)
		return nil
	}

	_, err = w.wsProtocol.RoundTrip(ctx, w.topic, batch)
	// Pass back any exception due
	if err != nil {
		log.L(ctx).Infof("WebSocket event batch %d delivery failed (len=%d,attempt=%d): %s", batch.BatchNumber, len(batch.Events), attempt, err)
		return err
	}
	log.L(ctx).Infof("WebSocket event batch %d complete (len=%d,attempt=%d)", batch.BatchNumber, len(batch.Events), attempt)
	return nil
}
