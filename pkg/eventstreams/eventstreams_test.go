// Copyright © 2022 Kaleido, Inc.
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
	"fmt"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func newTestEventStream(t *testing.T, extraSetup ...func(mdb *mockPersistence)) (context.Context, *eventStream[*GenericEventStream, testData], *mockEventSource, func()) {
	extraSetup = append(extraSetup, func(mdb *mockPersistence) {
		mdb.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil)
	})
	ctx, mgr, mes, done := newMockESManager(t, extraSetup...)
	es, err := mgr.initEventStream(&GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo(t.Name()),
			Status: ptrTo(EventStreamStatusStopped),
		},
	})
	assert.NoError(t, err)

	return ctx, es, mes, done
}

func TestEventStreamFields(t *testing.T) {

	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
	}
	assert.Equal(t, es.GetID(), es.GetID())
	t1 := fftypes.Now()
	es.SetCreated(t1)
	assert.Equal(t, t1, es.Created)
	t2 := fftypes.Now()
	es.SetUpdated(t2)
	assert.Equal(t, t2, es.Updated)
}

func TestWebSocketConfigSerialization(t *testing.T) {

	var wc *WebSocketConfig
	v, err := wc.Value()
	assert.Nil(t, v)
	assert.NoError(t, err)

	wc = &WebSocketConfig{
		DistributionMode: &DistributionModeBroadcast,
	}
	v, err = wc.Value()
	assert.NotNil(t, v)
	assert.NoError(t, err)

	wc1 := &WebSocketConfig{}
	err = wc1.Scan(v)
	assert.NoError(t, err)
	assert.Equal(t, DistributionModeBroadcast, *wc1.DistributionMode)

	wc2 := &WebSocketConfig{}
	err = wc2.Scan(string(v.([]byte)))
	assert.NoError(t, err)
	assert.Equal(t, DistributionModeBroadcast, *wc1.DistributionMode)

	var wc3 *WebSocketConfig
	err = wc3.Scan(nil)
	assert.NoError(t, err)
	assert.Nil(t, wc3)

}

func TestWebhookConfigSerialization(t *testing.T) {

	var wc *WebhookConfig
	v, err := wc.Value()
	assert.Nil(t, v)
	assert.NoError(t, err)

	u := "http://example.com"
	wc = &WebhookConfig{
		URL: &u,
	}
	v, err = wc.Value()
	assert.NotNil(t, v)
	assert.NoError(t, err)

	wc1 := &WebhookConfig{}
	err = wc1.Scan(v)
	assert.NoError(t, err)
	assert.Equal(t, "http://example.com", *wc1.URL)

	wc2 := &WebhookConfig{}
	err = wc2.Scan(string(v.([]byte)))
	assert.NoError(t, err)
	assert.Equal(t, "http://example.com", *wc1.URL)

	var wc3 *WebhookConfig
	err = wc3.Scan(nil)
	assert.NoError(t, err)
	assert.Nil(t, wc3)

}

func TestValidate(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t)
	done()

	es.spec = &GenericEventStream{}
	_, err := es.esm.validateStream(ctx, es.spec, LifecyclePhasePreInsertValidation)
	assert.Regexp(t, "FF00112", err)

	es.spec.Name = ptrTo("name1")
	_, err = es.esm.validateStream(ctx, es.spec, LifecyclePhasePreInsertValidation)
	assert.NoError(t, err)

	es.esm.runtime.(*mockEventSource).validate = func(ctx context.Context, conf *GenericEventStream) error {
		return fmt.Errorf("pop")
	}
	_, err = es.esm.validateStream(ctx, es.spec, LifecyclePhasePreInsertValidation)
	assert.Regexp(t, "pop", err)
	es.esm.runtime.(*mockEventSource).validate = func(ctx context.Context, conf *GenericEventStream) error { return nil }

	es.spec.TopicFilter = ptrTo("((((!Bad Regexp[")
	_, err = es.esm.validateStream(ctx, es.spec, LifecyclePhasePreInsertValidation)
	assert.Regexp(t, "FF00235", err)

	es.spec.TopicFilter = nil
	es.spec.Type = ptrTo(fftypes.FFEnum("wrong"))
	_, err = es.esm.validateStream(ctx, es.spec, LifecyclePhasePreInsertValidation)
	assert.Regexp(t, "FF00234", err)

	es.spec.Type = ptrTo(EventStreamTypeWebSocket)
	es.spec.WebSocket = &WebSocketConfig{
		DistributionMode: ptrTo(fftypes.FFEnum("wrong")),
	}
	_, err = es.esm.validateStream(ctx, es.spec, LifecyclePhasePreInsertValidation)
	assert.Regexp(t, "FF00234", err)

	es.spec.Type = ptrTo(EventStreamTypeWebhook)
	_, err = es.esm.validateStream(ctx, es.spec, LifecyclePhasePreInsertValidation)
	assert.Regexp(t, "FF00216", err)

	_, err = es.esm.initEventStream(es.spec)
	assert.Regexp(t, "FF00216", err)

	customType := fftypes.FFEnumValue("estype", "custom1")
	es.spec.Type = &customType
	_, err = es.esm.validateStream(ctx, es.spec, LifecyclePhasePreInsertValidation)
	assert.Regexp(t, "FF00217", err)

	_, err = es.esm.initEventStream(es.spec)
	assert.Regexp(t, "FF00217", err)

}

func TestRequestStopAlreadyStopping(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t)
	defer done()

	es.activeState = &activeStream[*GenericEventStream, testData]{}
	es.stopping = make(chan struct{})
	s := es.requestStop(ctx)
	close(s)
	es.activeState = nil

}

func TestRequestStopPersistFail(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t, func(mdb *mockPersistence) {
		mdb.eventStreams.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("pop"))
	})
	defer done()

	es.spec.Status = ptrTo(EventStreamStatusDeleted)
	as := &activeStream[*GenericEventStream, testData]{
		eventLoopDone: make(chan struct{}),
		batchLoopDone: make(chan struct{}),
	}
	as.ctx, as.cancelCtx = context.WithCancel(ctx)
	as.cancelCtx()
	close(as.eventLoopDone)
	close(as.batchLoopDone)
	es.activeState = as
	s := es.requestStop(ctx)
	<-s

}

func TestCheckSetStatusMachine(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t)
	done()

	// FAIL: Deleting -> Started
	es.spec.Status = ptrTo(EventStreamStatusDeleted)
	es.stopping = make(chan struct{})
	newRuntimeStatus, changeToPersist, _, err := es.checkSetStatus(ctx, ptrTo(EventStreamStatusStarted))
	assert.Equal(t, EventStreamStatusStoppingDeleted, newRuntimeStatus)
	assert.Nil(t, changeToPersist)
	assert.Regexp(t, "FF00231", err)

	// FAIL: Stopping -> Started
	es.spec.Status = ptrTo(EventStreamStatusStopped)
	es.stopping = make(chan struct{})
	newRuntimeStatus, changeToPersist, _, err = es.checkSetStatus(ctx, ptrTo(EventStreamStatusStarted))
	assert.Equal(t, EventStreamStatusStopping, newRuntimeStatus)
	assert.Nil(t, changeToPersist)
	assert.Regexp(t, "FF00230", err)

	// NO-OP: Stopping -> Stopping
	es.spec.Status = ptrTo(EventStreamStatusStopped)
	es.stopping = make(chan struct{})
	newRuntimeStatus, changeToPersist, _, err = es.checkSetStatus(ctx, ptrTo(EventStreamStatusStopped))
	assert.Equal(t, EventStreamStatusStopping, newRuntimeStatus)
	assert.Nil(t, changeToPersist)
	assert.NoError(t, err)

	// FAIL: Bad persisted status
	es.spec.Status = ptrTo(fftypes.FFEnum("wrong"))
	_, _, _, err = es.checkSetStatus(ctx, ptrTo(EventStreamStatusStarted))
	assert.Regexp(t, "FF00233", err)

}

func TestStopFailBadStatus(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t)
	done()

	// FAIL: Deleting -> Stopped
	es.spec.Status = ptrTo(EventStreamStatusDeleted)
	err := es.stop(ctx)
	assert.Regexp(t, "FF00231", err)

}

func TestStopFailPersistFail(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t, func(mdb *mockPersistence) {
		mdb.eventStreams.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("pop"))
	})
	done()

	// OK: Started -> Stopping
	es.spec.Status = ptrTo(EventStreamStatusStarted)
	err := es.stop(ctx)
	assert.Regexp(t, "pop", err)

}

func TestStartFailBadStatus(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t)
	done()

	// FAIL: Deleting -> Started
	es.spec.Status = ptrTo(EventStreamStatusDeleted)
	err := es.start(ctx)
	assert.Regexp(t, "FF00231", err)

}

func TestStartFailPersistFail(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t, func(mdb *mockPersistence) {
		mdb.eventStreams.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("pop"))
	})
	done()

	// OK: Stopped -> Started
	es.spec.Status = ptrTo(EventStreamStatusStopped)
	err := es.start(ctx)
	assert.Regexp(t, "pop", err)

}

func TestSuspendTimeout(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t)
	done()

	es.activeState = &activeStream[*GenericEventStream, testData]{}
	es.stopping = make(chan struct{})
	err := es.suspend(ctx)
	assert.Regexp(t, "FF00229", err)

}

func TestGetIDNil(t *testing.T) {
	assert.Empty(t, (&GenericEventStream{}).GetID())
	assert.Empty(t, (&EventStreamCheckpoint{}).GetID())
}

func TestCheckDocs(t *testing.T) {
	ffapi.CheckObjectDocumented(&GenericEventStream{})
}
