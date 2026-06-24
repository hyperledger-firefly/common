// Copyright © 2023 Kaleido, Inc.
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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/retry"
	"github.com/hyperledger-firefly/common/pkg/wsclient"
	"github.com/hyperledger-firefly/common/pkg/wsserver"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testData struct {
	Field1 int `json:"field1"`
}

type testSource struct {
	started             chan struct{}
	startCount          int
	sequenceStartedWith string
}

func (ts *testSource) Validate(ctx context.Context, conf *GenericEventStream) error {
	if conf.Name != nil && *conf.Name == "validator_fail_instruction" {
		return fmt.Errorf("validator_failed")
	}
	return nil
}

func (ts *testSource) WithRuntimeStatus(spec *GenericEventStream, status EventStreamStatus, stats *EventStreamStatistics) *GenericEventStream {
	return spec.WithRuntimeStatus(status, stats)
}

func (ts *testSource) Run(ctx context.Context, spec *GenericEventStream, checkpointSequenceID string, deliver Deliver[testData]) error {
	msgNumber := 0
	ts.startCount++
	ts.sequenceStartedWith = checkpointSequenceID
	for {
		select {
		case <-ts.started:
		case <-ctx.Done():
			return fmt.Errorf("exiting due to cancelled context")
		}

		// Broadcast batches of 10 events...
		events := make([]*Event[testData], 10)
		for iEv := 0; iEv < 10; iEv++ {
			// With one message on each of 10 topics
			topic := fmt.Sprintf("topic_%d", iEv)
			events[iEv] = &Event[testData]{
				EventCommon: EventCommon{
					Topic:      topic,
					SequenceID: fmt.Sprintf("%.12d", msgNumber),
				},
				Data: &testData{
					Field1: msgNumber,
				},
			}
			msgNumber++
		}
		if deliver(events) == Exit {
			return nil
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (ts *testSource) NewID() string {
	return fftypes.NewUUID().String()
}

// This test demonstrates the runtime function of the event stream module through a simple test,
// using an SQLite database for CRUD operations on the persisted event streams,
// and a fake stream of events.
func TestE2E_DeliveryWebSockets(t *testing.T) {
	ctx, p, wss, wsc, done := setupE2ETest(t)

	ts := &testSource{started: make(chan struct{})}
	close(ts.started) // start delivery immediately - will block as no WS connected

	mgr, err := NewEventStreamManager[*GenericEventStream, testData](ctx, GenerateConfig[*GenericEventStream, testData](ctx), p, wss, ts)
	require.NoError(t, err)

	// Create a stream to sub-select one topic
	es1 := &GenericEventStream{
		Type: &EventStreamTypeWebSocket,
		EventStreamSpecFields: EventStreamSpecFields{
			TopicFilter: ptrTo("topic_1"), // only one of the topics
			BatchSize:   ptrTo(10),
		},
	}
	created, err := mgr.UpsertStream(ctx, "stream1", es1)
	require.NoError(t, err)
	assert.True(t, created)

	// Connect our websocket and start it
	err = wsc.Connect()
	require.NoError(t, err)
	err = wsc.Send(ctx, []byte(`{"type":"start","stream":"stream1"}`))
	require.NoError(t, err)

	expectedNumber := 1
	for i := 0; i < 10; i++ {
		wsReceiveAck(ctx, t, wsc, func(batch *EventBatch[testData]) {
			// each batch should be 10
			assert.Len(t, batch.Events, 10)
			for _, e := range batch.Events {
				assert.Equal(t, "topic_1", e.Topic)
				assert.Equal(t, expectedNumber, e.Data.Field1)
				// messages are published 0,1,2 over 10 topics, and we're only getting one of those topics
				expectedNumber += 10
			}
		})
	}

	// Check we ran the loop just once, and from the empty string for the checkpoint (as there was no InitialSequenceID)
	done()
	assert.Equal(t, "", ts.sequenceStartedWith)
	assert.Equal(t, 1, ts.startCount)
}

func TestE2E_DeliveryWebSocketsBroadcast(t *testing.T) {
	ctx, p, wss, wsc, done := setupE2ETest(t)

	ts := &testSource{started: make(chan struct{})}
	close(ts.started) // start delivery immediately - will block as no WS connected

	mgr, err := NewEventStreamManager[*GenericEventStream, testData](ctx, GenerateConfig[*GenericEventStream, testData](ctx), p, wss, ts)
	require.NoError(t, err)

	// Create a stream to sub-select one topic
	es1 := &GenericEventStream{
		Type: &EventStreamTypeWebSocket,
		EventStreamSpecFields: EventStreamSpecFields{
			TopicFilter: ptrTo("topic_1"), // only one of the topics
			BatchSize:   ptrTo(10),
		},
		WebSocket: &WebSocketConfig{
			DistributionMode: &DistributionModeBroadcast,
		},
	}
	created, err := mgr.UpsertStream(ctx, "stream1", es1)
	require.NoError(t, err)
	assert.True(t, created)

	// Connect our websocket and start it
	err = wsc.Connect()
	require.NoError(t, err)
	err = wsc.Send(ctx, []byte(`{"type":"start","stream":"stream1"}`))
	require.NoError(t, err)

	expectedNumber := 1
	for i := 0; i < 10; i++ {
		data := <-wsc.Receive()
		var batch EventBatch[testData]
		err := json.Unmarshal(data, &batch)
		require.NoError(t, err)
		// each batch should be 10
		assert.Len(t, batch.Events, 10)
		for _, e := range batch.Events {
			assert.Equal(t, "topic_1", e.Topic)
			assert.Equal(t, expectedNumber, e.Data.Field1)
			// messages are published 0,1,2 over 10 topics, and we're only getting one of those topics
			expectedNumber += 10
		}
	}

	// Check we ran the loop just once, and from the empty string for the checkpoint (as there was no InitialSequenceID)
	done()
	assert.Equal(t, "", ts.sequenceStartedWith)
	assert.Equal(t, 1, ts.startCount)
}

func TestE2E_DeliveryWebSocketsNack(t *testing.T) {
	ctx, p, wss, wsc, done := setupE2ETest(t, func() {
		RetrySection.Set(retry.ConfigMaximumDelay, "1ms" /* spin quickly */)
	})

	ts := &testSource{started: make(chan struct{})}
	close(ts.started) // start delivery immediately - will block as no WS connected

	mgr, err := NewEventStreamManager[*GenericEventStream, testData](ctx, GenerateConfig[*GenericEventStream, testData](ctx), p, wss, ts)
	require.NoError(t, err)

	// Create a stream to sub-select one topic
	es1 := &GenericEventStream{
		Type: &EventStreamTypeWebSocket,
		EventStreamSpecFields: EventStreamSpecFields{
			TopicFilter: ptrTo("topic_1"), // only one of the topics
			BatchSize:   ptrTo(10),
		},
	}
	created, err := mgr.UpsertStream(ctx, "stream1", es1)
	require.NoError(t, err)
	assert.True(t, created)

	// Connect our websocket and start it
	err = wsc.Connect()
	require.NoError(t, err)
	err = wsc.Send(ctx, []byte(`{"type":"start","stream":"stream1"}`))
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		wsReceiveNack(ctx, t, wsc, func(batch *EventBatch[testData]) {
			expectedNumber := 1 // should be same each time
			assert.Len(t, batch.Events, 10)
			for _, e := range batch.Events {
				assert.Equal(t, "topic_1", e.Topic)
				assert.Equal(t, expectedNumber, e.Data.Field1) // same message repeated
				expectedNumber += 10
			}
		})
	}

	// Check we ran the loop just once, and from the empty string for the checkpoint (as there was no InitialSequenceID)
	done()
	assert.Equal(t, "", ts.sequenceStartedWith)
	assert.Equal(t, 1, ts.startCount)
}

func TestE2E_WebsocketDeliveryRestartReset(t *testing.T) {
	ctx, p, wss, wsc, done := setupE2ETest(t)
	defer done()

	ts := &testSource{started: make(chan struct{})}
	close(ts.started) // start delivery immediately - will block as no WS connected

	mgr, err := NewEventStreamManager[*GenericEventStream, testData](ctx, GenerateConfig[*GenericEventStream, testData](ctx), p, wss, ts)
	require.NoError(t, err)

	// Create a stream to sub-select one topic
	es1 := &GenericEventStream{
		Type: &EventStreamTypeWebSocket,
		EventStreamSpecFields: EventStreamSpecFields{
			TopicFilter: ptrTo("topic_1"), // only one of the topics
			BatchSize:   ptrTo(10),
		},
	}
	created, err := mgr.UpsertStream(ctx, "stream1", es1)
	require.NoError(t, err)
	assert.True(t, created)

	// Connect our websocket and start it
	err = wsc.Connect()
	require.NoError(t, err)
	err = wsc.Send(ctx, []byte(`{"type":"start","stream":"stream1"}`))
	require.NoError(t, err)

	// Get the first batch
	wsReceiveAck(ctx, t, wsc, func(batch *EventBatch[testData]) {})
	assert.Equal(t, "", ts.sequenceStartedWith)
	assert.Equal(t, 1, ts.startCount)

	// Wait for the checkpoint to be reflected in the status
	var ess *GenericEventStream
	for ess == nil || ess.Statistics == nil || ess.Statistics.Checkpoint == "" {
		time.Sleep(1 * time.Millisecond)
		ess, err = mgr.GetStreamByID(ctx, es1.GetID())
		require.NoError(t, err)
	}

	// Restart and check we get called with the checkpoint - note we don't reconnect the
	// websocket or restart that - it remains "started" from the websocket protocol
	// perspective throughout
	err = mgr.StopStream(ctx, es1.GetID())
	require.NoError(t, err)
	err = mgr.StartStream(ctx, es1.GetID())
	require.NoError(t, err)
	wsReceiveAck(ctx, t, wsc, func(batch *EventBatch[testData]) {})
	assert.Equal(t, "000000000091", ts.sequenceStartedWith)
	assert.Equal(t, 2, ts.startCount)

	// Reset it and check we get the reset
	err = mgr.ResetStream(ctx, es1.GetID(), ptrTo("first"))
	require.NoError(t, err)
	wsReceiveAck(ctx, t, wsc, func(batch *EventBatch[testData]) {})
	assert.Equal(t, "first", ts.sequenceStartedWith)
	assert.Equal(t, 3, ts.startCount)

}

func TestE2E_ResetStreamWhileAwaitingAck(t *testing.T) {
	ctx, p, wss, wsc, done := setupE2ETest(t)

	ts := &testSource{started: make(chan struct{})}
	close(ts.started) // start delivery immediately - will block as no WS connected

	mgr, err := NewEventStreamManager[*GenericEventStream, testData](ctx, GenerateConfig[*GenericEventStream, testData](ctx), p, wss, ts)
	require.NoError(t, err)

	// Create a stream to sub-select one topic
	es1 := &GenericEventStream{
		Type: &EventStreamTypeWebSocket,
		EventStreamSpecFields: EventStreamSpecFields{
			TopicFilter: ptrTo("topic_1"), // only one of the topics
			BatchSize:   ptrTo(10),
		},
	}
	created, err := mgr.UpsertStream(ctx, "stream1", es1)
	require.NoError(t, err)
	assert.True(t, created)

	// Connect our websocket and start it
	err = wsc.Connect()
	require.NoError(t, err)
	err = wsc.Send(ctx, []byte(`{"type":"start","stream":"stream1"}`))
	require.NoError(t, err)

	// Receive the message batch
	data := <-wsc.Receive()
	var batch EventBatch[testData]
	err = json.Unmarshal(data, &batch)
	require.NoError(t, err)

	// Do a reset before we ack.
	err = mgr.ResetStream(ctx, "stream1", ptrTo("12345"))
	require.NoError(t, err)

	// Should get the batch again
	data = <-wsc.Receive()
	err = json.Unmarshal(data, &batch)
	require.NoError(t, err)

	// Check we did the reset
	done()
	assert.Equal(t, "12345", ts.sequenceStartedWith)
	assert.Equal(t, 2, ts.startCount)
}

func TestE2E_DeliveryWebHooks200(t *testing.T) {
	ctx, p, wss, wsc, done := setupE2ETest(t)
	defer done()

	ts := &testSource{started: make(chan struct{})}
	close(ts.started) // start delivery immediately - will block as no WS connected

	mgr, err := NewEventStreamManager[*GenericEventStream, testData](ctx, GenerateConfig[*GenericEventStream, testData](ctx), p, wss, ts)
	require.NoError(t, err)

	got100 := make(chan struct{})
	var received int64
	expectedNumber := 1
	whServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		assert.Equal(t, http.MethodPut, req.Method)
		assert.Equal(t, "/some/path", req.URL.Path)
		assert.Equal(t, "my-value", req.Header.Get("My-Header"))

		var batch EventBatch[testData]
		err := json.NewDecoder(req.Body).Decode(&batch)
		require.NoError(t, err)

		// each batch should be 10
		assert.Len(t, batch.Events, 10)
		for _, e := range batch.Events {
			assert.Equal(t, "topic_1", e.Topic)
			// messages are published 0,1,2 over 10 topics, and we're only getting one of those topics
			assert.Equal(t, expectedNumber, e.Data.Field1)
			expectedNumber += 10
		}
		if (atomic.AddInt64(&received, 1)) == 100 {
			close(got100)
		}

		res.WriteHeader(200)
	}))
	defer whServer.Close()

	// Create a stream to sub-select one topic
	es1 := &GenericEventStream{
		Type: &EventStreamTypeWebhook,
		EventStreamSpecFields: EventStreamSpecFields{
			TopicFilter: ptrTo("topic_1"), // only one of the topics
			BatchSize:   ptrTo(10),
		},
		Webhook: &WebhookConfig{
			URL:    ptrTo(whServer.URL + "/some/path"),
			Method: ptrTo("PUT"),
			HTTP: &ffresty.HTTPConfig{
				HTTPHeaders: map[string]interface{}{
					"my-header": "my-value",
				},
			},
		},
	}
	created, err := mgr.UpsertStream(ctx, "stream1", es1)
	require.NoError(t, err)
	assert.True(t, created)

	// Connect our websocket and start it
	err = wsc.Connect()
	require.NoError(t, err)
	err = wsc.Send(ctx, []byte(`{"type":"start","stream":"stream1"}`))
	require.NoError(t, err)

	// Check we ran the loop just once, and from the empty string for the checkpoint (as there was no InitialSequenceID)
	<-got100
	assert.Equal(t, "", ts.sequenceStartedWith)
	assert.Equal(t, 1, ts.startCount)
}

func TestE2E_DeliveryWebHooks500Retry(t *testing.T) {
	ctx, p, wss, wsc, done := setupE2ETest(t, func() {
		RetrySection.Set(retry.ConfigMaximumDelay, "1ms" /* spin quickly */)
	})
	defer done()

	ts := &testSource{started: make(chan struct{})}
	close(ts.started) // start delivery immediately - will block as no WS connected

	mgr, err := NewEventStreamManager[*GenericEventStream, testData](ctx, GenerateConfig[*GenericEventStream, testData](ctx), p, wss, ts)
	require.NoError(t, err)

	gotFiveTimes := make(chan struct{})
	var received int64
	whServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		assert.Equal(t, http.MethodPut, req.Method)
		assert.Equal(t, "/some/path", req.URL.Path)
		assert.Equal(t, "my-value", req.Header.Get("My-Header"))

		var batch EventBatch[testData]
		err := json.NewDecoder(req.Body).Decode(&batch)
		require.NoError(t, err)

		// We should get the same batch redelivered
		expectedNumber := 1
		assert.Len(t, batch.Events, 10)
		for _, e := range batch.Events {
			assert.Equal(t, "topic_1", e.Topic)
			assert.Equal(t, expectedNumber, e.Data.Field1)
			expectedNumber += 10
		}
		if (atomic.AddInt64(&received, 1)) == 5 {
			close(gotFiveTimes)
		}

		res.WriteHeader(500)
	}))
	defer whServer.Close()

	// Create a stream to sub-select one topic
	es1 := &GenericEventStream{
		Type: &EventStreamTypeWebhook,
		EventStreamSpecFields: EventStreamSpecFields{
			TopicFilter: ptrTo("topic_1"), // only one of the topics
			BatchSize:   ptrTo(10),
		},
		Webhook: &WebhookConfig{
			URL:    ptrTo(whServer.URL + "/some/path"),
			Method: ptrTo("PUT"),
			HTTP: &ffresty.HTTPConfig{
				HTTPHeaders: map[string]interface{}{
					"my-header": "my-value",
				},
			},
		},
	}
	created, err := mgr.UpsertStream(ctx, "stream1", es1)
	require.NoError(t, err)
	assert.True(t, created)

	// Connect our websocket and start it
	err = wsc.Connect()
	require.NoError(t, err)
	err = wsc.Send(ctx, []byte(`{"type":"start","stream":"stream1"}`))
	require.NoError(t, err)

	// Check we ran the loop just once, and from the empty string for the checkpoint (as there was no InitialSequenceID)
	<-gotFiveTimes
	assert.Equal(t, "", ts.sequenceStartedWith)
	assert.Equal(t, 1, ts.startCount)
}

// This test demonstrates the CRUD features
func TestE2E_CRUDLifecycle(t *testing.T) {
	ctx, p, wss, _, done := setupE2ETest(t)
	defer done()

	ts := &testSource{
		started: make(chan struct{}), // we never start it
	}

	mgr, err := NewEventStreamManager[*GenericEventStream, testData](ctx, GenerateConfig[*GenericEventStream, testData](ctx), p, wss, ts)
	require.NoError(t, err)

	// Create first event stream started
	es1 := &GenericEventStream{
		Type: &EventStreamTypeWebSocket,
		EventStreamSpecFields: EventStreamSpecFields{
			TopicFilter: ptrTo("topic1"), // only one of the topics
		},
	}
	created, err := mgr.UpsertStream(ctx, "stream1", es1)
	require.NoError(t, err)
	assert.True(t, created)

	// Create second event stream stopped
	es2 := &GenericEventStream{
		Type: &EventStreamTypeWebSocket,
		EventStreamSpecFields: EventStreamSpecFields{
			TopicFilter: ptrTo("topic2"), // only one of the topics
			Status:      &EventStreamStatusStopped,
		},
	}
	created, err = mgr.UpsertStream(ctx, "stream2", es2)
	require.NoError(t, err)
	assert.True(t, created)

	// Find the second one by topic filter
	esList, _, err := mgr.ListStreams(ctx, GenericEventStreamFilters.NewFilter(ctx).Eq("topicfilter", "topic2"))
	require.NoError(t, err)
	assert.Len(t, esList, 1)
	assert.Equal(t, "stream2", *esList[0].Name)
	assert.Equal(t, "topic2", *esList[0].TopicFilter)
	assert.Equal(t, 50, *esList[0].BatchSize) // picked up default when it was loaded
	assert.Equal(t, EventStreamStatusStopped, *esList[0].Status)

	// Get the first by ID
	es1c, err := mgr.GetStreamByID(ctx, es1.GetID(), dbsql.FailIfNotFound)
	require.NoError(t, err)
	assert.Equal(t, "stream1", *es1c.Name)
	assert.Equal(t, EventStreamStatusStarted, *es1c.Status)

	// Rename second event stream
	es2.Name = ptrTo("stream2a")
	created, err = mgr.UpsertStream(ctx, "" /* ID is in es2 object */, es2)
	require.NoError(t, err)
	assert.False(t, created)

	// Start and re-stop, then delete the second event stream
	err = mgr.StartStream(ctx, es2.GetID())
	require.NoError(t, err)
	es2c, err := mgr.GetStreamByID(ctx, es2.GetID(), dbsql.FailIfNotFound)
	require.NoError(t, err)
	assert.Equal(t, EventStreamStatusStarted, *es2c.Status)
	assert.Equal(t, "stream2a", *es2c.Name)
	err = mgr.StopStream(ctx, es2.GetID())
	require.NoError(t, err)
	es2c, err = mgr.GetStreamByID(ctx, es2.GetID(), dbsql.FailIfNotFound)
	require.NoError(t, err)
	assert.Equal(t, EventStreamStatusStopped, *es2c.Status)
	err = mgr.DeleteStream(ctx, es2.GetID())
	require.NoError(t, err)

	// Delete the first stream (which is running still)
	err = mgr.DeleteStream(ctx, *es1.Name)
	require.NoError(t, err)

	// Check no streams left
	esList, _, err = mgr.ListStreams(ctx, GenericEventStreamFilters.NewFilter(ctx).And())
	require.NoError(t, err)
	assert.Empty(t, esList)

}

func wsReceiveAck(ctx context.Context, t *testing.T, wsc wsclient.WSClient, cb func(batch *EventBatch[testData])) {
	data := <-wsc.Receive()
	var batch EventBatch[testData]
	err := json.Unmarshal(data, &batch)
	require.NoError(t, err)
	cb(&batch)
	err = wsc.Send(ctx, []byte(fmt.Sprintf(`{"type":"ack","stream":"stream1","batchNumber":%d}`, batch.BatchNumber)))
	require.NoError(t, err)
}

func wsReceiveNack(ctx context.Context, t *testing.T, wsc wsclient.WSClient, cb func(batch *EventBatch[testData])) {
	data := <-wsc.Receive()
	var batch EventBatch[testData]
	err := json.Unmarshal(data, &batch)
	require.NoError(t, err)
	cb(&batch)
	err = wsc.Send(ctx, []byte(fmt.Sprintf(`{"type":"nack","stream":"stream1","batchNumber":%d}`, batch.BatchNumber)))
	require.NoError(t, err)
}

func setupE2ETest(t *testing.T, extraSetup ...func()) (context.Context, Persistence[*GenericEventStream], wsserver.Protocol, wsclient.WSClient, func()) {
	logrus.SetLevel(logrus.TraceLevel)

	ctx := context.Background()
	config.RootConfigReset()
	conf := config.RootSection("ut")
	dbConf := conf.SubSection("db")
	esConf := conf.SubSection("eventstreams")
	wsServerConf := conf.SubSection("wss")
	wsserver.InitConfig(wsServerConf)
	dbsql.InitSQLiteConfig(dbConf)
	InitConfig(esConf)

	dbConf.Set(dbsql.SQLConfMigrationsAuto, true)
	dbConf.Set(dbsql.SQLConfDatasourceURL, "file::memory:")
	dbConf.Set(dbsql.SQLConfMigrationsAuto, true)
	dbConf.Set(dbsql.SQLConfMigrationsDirectory, "../../test/es_demo_migrations")
	dbConf.Set(dbsql.SQLConfMaxConnections, 1)

	CheckpointsConfig.Set(ConfigCheckpointsAsynchronous, false)

	for _, fn := range extraSetup {
		fn()
	}

	db, err := dbsql.NewSQLiteProvider(ctx, dbConf)
	require.NoError(t, err)

	p := NewGenericEventStreamPersistence(db, dbsql.UUIDValidator)
	p.EventStreams().Validate()
	p.Checkpoints().Validate()

	wss := wsserver.NewWebSocketServer(ctx, wsserver.GenerateConfig(wsServerConf))
	server := httptest.NewServer(http.HandlerFunc(wss.Handler))

	// Build the WS connection, but don't connect it yet
	wsc, err := wsclient.New(ctx, &wsclient.WSConfig{
		HTTPURL: server.URL,
	}, nil, nil)

	return ctx, p, wss, wsc, func() {
		p.Close()
		server.Close()
		wsc.Close()
	}

}
