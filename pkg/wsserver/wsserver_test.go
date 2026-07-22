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

package wsserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	ws "github.com/gorilla/websocket"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
)

type testPayload struct {
	BatchHeader
	Message string
}

func (tp *testPayload) GetBatchHeader() *BatchHeader {
	return &tp.BatchHeader
}

func newTestWebSocketServer() (*webSocketServer, *httptest.Server) {
	config.RootConfigReset()
	utConf := config.RootSection("ut")
	InitConfig(utConf)
	s := NewWebSocketServer(context.Background(), GenerateConfig(utConf)).(*webSocketServer)
	ts := httptest.NewServer(http.HandlerFunc(s.Handler))
	return s, ts
}

func TestRoundTripStartFirstGood(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	assert := assert.New(t)

	w, ts := newTestWebSocketServer()
	defer ts.Close()
	defer w.Close()

	u, err := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"
	c, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)

	c.WriteJSON(&WebSocketCommandMessage{
		Type: "start",
		// note this test runs on the "" (empty string) stream
	})

	// Double start will be ignored
	c.WriteJSON(&WebSocketCommandMessage{
		Type: "start",
	})

	// Wait in this test until we've locked in the start
	err = w.waitStreamConnections(w.ctx, "", func(ss *streamState) error { return nil })
	assert.NoError(err)

	for i := 1; i <= 10; i++ {

		roundTripComplete := make(chan struct{})
		go func() {
			cm, err := w.RoundTrip(w.ctx, "", &testPayload{Message: "Hello World"})
			assert.NoError(err)
			assert.Equal("good ack", cm.Message)
			defer close(roundTripComplete)
		}()

		var received testPayload
		err = c.ReadJSON(&received)
		assert.NoError(err)
		assert.Equal(int64(i), received.BatchNumber)
		assert.Equal("Hello World", received.Message)

		c.WriteJSON(&WebSocketCommandMessage{
			Type: "ignoreme",
		})

		c.WriteJSON(&WebSocketCommandMessage{
			Type:        "ack",
			BatchNumber: 12345,
			Message:     "bad ack",
		})

		c.WriteJSON(&WebSocketCommandMessage{
			Type:        "ack",
			BatchNumber: received.BatchNumber,
			Message:     "good ack",
		})
		<-roundTripComplete

	}

}

func TestRoundTripStartWhileRoundTripWaitingGood(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	assert := assert.New(t)

	w, ts := newTestWebSocketServer()
	defer ts.Close()
	defer w.Close()

	u, err := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"
	c, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)

	roundTripComplete := make(chan struct{})
	go func() {
		cm, err := w.RoundTrip(w.ctx, "stream1", &testPayload{Message: "Hello World"})
		assert.NoError(err)
		assert.Equal("good ack", cm.Message)
		defer close(roundTripComplete)
	}()

	time.Sleep(50 * time.Millisecond)

	c.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: "stream1",
	})

	var received testPayload
	err = c.ReadJSON(&received)
	assert.NoError(err)
	assert.Equal(int64(1), received.BatchNumber)
	assert.Equal("Hello World", received.Message)

	c.WriteJSON(&WebSocketCommandMessage{
		Type:        "ack",
		BatchNumber: received.BatchNumber,
		Message:     "good ack",
		Stream:      "stream1",
	})
	<-roundTripComplete
}

func TestRoundTripInFlight(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	assert := assert.New(t)

	w, ts := newTestWebSocketServer()
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"
	c, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)

	c.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: "stream1",
	})

	roundTripComplete := make(chan struct{})
	go func() {
		defer close(roundTripComplete)
		_, err := w.RoundTrip(w.ctx, "stream1", &testPayload{Message: "Hello World"})
		assert.Regexp("FF00228", err)
	}()

	var received testPayload
	err = c.ReadJSON(&received)
	assert.NoError(err)

	_, err = w.RoundTrip(w.ctx, "stream1", &testPayload{Message: "Hello World"})
	assert.Regexp("FF00243", err)

	w.Close()
	<-roundTripComplete
}

func TestRoundTripClientNack(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	assert := assert.New(t)

	w, ts := newTestWebSocketServer()
	defer ts.Close()
	defer w.Close()

	u, err := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"
	c, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)

	roundTripComplete := make(chan struct{})
	go func() {
		cm, err := w.RoundTrip(w.ctx, "stream1", &testPayload{Message: "Don't Panic!"})
		assert.Regexp("Error received from WebSocket client: Panic!", err)
		assert.Equal("Panic!", cm.Message)
		defer close(roundTripComplete)
	}()

	c.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: "stream1",
	})

	var received testPayload
	err = c.ReadJSON(&received)
	assert.NoError(err)
	assert.Equal(int64(1), received.BatchNumber)
	assert.Equal("Don't Panic!", received.Message)

	c.WriteJSON(&WebSocketCommandMessage{
		Type:    "error",
		Message: "Panic!",
		Stream:  "stream1",
		// Note we drive fallback "last batch" processing here, as we omit a batch number
	})
	<-roundTripComplete
}

func TestConnectStreamIsolation(t *testing.T) {
	assert := assert.New(t)

	w, ts := newTestWebSocketServer()
	defer ts.Close()
	defer w.Close()

	u, err := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"
	c1, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)
	c2, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)

	c1.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: "stream1",
	})

	c2.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: "stream2",
	})

	roundTripComplete1 := make(chan struct{})
	go func() {
		cm, err := w.RoundTrip(w.ctx, "stream1", &testPayload{Message: "Hello Number 1"})
		assert.NoError(err)
		assert.Equal("stream1 message", cm.Message)
		defer close(roundTripComplete1)
	}()
	roundTripComplete2 := make(chan struct{})
	go func() {
		cm, err := w.RoundTrip(w.ctx, "stream2", &testPayload{Message: "Hello Number 2"})
		assert.NoError(err)
		assert.Equal("stream2 message", cm.Message)
		defer close(roundTripComplete2)
	}()

	var received testPayload
	err = c1.ReadJSON(&received)
	assert.NoError(err)
	assert.Equal(int64(1), received.BatchNumber)
	assert.Equal("Hello Number 1", received.Message)
	c1.WriteJSON(&WebSocketCommandMessage{
		Type:    "ack",
		Stream:  "stream1",
		Message: "stream1 message",
	})

	err = c2.ReadJSON(&received)
	assert.NoError(err)
	assert.Equal(int64(1), received.BatchNumber)
	assert.Equal("Hello Number 2", received.Message)
	c2.WriteJSON(&WebSocketCommandMessage{
		Type:    "ack",
		Stream:  "stream2",
		Message: "stream2 message",
	})

	<-roundTripComplete1
	<-roundTripComplete2

}

func TestConnectAbandonRequest(t *testing.T) {
	assert := assert.New(t)

	w, ts := newTestWebSocketServer()
	defer ts.Close()
	defer w.Close()

	u, err := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"
	c, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)

	c.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: "stream1",
	})

	// Wait in this test until we've locked in the start
	err = w.waitStreamConnections(w.ctx, "stream1", func(ss *streamState) error { return nil })
	assert.NoError(err)

	roundTripComplete := make(chan struct{})
	go func() {
		_, err := w.RoundTrip(w.ctx, "stream1", &testPayload{Message: "Hello World"})
		assert.Regexp("FF00228", err)
		defer close(roundTripComplete)
	}()

	// Close the client while we've got an active read stream
	c.Close()

	// We should find the read stream closes out
	<-roundTripComplete
}

func TestConnectBadWebsocketHandshake(t *testing.T) {
	assert := assert.New(t)

	w, ts := newTestWebSocketServer()
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	u.Path = "/ws"

	res, err := http.Get(u.String())
	assert.NoError(err)
	assert.Equal(400, res.StatusCode)

	w.Close()

}

func TestBroadcastStartWithoutConnections(t *testing.T) {
	assert := assert.New(t)

	w, ts := newTestWebSocketServer()
	defer ts.Close()
	defer w.Close()

	u, _ := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"
	stream := "banana"

	go w.Broadcast(w.ctx, stream, "Hello World")

	c1, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)
	c1.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: stream,
	})

	c2, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)
	c2.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: stream,
	})
}

func TestBroadcast(t *testing.T) {
	assert := assert.New(t)

	w, ts := newTestWebSocketServer()
	defer ts.Close()
	defer w.Close()

	u, _ := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"
	stream := "banana"

	c1, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)
	c1.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: stream,
	})

	c2, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)
	c2.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: stream,
	})

	// Wait until the clients have subscribed to the stream before proceeding
	count := 0
	for count < 2 {
		time.Sleep(10 * time.Millisecond)
		err = w.waitStreamConnections(w.ctx, stream, func(ss *streamState) error {
			count = len(ss.conns)
			return nil
		})
		assert.NoError(err)
	}

	var val string

	w.Broadcast(w.ctx, stream, "Hello World")
	c1.ReadJSON(&val)
	assert.Equal("Hello World", val)
	c2.ReadJSON(&val)
	assert.Equal("Hello World", val)

	w.Broadcast(w.ctx, stream, "Hello World Again")
	c1.ReadJSON(&val)
	assert.Equal("Hello World Again", val)
	c2.ReadJSON(&val)
	assert.Equal("Hello World Again", val)

}

func TestActionsAfterClose(t *testing.T) {
	assert := assert.New(t)

	w, ts := newTestWebSocketServer()
	defer ts.Close()
	defer w.Close()

	u, _ := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"
	c, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)

	c.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: "stream1",
	})

	closedCtx, closeCtx := context.WithCancel(context.Background())
	closeCtx()
	err = w.waitStreamConnections(closedCtx, "stream1", func(ss *streamState) error { return nil })
	assert.Regexp("FF00154", err)

	var conn *webSocketConnection
	err = w.waitStreamConnections(w.ctx, "stream1", func(ss *streamState) error {
		conn = ss.conns[0]
		return nil
	})
	assert.NoError(err)

	c.Close()
	<-conn.closing

	// Send after close
	conn.send = make(chan interface{})
	conn.closing = make(chan struct{})
	go func() { conn.send <- "anything" }()
	conn.sender()

	// Broadcast after close
	close(conn.closing)
	w.streamMap = map[string]*streamState{
		"stream1": {
			conns: []*webSocketConnection{
				conn,
			},
		},
	}
	w.Broadcast(w.ctx, "stream1", "test1")

	// RoundTrip after close
	_, err = w.RoundTrip(w.ctx, "stream1", &testPayload{})
	assert.Regexp("FF00228", err)

	// round trip not in fight
	w.completeRoundTrip("stream1", &WebSocketCommandMessage{}, nil)
}

func TestRoundTripClientAckTimeout(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	assert := assert.New(t)

	w, ts := newTestWebSocketServer()
	defer ts.Close()
	defer w.Close()
	w.conf.AckTimeout = 1 * time.Microsecond

	u, err := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"
	c, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)

	roundTripComplete := make(chan struct{})
	go func() {
		_, err := w.RoundTrip(w.ctx, "stream1", &testPayload{Message: "anybody there"})
		assert.Regexp("FF00244", err)
		defer close(roundTripComplete)
	}()

	c.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: "stream1",
	})

	var received testPayload
	err = c.ReadJSON(&received)
	assert.NoError(err)
	assert.Equal(int64(1), received.BatchNumber)
	assert.Equal("anybody there", received.Message)

	<-roundTripComplete
}

func TestRoundTripContextCancelled(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)

	assert := assert.New(t)

	w, ts := newTestWebSocketServer()
	defer ts.Close()
	defer w.Close()
	w.conf.AckTimeout = 1 * time.Microsecond

	u, err := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"
	c, _, err := ws.DefaultDialer.Dial(u.String(), nil)
	assert.NoError(err)
	defer c.Close()

	c.WriteJSON(&WebSocketCommandMessage{
		Type:   "start",
		Stream: "stream1",
	})

	err = w.waitStreamConnections(w.ctx, "stream1", func(ss *streamState) error { return nil })
	assert.NoError(err)

	// close the context before round trip
	canceledCtx, cancelCtx := context.WithCancel(context.Background())
	cancelCtx()

	// Block the send
	w.streamMap["stream1"].conns[0].send = make(chan interface{})

	_, err = w.RoundTrip(canceledCtx, "stream1", &testPayload{Message: "anybody there"})
	assert.Regexp("FF00154", err)
}
