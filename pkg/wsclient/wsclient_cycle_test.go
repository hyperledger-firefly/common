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

package wsclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func cycleTestConfig(url string) *WSConfig {
	return &WSConfig{
		HTTPURL:                    url,
		InitialDelay:               5 * time.Millisecond,
		MaximumDelay:               20 * time.Millisecond,
		ConnectionCycleInterval:    75 * time.Millisecond,
		ConnectionCycleQuiesceTime: 150 * time.Millisecond,
	}
}

func nextConn(t *testing.T, connections chan *TestWSConnection) *TestWSConnection {
	t.Helper()
	select {
	case c := <-connections:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a connection to the test server")
		return nil
	}
}

func expectMsg(t *testing.T, c *TestWSConnection, expected string) {
	t.Helper()
	select {
	case msg := <-c.ToServer:
		assert.Equal(t, expected, msg)
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for message %q", expected)
	}
}

func waitDone(t *testing.T, c *TestWSConnection) {
	t.Helper()
	select {
	case <-c.Done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for connection to close")
	}
}

// waitForSwitch polls (white box) until the current connection is no longer the supplied
// one - so the test knows promotion has completed and the old connection is quiescing
func waitForSwitch(t *testing.T, wsc WSClient, from *wsConnection) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c := wsc.(*wsClient).currentConn()
		if c != nil && c != from {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for connection switch")
}

func TestWSConnectionCycleE2E(t *testing.T) {
	connections, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	wsConfig := cycleTestConfig(url)
	wsConfig.HeartbeatInterval = 25 * time.Millisecond
	wsConfig.PreDisconnectHandler = func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`unsubscribe`))
	}
	afterConnect := func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`subscribe`))
	}

	wsc, err := New(context.Background(), wsConfig, nil, afterConnect)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)
	expectMsg(t, conn1, `subscribe`)

	// The first cycle establishes a second connection - the pre-disconnect handler sends
	// on the OLD connection, and the post-connect handler sends on the NEW connection
	conn2 := nextConn(t, connections)
	expectMsg(t, conn1, `unsubscribe`)
	expectMsg(t, conn2, `subscribe`)

	// During the quiesce period the old connection still delivers inbound messages
	conn1.FromServer <- `old inbound`
	assert.Equal(t, `old inbound`, string(<-wsc.Receive()))

	// The old connection closes at the end of the quiesce period
	waitDone(t, conn1)

	// After the cycle, sends go to the new connection, and inbound flows on it
	err = wsc.Send(context.Background(), []byte(`to new conn`))
	assert.NoError(t, err)
	expectMsg(t, conn2, `to new conn`)
	conn2.FromServer <- `new inbound`
	assert.Equal(t, `new inbound`, string(<-wsc.Receive()))

	// A second cycle proves the timer restarts after the quiesce completes
	conn3 := nextConn(t, connections)
	select {
	case <-conn1.Done:
		// at most two connections ever - the first is long gone before the third arrives
	default:
		t.Fatal("conn1 still live when conn3 established")
	}
	expectMsg(t, conn2, `unsubscribe`)
	expectMsg(t, conn3, `subscribe`)
	waitDone(t, conn2)

	// Close also runs the pre-disconnect handler, on the final connection
	wsc.Close()
	expectMsg(t, conn3, `unsubscribe`)
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
	waitDone(t, conn3)
}

func TestWSConnectionCycleReceiveExt(t *testing.T) {
	connections, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	wsConfig := cycleTestConfig(url)
	wsConfig.HeartbeatInterval = 25 * time.Millisecond
	wsConfig.ReceiveExt = true
	afterConnect := func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`subscribe`))
	}

	wsc, err := New(context.Background(), wsConfig, nil, afterConnect)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)
	expectMsg(t, conn1, `subscribe`)

	// No pre-disconnect handler configured - the cycle works fine without one
	conn2 := nextConn(t, connections)
	expectMsg(t, conn2, `subscribe`)

	// The old connection still delivers via the ext receive channel during quiesce
	conn1.FromServer <- `old inbound`
	payload := <-wsc.ReceiveExt()
	b, err := io.ReadAll(payload.Reader)
	assert.NoError(t, err)
	assert.Equal(t, `old inbound`, string(b))
	payload.Processed()

	waitDone(t, conn1)

	conn2.FromServer <- `new inbound`
	payload = <-wsc.ReceiveExt()
	b, err = io.ReadAll(payload.Reader)
	assert.NoError(t, err)
	assert.Equal(t, `new inbound`, string(b))
	payload.Processed()

	wsc.Close()
	_, ok := <-wsc.ReceiveExt()
	assert.False(t, ok)
}

func TestWSCycleDialFailureKeepsOldConnection(t *testing.T) {
	connections, url, rejectNext, done := NewTestWSServerMulti(nil)
	defer done()

	wsConfig := cycleTestConfig(url)
	wsConfig.ConnectionCycleInterval = 50 * time.Millisecond

	wsc, err := New(context.Background(), wsConfig, nil, nil)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)

	// Make the cycle's dial attempts fail - the old connection must remain fully active
	rejectNext(true)
	time.Sleep(150 * time.Millisecond) // well past the cycle boundary

	err = wsc.Send(context.Background(), []byte(`still on old`))
	assert.NoError(t, err)
	expectMsg(t, conn1, `still on old`)
	conn1.FromServer <- `old still receiving`
	assert.Equal(t, `old still receiving`, string(<-wsc.Receive()))

	// Allow the dial to succeed - the cycle completes
	rejectNext(false)
	conn2 := nextConn(t, connections)
	waitDone(t, conn1)

	err = wsc.Send(context.Background(), []byte(`on new`))
	assert.NoError(t, err)
	expectMsg(t, conn2, `on new`)

	wsc.Close()
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSCycleAfterConnectFailureRetries(t *testing.T) {
	connections, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	var connCount, preDisconnectCount atomic.Int32
	wsConfig := cycleTestConfig(url)
	wsConfig.PreDisconnectHandler = func(ctx context.Context, w WSClient) error {
		preDisconnectCount.Add(1)
		return nil
	}
	afterConnect := func(ctx context.Context, w WSClient) error {
		if connCount.Add(1) == 2 {
			return fmt.Errorf("pop") // fail the first attempt of the first cycle
		}
		return w.Send(ctx, []byte(`subscribe`))
	}

	wsc, err := New(context.Background(), wsConfig, nil, afterConnect)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)
	expectMsg(t, conn1, `subscribe`)

	// afterConnect fails on this connection, so it is torn down and the dial retried
	conn2 := nextConn(t, connections)
	waitDone(t, conn2)

	// The retry succeeds, and the cycle completes
	conn3 := nextConn(t, connections)
	expectMsg(t, conn3, `subscribe`)
	waitDone(t, conn1)

	// The pre-disconnect handler ran exactly once, despite the retry
	assert.Equal(t, int32(1), preDisconnectCount.Load())

	wsc.Close()
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSCyclePreDisconnectErrorNonFatal(t *testing.T) {
	connections, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	wsConfig := cycleTestConfig(url)
	wsConfig.PreDisconnectHandler = func(ctx context.Context, w WSClient) error {
		return fmt.Errorf("pop")
	}
	afterConnect := func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`subscribe`))
	}

	wsc, err := New(context.Background(), wsConfig, nil, afterConnect)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)
	expectMsg(t, conn1, `subscribe`)

	// The cycle completes regardless of the pre-disconnect handler failing
	conn2 := nextConn(t, connections)
	expectMsg(t, conn2, `subscribe`)
	waitDone(t, conn1)

	err = wsc.Send(context.Background(), []byte(`on new`))
	assert.NoError(t, err)
	expectMsg(t, conn2, `on new`)

	wsc.Close()
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSCycleCloseMidQuiesce(t *testing.T) {
	connections, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	wsConfig := cycleTestConfig(url)
	wsConfig.ConnectionCycleInterval = 25 * time.Millisecond
	wsConfig.ConnectionCycleQuiesceTime = 1 * time.Minute // Close must not wait for this

	wsc, err := New(context.Background(), wsConfig, nil, nil)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)
	c1 := wsc.(*wsClient).currentConn()

	_ = nextConn(t, connections)
	waitForSwitch(t, wsc, c1) // the old connection is now quiescing

	wsc.Close()

	// Both connections close promptly, and the receive channel closes
	waitDone(t, conn1)
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSCycleCloseBeforeSwitch(t *testing.T) {
	connections, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	var wsc WSClient
	var connCount atomic.Int32
	afterConnect := func(ctx context.Context, w WSClient) error {
		if connCount.Add(1) == 2 {
			wsc.Close() // close while the cycle is mid-flight, before the switch
		}
		return nil
	}

	wsConfig := cycleTestConfig(url)
	wsConfig.ConnectionCycleInterval = 25 * time.Millisecond

	wsc, err := New(context.Background(), wsConfig, nil, afterConnect)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	// Both the original connection, and the orphaned new one, are cleaned up
	conn1 := nextConn(t, connections)
	conn2 := nextConn(t, connections)
	waitDone(t, conn1)
	waitDone(t, conn2)
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSCycleOldConnFailsDuringQuiesce(t *testing.T) {
	connections, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	wsConfig := cycleTestConfig(url)
	wsConfig.ConnectionCycleInterval = 25 * time.Millisecond
	wsConfig.ConnectionCycleQuiesceTime = 1 * time.Minute
	afterConnect := func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`subscribe`))
	}

	wsc, err := New(context.Background(), wsConfig, nil, afterConnect)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)
	expectMsg(t, conn1, `subscribe`)
	c1 := wsc.(*wsClient).currentConn()

	conn2 := nextConn(t, connections)
	expectMsg(t, conn2, `subscribe`)
	waitForSwitch(t, wsc, c1) // the old connection is now quiescing

	// The server kills the old connection during the (very long) quiesce - the cycle
	// completes early rather than waiting for the quiesce timer, proven by the next
	// cycle establishing a third connection well within the quiesce time
	conn1.CloseConn()
	waitDone(t, conn1)
	conn3 := nextConn(t, connections)
	expectMsg(t, conn3, `subscribe`)

	wsc.Close()
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSCycleTimerResetOnErrorReconnect(t *testing.T) {
	connections, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	var preDisconnectCount atomic.Int32
	wsConfig := cycleTestConfig(url)
	wsConfig.ConnectionCycleInterval = 300 * time.Millisecond
	wsConfig.ConnectionCycleQuiesceTime = 25 * time.Millisecond
	wsConfig.PreDisconnectHandler = func(ctx context.Context, w WSClient) error {
		preDisconnectCount.Add(1)
		return w.Send(ctx, []byte(`unsubscribe`))
	}
	afterConnect := func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`subscribe`))
	}

	wsc, err := New(context.Background(), wsConfig, nil, afterConnect)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)
	expectMsg(t, conn1, `subscribe`)

	// Kill the connection - an error-driven reconnect, which does NOT run the
	// pre-disconnect handler, and resets the cycle timer
	conn1.CloseConn()
	conn2 := nextConn(t, connections)
	expectMsg(t, conn2, `subscribe`)
	assert.Equal(t, int32(0), preDisconnectCount.Load())

	// The next planned cycle from the reconnected connection does run it
	conn3 := nextConn(t, connections)
	expectMsg(t, conn2, `unsubscribe`)
	expectMsg(t, conn3, `subscribe`)
	assert.Equal(t, int32(1), preDisconnectCount.Load())

	wsc.Close()
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSCycleCloseDuringDialRetry(t *testing.T) {
	connections, url, rejectNext, done := NewTestWSServerMulti(nil)
	defer done()

	wsConfig := cycleTestConfig(url)
	wsConfig.ConnectionCycleInterval = 25 * time.Millisecond

	wsc, err := New(context.Background(), wsConfig, nil, nil)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)

	// The cycle fires and goes into dial retry - then we close mid-retry
	rejectNext(true)
	time.Sleep(100 * time.Millisecond)
	wsc.Close()

	waitDone(t, conn1)
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSCycleQuiesceZero(t *testing.T) {
	connections, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	wsConfig := cycleTestConfig(url)
	wsConfig.ConnectionCycleInterval = 50 * time.Millisecond
	wsConfig.ConnectionCycleQuiesceTime = 0 // immediate close after the switch
	afterConnect := func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`subscribe`))
	}

	wsc, err := New(context.Background(), wsConfig, nil, afterConnect)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)
	expectMsg(t, conn1, `subscribe`)

	conn2 := nextConn(t, connections)
	expectMsg(t, conn2, `subscribe`)
	waitDone(t, conn1)

	err = wsc.Send(context.Background(), []byte(`on new`))
	assert.NoError(t, err)
	expectMsg(t, conn2, `on new`)

	wsc.Close()
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSCycleInactiveWithDisableReconnect(t *testing.T) {
	connections, url, _, done := NewTestWSServerMulti(func(req *http.Request) {
		assert.Equal(t, "/", req.URL.Path)
	})
	defer done()

	// A plain HTTP request fails the websocket upgrade, and is ignored by the test server
	res, err := http.Get(strings.Replace(url, "ws://", "http://", 1))
	assert.NoError(t, err)
	res.Body.Close()

	wsConfig := cycleTestConfig(url)
	wsConfig.ConnectionCycleInterval = 20 * time.Millisecond
	wsConfig.DisableReconnect = true // warns, and cycling is inactive

	wsc, err := New(context.Background(), wsConfig, nil, nil)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)
	select {
	case <-connections:
		t.Fatal("unexpected second connection with cycling inactive")
	case <-time.After(150 * time.Millisecond):
	}

	wsc.Close()
	waitDone(t, conn1)
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSReconnectAfterConnectFailureCleansUp(t *testing.T) {
	// Not a cycling test (cycling disabled) - covers the teardown of a connection whose
	// afterConnect fails on an error-driven reconnect, before dialing again
	connections, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	var connCount atomic.Int32
	afterConnect := func(ctx context.Context, w WSClient) error {
		if connCount.Add(1) == 2 {
			return fmt.Errorf("pop")
		}
		return w.Send(ctx, []byte(`subscribe`))
	}

	wsConfig := cycleTestConfig(url)
	wsConfig.ConnectionCycleInterval = 0 // disabled

	wsc, err := New(context.Background(), wsConfig, nil, afterConnect)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)
	expectMsg(t, conn1, `subscribe`)

	// Kill the connection - the reconnect's afterConnect fails, so that connection is
	// torn down and the client reconnects again
	conn1.CloseConn()
	conn2 := nextConn(t, connections)
	waitDone(t, conn2)

	conn3 := nextConn(t, connections)
	expectMsg(t, conn3, `subscribe`)

	err = wsc.Send(context.Background(), []byte(`hello`))
	assert.NoError(t, err)
	expectMsg(t, conn3, `hello`)

	wsc.Close()
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSClosePreDisconnect(t *testing.T) {
	// The pre-disconnect handler runs on Close() too - even with cycling disabled - so
	// all pre-close processing lives in the one handler
	connections, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	wsConfig := &WSConfig{
		HTTPURL: url,
		PreDisconnectHandler: func(ctx context.Context, w WSClient) error {
			return w.Send(ctx, []byte(`unsubscribe`))
		},
	}

	wsc, err := New(context.Background(), wsConfig, nil, nil)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)

	wsc.Close()
	expectMsg(t, conn1, `unsubscribe`)
	waitDone(t, conn1)
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSCloseFromPreDisconnectNoDeadlock(t *testing.T) {
	connections, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	wsConfig := &WSConfig{HTTPURL: url}
	wsConfig.PreDisconnectHandler = func(ctx context.Context, w WSClient) error {
		w.Close()                              // re-entrant Close proceeds with the close - no recursion, no deadlock
		return w.Send(ctx, []byte(`too late`)) // fails, as the client is now closing
	}

	wsc, err := New(context.Background(), wsConfig, nil, nil)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	conn1 := nextConn(t, connections)

	wsc.Close()
	waitDone(t, conn1)
	_, ok := <-wsc.Receive()
	assert.False(t, ok)
}

func TestWSCloseNeverConnectedSkipsPreDisconnect(t *testing.T) {
	var preDisconnectCount atomic.Int32
	wsConfig := &WSConfig{
		HTTPURL: "http://localhost:12345",
		PreDisconnectHandler: func(ctx context.Context, w WSClient) error {
			preDisconnectCount.Add(1)
			return nil
		},
	}

	wsc, err := New(context.Background(), wsConfig, nil, nil)
	assert.NoError(t, err)

	// Never connected - there is no connection to run the handler against
	wsc.Close()
	assert.Equal(t, int32(0), preDisconnectCount.Load())
}

func TestWSSendRetriesOnConnectionCycle(t *testing.T) {
	w := &wsClient{
		ctx:     context.Background(),
		send:    make(chan []byte),
		closing: make(chan struct{}),
	}
	c1 := &wsConnection{w: w, sendDone: make(chan []byte)}
	c2 := &wsConnection{w: w, sendDone: make(chan []byte)}
	w.current = c1

	sendComplete := make(chan error, 1)
	go func() {
		sendComplete <- w.Send(context.Background(), []byte(`hello`))
	}()
	time.Sleep(20 * time.Millisecond) // let Send block in its select

	// Simulate a connection cycle - the new connection takes over the shared send
	// channel, and the old connection's sendLoop exits. Send must retry against the
	// new connection rather than failing.
	w.stateMux.Lock()
	w.current = c2
	w.stateMux.Unlock()
	close(c1.sendDone)

	assert.Equal(t, `hello`, string(<-w.send))
	assert.NoError(t, <-sendComplete)
}

func TestConnBoundClientSendErrors(t *testing.T) {
	w := &wsClient{
		ctx:     context.Background(),
		closing: make(chan struct{}),
	}
	c := &wsConnection{w: w, send: make(chan *trackedSend), sendDone: make(chan []byte)}

	// Rate limiter failure
	w.rateLimiter = rate.NewLimiter(rate.Limit(1), 0)
	err := w.boundTo(c).Send(context.Background(), []byte(`a`))
	assert.Regexp(t, "burst", err)
	w.rateLimiter = nil

	// Context cancelled
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	err = w.boundTo(c).Send(cancelled, []byte(`a`))
	assert.Regexp(t, "FF00146", err)

	// Connection retired (its sendLoop exited)
	close(c.sendDone)
	err = w.boundTo(c).Send(context.Background(), []byte(`a`))
	assert.Regexp(t, "FF00147", err)

	// Client closing
	c2 := &wsConnection{w: w, send: make(chan *trackedSend), sendDone: make(chan []byte)}
	close(w.closing)
	err = w.boundTo(c2).Send(context.Background(), []byte(`a`))
	assert.Regexp(t, "FF00147", err)
}

func TestConnBoundClientSendWriteFailure(t *testing.T) {
	_, url, _, done := NewTestWSServerMulti(nil)
	defer done()

	wsconn, _, err := websocket.DefaultDialer.Dial(url, nil)
	assert.NoError(t, err)
	wsconn.Close() // the write will fail

	w := &wsClient{
		ctx:     context.Background(),
		closing: make(chan struct{}),
	}
	c := w.newConnection(wsconn)
	go c.sendLoop()
	defer close(c.receiverDone)

	err = w.boundTo(c).Send(context.Background(), []byte(`fails to write`))
	assert.Regexp(t, "FF00147", err)
}

func TestConnBoundClientSendWriteTimeout(t *testing.T) {
	w := &wsClient{
		ctx:     context.Background(),
		closing: make(chan struct{}),
	}
	c := &wsConnection{w: w, send: make(chan *trackedSend), sendDone: make(chan []byte)}
	go func() { <-c.send }() // accepts the handoff, but the write never completes

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := w.boundTo(c).Send(ctx, []byte(`never written`))
	assert.Regexp(t, "FF00146", err)
}
