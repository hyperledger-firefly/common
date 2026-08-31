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

package wsclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func generateConfig() *WSConfig {
	return &WSConfig{}
}

// lastPingCompleted reads the heartbeat state of the current connection race-safely
func lastPingCompleted(wsc WSClient) time.Time {
	c := wsc.(*wsClient).currentConn()
	if c == nil {
		return time.Time{}
	}
	c.heartbeatMux.Lock()
	defer c.heartbeatMux.Unlock()
	return c.lastPingCompleted
}

func TestWSClientE2ETLS(t *testing.T) {

	publicKeyFile, privateKeyFile := GenerateTLSCertficates(t)
	defer os.Remove(privateKeyFile.Name())
	defer os.Remove(publicKeyFile.Name())
	toServer, fromServer, url, close, err := NewTestTLSWSServer(func(req *http.Request) {
		assert.Equal(t, "in-beforeConnect", req.Header.Get("added-header"))
		assert.Equal(t, "/test/updated", req.URL.Path)
	}, publicKeyFile, privateKeyFile)
	defer close()
	assert.NoError(t, err)

	first := true
	beforeConnect := func(ctx context.Context, w WSClient) error {
		w.SetHeader("added-header", "in-beforeConnect")
		if first {
			first = false
			return fmt.Errorf("first run fails")
		}
		return nil
	}

	afterConnect := func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`after connect message`))
	}

	// Init clean config
	wsConfig := generateConfig()

	wsConfig.HTTPURL = url
	wsConfig.WSKeyPath = "/test"
	wsConfig.HeartbeatInterval = 50 * time.Millisecond
	wsConfig.InitialConnectAttempts = 2
	rootCAs := x509.NewCertPool()
	caPEM, _ := os.ReadFile(publicKeyFile.Name())
	ok := rootCAs.AppendCertsFromPEM(caPEM)
	assert.True(t, ok)
	cert, err := tls.LoadX509KeyPair(publicKeyFile.Name(), privateKeyFile.Name())
	assert.NoError(t, err)

	wsConfig.TLSClientConfig = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      rootCAs,
	}

	wsc, err := New(context.Background(), wsConfig, beforeConnect, afterConnect)
	assert.NoError(t, err)

	//  Change the settings and connect
	wsc.SetURL(wsc.URL() + "/updated")
	err = wsc.Connect()
	assert.NoError(t, err)

	// Test server rejects a 2nd connection
	wsc2, err := New(context.Background(), wsConfig, beforeConnect, afterConnect)
	assert.NoError(t, err)
	wsc2.SetURL(wsc2.URL() + "/updated")
	err = wsc2.Connect()
	assert.Error(t, err)
	wsc2.Close()

	// Receive the message automatically sent in afterConnect
	message1 := <-toServer
	assert.Equal(t, `after connect message`, message1)

	// Tell the unit test server to send us a reply, and confirm it
	fromServer <- `some data from server`
	reply := <-wsc.Receive()
	assert.Equal(t, `some data from server`, string(reply))

	// Send some data back
	err = wsc.Send(context.Background(), []byte(`some data to server`))
	assert.NoError(t, err)

	// Check the sevrer got it
	message2 := <-toServer
	assert.Equal(t, `some data to server`, message2)

	// Check heartbeating works
	beforePing := time.Now()
	for lastPingCompleted(wsc).Before(beforePing) {
		time.Sleep(10 * time.Millisecond)
	}

	// Close the client
	wsc.Close()

}

func TestWSClientE2EBG(t *testing.T) {

	toServer, fromServer, url, close := NewTestWSServer(func(req *http.Request) {
		assert.Equal(t, "/test/updated", req.URL.Path)
	})
	defer close()

	first := true
	beforeConnect := func(ctx context.Context, w WSClient) error {
		if first {
			first = false
			return fmt.Errorf("first run fails")
		}
		return nil
	}
	afterConnect := func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`after connect message`))
	}

	// Init clean config
	wsConfig := generateConfig()

	wsConfig.HTTPURL = url
	wsConfig.WSKeyPath = "/test"
	wsConfig.HeartbeatInterval = 50 * time.Millisecond
	wsConfig.BackgroundConnect = true

	wsc, err := New(context.Background(), wsConfig, beforeConnect, afterConnect)
	assert.NoError(t, err)

	//  Change the settings and connect
	wsc.SetURL(wsc.URL() + "/updated")
	err = wsc.Connect()
	assert.NoError(t, err)

	// Receive the message automatically sent in afterConnect
	message1 := <-toServer
	assert.Equal(t, `after connect message`, message1)

	// Tell the unit test server to send us a reply, and confirm it
	fromServer <- `some data from server`
	reply := <-wsc.Receive()
	assert.Equal(t, `some data from server`, string(reply))

	// Send some data back
	err = wsc.Send(context.Background(), []byte(`some data to server`))
	assert.NoError(t, err)

	// Check the sevrer got it
	message2 := <-toServer
	assert.Equal(t, `some data to server`, message2)

	// Check heartbeating works
	beforePing := time.Now()
	for lastPingCompleted(wsc).Before(beforePing) {
		time.Sleep(10 * time.Millisecond)
	}

	// Close the client
	wsc.Close()

}

func TestWSClientE2EReceiveExt(t *testing.T) {

	toServer, fromServer, url, close := NewTestWSServer(func(req *http.Request) {
		assert.Equal(t, "/test/updated", req.URL.Path)
	})
	defer close()

	first := true
	beforeConnect := func(ctx context.Context, w WSClient) error {
		if first {
			first = false
			return fmt.Errorf("first run fails")
		}
		return nil
	}
	afterConnect := func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`after connect message`))
	}

	// Init clean config
	wsConfig := generateConfig()

	wsConfig.HTTPURL = url
	wsConfig.WSKeyPath = "/test"
	wsConfig.HeartbeatInterval = 50 * time.Millisecond
	wsConfig.InitialConnectAttempts = 2
	wsConfig.ReceiveExt = true

	wsc, err := New(context.Background(), wsConfig, beforeConnect, afterConnect)
	assert.NoError(t, err)

	//  Change the settings and connect
	wsc.SetURL(wsc.URL() + "/updated")
	err = wsc.Connect()
	assert.NoError(t, err)

	// Receive the message automatically sent in afterConnect
	message1 := <-toServer
	assert.Equal(t, `after connect message`, message1)

	// Tell the unit test server to send us a reply, and confirm it
	fromServer <- `some data from server`
	reply := <-wsc.ReceiveExt()
	assert.Equal(t, websocket.TextMessage, reply.MessageType)
	msgData, err := io.ReadAll(reply.Reader)
	assert.NoError(t, err)
	assert.Equal(t, `some data from server`, string(msgData))
	reply.Processed()

	// Send some data back
	err = wsc.Send(context.Background(), []byte(`some data to server`))
	assert.NoError(t, err)

	// Check the sevrer got it
	message2 := <-toServer
	assert.Equal(t, `some data to server`, message2)

	// Check heartbeating works
	beforePing := time.Now()
	for lastPingCompleted(wsc).Before(beforePing) {
		time.Sleep(10 * time.Millisecond)
	}

	// Close the client
	wsc.Close()

}

func TestWSNeverConnectBG(t *testing.T) {
	closedSvr := httptest.NewServer(&http.ServeMux{})
	closedSvr.Close()

	wsc, err := New(context.Background(), &WSConfig{
		HTTPURL:           closedSvr.URL,
		BackgroundConnect: true,
	}, nil, nil)
	assert.NoError(t, err)
	err = wsc.Connect()
	assert.NoError(t, err)

	wsc.Close()
}

func TestWSBackgroundConnectCancelRace(t *testing.T) {
	// Connect() publishes the background connect state that Close() tears down, and Close()
	// is driven concurrently by cancellation of the context passed to New()
	closedSvr := httptest.NewServer(&http.ServeMux{})
	closedSvr.Close()

	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		wsc, err := New(ctx, &WSConfig{
			HTTPURL:           closedSvr.URL,
			BackgroundConnect: true,
			InitialDelay:      1 * time.Millisecond,
			MaximumDelay:      5 * time.Millisecond,
		}, nil, nil)
		require.NoError(t, err)

		wg := new(sync.WaitGroup)
		wg.Add(2)
		go func() {
			defer wg.Done()
			assert.NoError(t, wsc.Connect())
		}()
		go func() {
			defer wg.Done()
			cancel()
		}()
		wg.Wait()

		// Must be safe, whether or not the context driven close got there first
		wsc.Close()
		cancel()
	}
}

func TestWSConcurrentClose(t *testing.T) {
	closedSvr := httptest.NewServer(&http.ServeMux{})
	closedSvr.Close()

	wsc, err := New(context.Background(), &WSConfig{
		HTTPURL:           closedSvr.URL,
		BackgroundConnect: true,
		InitialDelay:      1 * time.Millisecond,
		MaximumDelay:      5 * time.Millisecond,
	}, nil, nil)
	require.NoError(t, err)
	require.NoError(t, wsc.Connect())

	wg := new(sync.WaitGroup)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wsc.Close()
		}()
	}
	wg.Wait()
}

func TestWSClosedWhileConnecting(t *testing.T) {
	// Note this test does not use NewTestWSServer, as it needs no interaction with the
	// server, and that server's connection tracking is not race detector safe
	upgrader := &websocket.Upgrader{}
	svr := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(res, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer svr.Close()

	wsConfig := generateConfig()
	wsConfig.HTTPURL = svr.URL

	var wsc WSClient
	wsc, err := New(context.Background(), wsConfig, func(ctx context.Context, w WSClient) error {
		// Close the client underneath ourselves, so the connection we establish is orphaned
		wsc.Close()
		return nil
	}, nil)
	require.NoError(t, err)

	err = wsc.Connect()
	assert.Regexp(t, "FF00147", err)
}

func TestWSClientBadWSURL(t *testing.T) {
	wsConfig := generateConfig()
	wsConfig.WebSocketURL = ":::"

	_, err := New(context.Background(), wsConfig, nil, nil)
	assert.Regexp(t, "FF00238", err)
}

func TestWSClientBadHTTPURL(t *testing.T) {
	wsConfig := generateConfig()
	wsConfig.HTTPURL = ":::"

	_, err := New(context.Background(), wsConfig, nil, nil)
	assert.Regexp(t, "FF00149", err)
}

func TestBasicAuthRemap(t *testing.T) {
	wsConfig := generateConfig()
	wsConfig.HTTPURL = "https://user:pass@test:12345"
	wsConfig.WSKeyPath = "/websocket"

	url, err := buildWSUrl(context.Background(), wsConfig)
	assert.NoError(t, err)
	assert.Equal(t, "wss://test:12345/websocket", url)
	assert.Equal(t, "user", wsConfig.AuthUsername)
	assert.Equal(t, "pass", wsConfig.AuthPassword)
}

func TestHTTPToWSURLRemap(t *testing.T) {
	wsConfig := generateConfig()
	wsConfig.HTTPURL = "http://test:12345"
	wsConfig.WSKeyPath = "/websocket"

	url, err := buildWSUrl(context.Background(), wsConfig)
	assert.NoError(t, err)
	assert.Equal(t, "ws://test:12345/websocket", url)
}

func TestHTTPSToWSSURLRemap(t *testing.T) {
	wsConfig := generateConfig()
	wsConfig.HTTPURL = "https://test:12345"

	url, err := buildWSUrl(context.Background(), wsConfig)
	assert.NoError(t, err)
	assert.Equal(t, "wss://test:12345", url)
}

func TestWSFailStartupHttp500(t *testing.T) {
	svr := httptest.NewServer(http.HandlerFunc(
		func(rw http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "custom value", r.Header.Get("Custom-Header"))
			assert.Equal(t, "Basic dXNlcjpwYXNz", r.Header.Get("Authorization"))
			rw.WriteHeader(500)
			rw.Write([]byte(`{"error": "pop"}`))
		},
	))
	defer svr.Close()

	wsConfig := generateConfig()
	wsConfig.WebSocketURL = fmt.Sprintf("ws://%s", svr.Listener.Addr())
	wsConfig.HTTPHeaders = map[string]interface{}{
		"custom-header": "custom value",
	}
	wsConfig.AuthUsername = "user"
	wsConfig.AuthPassword = "pass"
	wsConfig.InitialDelay = 1
	wsConfig.InitialConnectAttempts = 1

	w, _ := New(context.Background(), wsConfig, nil, nil)
	err := w.Connect()
	assert.Regexp(t, "FF00148", err)
}

func TestWSFailStartupConnect(t *testing.T) {

	svr := httptest.NewServer(http.HandlerFunc(
		func(rw http.ResponseWriter, r *http.Request) {
			rw.WriteHeader(500)
		},
	))
	svr.Close()

	wsConfig := generateConfig()
	wsConfig.HTTPURL = fmt.Sprintf("ws://%s", svr.Listener.Addr())
	wsConfig.InitialDelay = 1
	wsConfig.InitialConnectAttempts = 1

	w, _ := New(context.Background(), wsConfig, nil, nil)
	err := w.Connect()
	assert.Regexp(t, "FF00148", err)
}

func TestWSSendClosed(t *testing.T) {

	wsConfig := generateConfig()
	wsConfig.HTTPURL = "http://test:12345"

	w, err := New(context.Background(), wsConfig, nil, nil)
	assert.NoError(t, err)
	w.Close()

	err = w.Send(context.Background(), []byte(`sent after close`))
	assert.Regexp(t, "FF00147", err)
}

func TestWSSendCanceledContext(t *testing.T) {

	w := &wsClient{
		send:    make(chan []byte),
		closing: make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := w.Send(ctx, []byte(`sent after close`))
	assert.Regexp(t, "FF00146", err)
}

func TestWSSenderClosed(t *testing.T) {

	w := &wsClient{
		send: make(chan []byte),
	}
	c := &wsConnection{
		w:        w,
		sendDone: make(chan []byte),
	}
	w.current = c
	close(c.sendDone)

	err := w.Send(context.Background(), []byte(`sent after close`))
	assert.Regexp(t, "FF00147", err)
}

func TestWSConnectClosed(t *testing.T) {

	w := &wsClient{
		ctx:    context.Background(),
		closed: true,
	}

	err := w.connect(false)
	assert.Regexp(t, "FF00147", err)
}

func TestWSReadLoopSendFailure(t *testing.T) {

	toServer, fromServer, url, done := NewTestWSServer(nil)
	defer done()

	wsconn, _, err := websocket.DefaultDialer.Dial(url, nil)
	assert.NoError(t, err)
	wsconn.WriteJSON(map[string]string{"type": "listen", "topic": "topic1"})
	assert.NoError(t, err)
	<-toServer
	w := &wsClient{
		ctx: context.Background(),
	}
	c := &wsConnection{
		w:        w,
		conn:     wsconn,
		sendDone: make(chan []byte, 1),
	}

	// Queue a message for the receiver, then immediately close the sender channel
	fromServer <- `some data from server`
	close(c.sendDone)

	// Ensure the readLoop exits immediately
	c.readLoop()

	// Try reconnect, should fail here
	_, _, err = websocket.DefaultDialer.Dial(url, nil)
	assert.Error(t, err)

}

func TestWSReadLoopExtSendFailure(t *testing.T) {

	toServer, fromServer, url, done := NewTestWSServer(nil)
	defer done()

	wsconn, _, err := websocket.DefaultDialer.Dial(url, nil)
	assert.NoError(t, err)
	wsconn.WriteJSON(map[string]string{"type": "listen", "topic": "topic1"})
	assert.NoError(t, err)
	<-toServer
	w := &wsClient{
		ctx:           context.Background(),
		useReceiveExt: true,
	}
	c := &wsConnection{
		w:        w,
		conn:     wsconn,
		sendDone: make(chan []byte, 1),
	}

	// Queue a message for the receiver, then immediately close the sender channel
	fromServer <- `some data from server`
	close(c.sendDone)

	// Ensure the readLoop exits immediately
	c.readLoopExt()

	// Try reconnect, should fail here
	_, _, err = websocket.DefaultDialer.Dial(url, nil)
	assert.Error(t, err)

}

func TestWSReadLoopExtProcessedFailure(t *testing.T) {

	toServer, fromServer, url, done := NewTestWSServer(nil)
	defer done()

	wsconn, _, err := websocket.DefaultDialer.Dial(url, nil)
	assert.NoError(t, err)
	wsconn.WriteJSON(map[string]string{"type": "listen", "topic": "topic1"})
	assert.NoError(t, err)
	<-toServer
	w := &wsClient{
		ctx:           context.Background(),
		receiveExt:    make(chan *WSPayload),
		useReceiveExt: true,
	}
	c := &wsConnection{
		w:        w,
		conn:     wsconn,
		sendDone: make(chan []byte, 1),
	}

	// Queue a message for the receiver, then immediately close the sender channel
	fromServer <- `some data from server`

	// Ensure the readLoop exits immediately
	readLoopDone := make(chan struct{})
	go func() {
		c.readLoopExt()
		close(readLoopDone)
	}()

	_ = <-w.receiveExt // we don't bother to ack this, making the client wait indefinitely
	close(c.sendDone)
	<-readLoopDone

	// Try reconnect, should fail here
	_, _, err = websocket.DefaultDialer.Dial(url, nil)
	assert.Error(t, err)

}

func TestWSReconnectFail(t *testing.T) {

	_, _, url, done := NewTestWSServer(nil)
	defer done()

	wsconn, _, err := websocket.DefaultDialer.Dial(url, nil)
	assert.NoError(t, err)
	wsconn.Close()
	ctxCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	w := &wsClient{
		ctx:        ctxCanceled,
		receive:    make(chan []byte),
		receiveExt: make(chan *WSPayload),
		send:       make(chan []byte),
		closing:    make(chan struct{}),
	}
	c := w.newConnection(wsconn)
	close(c.promoted)
	w.current = c
	close(w.send) // will mean sender exits immediately

	w.receiveReconnectLoop()
}

func TestWSDisableReconnect(t *testing.T) {

	_, _, url, done := NewTestWSServer(nil)
	defer done()

	wsconn, _, err := websocket.DefaultDialer.Dial(url, nil)
	assert.NoError(t, err)
	wsconn.Close()
	ctxCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	w := &wsClient{
		ctx:              ctxCanceled,
		receive:          make(chan []byte),
		receiveExt:       make(chan *WSPayload),
		send:             make(chan []byte),
		closing:          make(chan struct{}),
		disableReconnect: true,
	}
	c := w.newConnection(wsconn)
	close(c.promoted)
	w.current = c

	w.receiveReconnectLoop()
}

func TestWSSendFail(t *testing.T) {

	_, _, url, done := NewTestWSServer(nil)
	defer done()

	wsconn, _, err := websocket.DefaultDialer.Dial(url, nil)
	assert.NoError(t, err)
	wsconn.Close()
	w := &wsClient{
		ctx:        context.Background(),
		receive:    make(chan []byte),
		receiveExt: make(chan *WSPayload),
		send:       make(chan []byte, 1),
		closing:    make(chan struct{}),
	}
	c := w.newConnection(wsconn)
	close(c.promoted)
	w.current = c
	w.send <- []byte(`wakes sender`)
	c.sendLoop()
	<-c.sendDone
}

func TestWSSendInstructClose(t *testing.T) {

	_, _, url, done := NewTestWSServer(nil)
	defer done()

	wsconn, _, err := websocket.DefaultDialer.Dial(url, nil)
	assert.NoError(t, err)
	wsconn.Close()
	w := &wsClient{
		ctx:        context.Background(),
		receive:    make(chan []byte),
		receiveExt: make(chan *WSPayload),
		send:       make(chan []byte, 1),
		closing:    make(chan struct{}),
	}
	c := w.newConnection(wsconn)
	close(c.receiverDone)
	c.sendLoop()
	<-c.sendDone
}

func TestHeartbeatTimedout(t *testing.T) {

	now := time.Now()
	w := &wsClient{
		ctx:               context.Background(),
		heartbeatInterval: 1 * time.Microsecond,
	}
	c := &wsConnection{
		w:              w,
		send:           make(chan *trackedSend),
		sendDone:       make(chan []byte),
		receiverDone:   make(chan struct{}),
		promoted:       make(chan struct{}),
		demoted:        make(chan struct{}),
		activePingSent: &now,
	}

	c.sendLoop()

}

func TestHeartbeatSendFailed(t *testing.T) {

	_, _, url, close := NewTestWSServer(func(req *http.Request) {})
	defer close()

	wsc, err := New(context.Background(), &WSConfig{HTTPURL: url}, nil, func(ctx context.Context, w WSClient) error { return nil })
	assert.NoError(t, err)
	defer wsc.Close()

	err = wsc.Connect()
	assert.NoError(t, err)

	// Close and use the underlying connection to drive a failure to send a heartbeat
	wsconn := wsc.(*wsClient).currentConn().conn
	wsconn.Close()
	w := &wsClient{
		ctx:               context.Background(),
		heartbeatInterval: 1 * time.Microsecond,
	}
	c := &wsConnection{
		w:            w,
		conn:         wsconn,
		send:         make(chan *trackedSend),
		sendDone:     make(chan []byte),
		receiverDone: make(chan struct{}),
		promoted:     make(chan struct{}),
		demoted:      make(chan struct{}),
	}

	c.sendLoop()

}

func TestTestServerFailsSecondConnect(t *testing.T) {

	_, _, url, done := NewTestWSServer(nil)
	defer done()

	wsc, err := New(context.Background(), &WSConfig{HTTPURL: url}, nil, func(ctx context.Context, w WSClient) error { return nil })
	assert.NoError(t, err)
	defer wsc.Close()

	err = wsc.Connect()
	assert.NoError(t, err)

	wsc2, err := New(context.Background(), &WSConfig{HTTPURL: url}, nil, func(ctx context.Context, w WSClient) error { return nil })
	assert.NoError(t, err)
	defer wsc2.Close()

	err = wsc2.Connect()
	assert.Error(t, err)

}

func TestTestTLSServerFailsBadCerts(t *testing.T) {

	filePath := path.Join(os.TempDir(), "badfile")
	err := os.WriteFile(filePath, []byte(`will be deleted`), 0644)
	assert.NoError(t, err)
	closedFile, err := os.Open(filePath)
	assert.NoError(t, err)
	err = closedFile.Close()
	assert.NoError(t, err)
	_, _, _, _, err = NewTestTLSWSServer(nil, closedFile, closedFile)
	assert.Error(t, err)

}

func TestWSClientContextClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	wsConfig := generateConfig()
	wsc, err := New(ctx, wsConfig, nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, wsc)
	err = wsc.Send(context.Background(), []byte{})
	assert.Regexp(t, "FF00147", err)
}

func TestRequestWithRateLimiter(t *testing.T) {
	rps := 5
	expectedNumberOfRequest := 20 // should take longer than 3 seconds less than 4 seconds

	wsMockCount := 0
	toServer, _, url, close := NewTestWSServer(func(req *http.Request) {
		wsMockCount++
		assert.Equal(t, "/test/updated", req.URL.Path)
	})
	defer close()

	beforeConnect := func(ctx context.Context, w WSClient) error {
		return nil
	}
	afterConnect := func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`after connect message`))
	}

	// Init clean config
	wsConfig := generateConfig()

	wsConfig.HTTPURL = url
	wsConfig.WSKeyPath = "/test"
	wsConfig.ThrottleRequestsPerSecond = rps

	wsc, err := New(context.Background(), wsConfig, beforeConnect, afterConnect)
	assert.NoError(t, err)

	//  Change the settings and connect
	wsc.SetURL(wsc.URL() + "/updated")
	err = wsc.Connect()
	assert.NoError(t, err)

	// Receive the message automatically sent in afterConnect
	message1 := <-toServer
	assert.Equal(t, `after connect message`, message1)

	requestChan := make(chan bool, expectedNumberOfRequest)
	startTime := time.Now()
	for i := 0; i < expectedNumberOfRequest; i++ {
		go func() {
			// Send some data back
			err := wsc.Send(context.Background(), []byte(`some data to server`))
			assert.NoError(t, err)

			// Check the sevrer got it
			message2 := <-toServer
			assert.Equal(t, `some data to server`, message2)
			requestChan <- true
		}()
	}
	count := 0
	for {
		<-requestChan
		count++
		if count == expectedNumberOfRequest {
			break

		}
	}

	// check duration is between 3 and 4 seconds based on the rate limit setting
	duration := time.Since(startTime)
	assert.GreaterOrEqual(t, duration, 3*time.Second)
	assert.LessOrEqual(t, duration, 4*time.Second)
	// Close the client
	wsc.Close()

}

func TestRequestWithRateLimiterHighBurst(t *testing.T) {
	expectedNumberOfRequest := 20 // allow all requests to be processed within 1 second

	wsMockCount := 0
	toServer, _, url, close := NewTestWSServer(func(req *http.Request) {
		wsMockCount++
		assert.Equal(t, "/test/updated", req.URL.Path)
	})
	defer close()

	beforeConnect := func(ctx context.Context, w WSClient) error {
		return nil
	}
	afterConnect := func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`after connect message`))
	}

	// Init clean config
	wsConfig := generateConfig()

	wsConfig.HTTPURL = url
	wsConfig.WSKeyPath = "/test"
	wsConfig.ThrottleBurst = expectedNumberOfRequest

	wsc, err := New(context.Background(), wsConfig, beforeConnect, afterConnect)
	assert.NoError(t, err)

	//  Change the settings and connect
	wsc.SetURL(wsc.URL() + "/updated")
	err = wsc.Connect()
	assert.NoError(t, err)

	// Receive the message automatically sent in afterConnect
	message1 := <-toServer
	assert.Equal(t, `after connect message`, message1)

	requestChan := make(chan bool, expectedNumberOfRequest)
	startTime := time.Now()
	for i := 0; i < expectedNumberOfRequest; i++ {
		go func() {
			// Send some data back
			err := wsc.Send(context.Background(), []byte(`some data to server`))
			assert.NoError(t, err)

			// Check the sevrer got it
			message2 := <-toServer
			assert.Equal(t, `some data to server`, message2)
			requestChan <- true
		}()
	}
	count := 0
	for {
		<-requestChan
		count++
		if count == expectedNumberOfRequest {
			break

		}
	}

	duration := time.Since(startTime)
	assert.Less(t, duration, 1*time.Second)
	// Close the client
	wsc.Close()
}

func TestRateLimiterFailure(t *testing.T) {
	toServer, _, url, close := NewTestWSServer(func(req *http.Request) {
		assert.Equal(t, "/test/updated", req.URL.Path)
	})
	defer close()

	beforeConnect := func(ctx context.Context, w WSClient) error {
		return nil
	}
	afterConnect := func(ctx context.Context, w WSClient) error {
		return w.Send(ctx, []byte(`after connect message`))
	}

	// Init clean config
	wsConfig := generateConfig()

	wsConfig.HTTPURL = url
	wsConfig.WSKeyPath = "/test"

	wsc, err := New(context.Background(), wsConfig, beforeConnect, afterConnect)
	assert.NoError(t, err)

	//  Change the settings and connect
	wsc.SetURL(wsc.URL() + "/updated")
	err = wsc.Connect()
	assert.NoError(t, err)

	// Receive the message automatically sent in afterConnect
	message1 := <-toServer
	assert.Equal(t, `after connect message`, message1)

	internalWsc := wsc.(*wsClient)
	internalWsc.rateLimiter = rate.NewLimiter(rate.Limit(1), 0) // artificially create an broken rate limiter, this is not possible with our config default
	// Send some data back
	err = wsc.Send(context.Background(), []byte(`some data to server`))
	assert.Error(t, err)
	assert.Regexp(t, "exceeds", err)

	// Close the client
	wsc.Close()
}

func TestWSWrap(t *testing.T) {
	ctx := context.Background()
	logrus.SetLevel(logrus.DebugLevel)

	passWS := make(chan (*websocket.Conn))
	svr := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		upgrader := &websocket.Upgrader{WriteBufferSize: 1024, ReadBufferSize: 1024}
		ws, err := upgrader.Upgrade(res, req, http.Header{})
		require.NoError(t, err)
		passWS <- ws
	}))
	defer svr.Close()

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)

		wsc, err := New(ctx, &WSConfig{HTTPURL: svr.URL}, nil, nil)
		require.NoError(t, err)
		err = wsc.Connect()
		require.NoError(t, err)

		wsc.Send(ctx, []byte(`hello`))
		msg1 := <-wsc.Receive()
		require.Equal(t, `hi`, string(msg1))

		wsc.Close()

	}()

	// Get the conn
	rawWSC := <-passWS

	// Wrap it
	serverDone := make(chan struct{})
	wsc := Wrap(ctx, WSWrapConfig{}, rawWSC, func() {
		close(serverDone)
	})

	msg1 := <-wsc.Receive()
	require.Equal(t, `hello`, string(msg1))
	err := wsc.Send(ctx, []byte(`hi`))
	require.NoError(t, err)

	<-clientDone
	<-serverDone
}
