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
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hyperledger-firefly/common/pkg/ffresty"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/retry"
	"golang.org/x/time/rate"
)

type WSConfig struct {
	HTTPURL                   string             `json:"httpUrl,omitempty"`
	WebSocketURL              string             `json:"wsUrl,omitempty"`
	WSKeyPath                 string             `json:"wsKeyPath,omitempty"`
	ReadBufferSize            int                `json:"readBufferSize,omitempty"`
	WriteBufferSize           int                `json:"writeBufferSize,omitempty"`
	InitialDelay              time.Duration      `json:"initialDelay,omitempty"`
	MaximumDelay              time.Duration      `json:"maximumDelay,omitempty"`
	DelayFactor               float64            `json:"delayFactor,omitempty"`
	BackgroundConnect         bool               `json:"backgroundConnect,omitempty"`
	InitialConnectAttempts    int                `json:"initialConnectAttempts,omitempty"` // recommend backgroundConnect instead
	DisableReconnect          bool               `json:"disableReconnect"`
	AuthUsername              string             `json:"authUsername,omitempty"`
	AuthPassword              string             `json:"authPassword,omitempty"`
	ThrottleRequestsPerSecond int                `json:"requestsPerSecond,omitempty"`
	ThrottleBurst             int                `json:"burst,omitempty"`
	HTTPHeaders               fftypes.JSONObject `json:"headers,omitempty"`
	HeartbeatInterval         time.Duration      `json:"heartbeatInterval,omitempty"`
	TLSClientConfig           *tls.Config        `json:"tlsClientConfig,omitempty"`
	ConnectionTimeout         time.Duration      `json:"connectionTimeout,omitempty"`
	// NetDialer carries the custom DNS resolver and SSRF egress guard (CIDR denylist) for the
	// underlying TCP connection. Built by GenerateConfig from the net config; cannot be set in
	// JSON. Left nil for hand-built configs, in which case the default net dialer is used.
	NetDialer *net.Dialer `json:"-"`
	// ConnectionCycleInterval when non-zero enables proactive replacement of the connection
	// on this interval - the new connection is fully established (including afterConnect)
	// before sends switch over and the old connection is quiesced and closed.
	// The interval restarts from the end of each quiesce/close, and from any reconnect
	// due to a connection error - so at most two connections ever exist concurrently.
	ConnectionCycleInterval time.Duration `json:"connectionCycleInterval,omitempty"`
	// ConnectionCycleQuiesceTime is how long the old connection continues to deliver inbound
	// messages after a connection cycle switches sends to the new connection, before it is closed
	ConnectionCycleQuiesceTime time.Duration `json:"connectionCycleQuiesceTime,omitempty"`
	// The lifecycle handlers cannot be set in JSON - they must be configured on the code interface
	PreConnectHandler    WSPreConnectHandler    `json:"-"`
	PostConnectHandler   WSPostConnectHandler   `json:"-"`
	PreDisconnectHandler WSPreDisconnectHandler `json:"-"`
	// This one cannot be set in JSON - must be configured on the code interface
	ReceiveExt bool
}

type WSWrapConfig struct {
	HeartbeatInterval         time.Duration `json:"heartbeatInterval,omitempty"`
	ThrottleRequestsPerSecond int           `json:"requestsPerSecond,omitempty"`
	ThrottleBurst             int           `json:"burst,omitempty"`
	// This one cannot be set in JSON - must be configured on the code interface
	ReceiveExt bool
}

// WSPayload allows API consumers of this package to stream data, and inspect the message
// type, rather than just being passed the bytes directly.
type WSPayload struct {
	MessageType int
	Reader      io.Reader
	processed   chan struct{}
}

func NewWSPayload(mt int, r io.Reader) *WSPayload {
	return &WSPayload{
		MessageType: mt,
		Reader:      r,
		processed:   make(chan struct{}),
	}
}

// Must call done on each payload, before being delivered the next
func (wsp *WSPayload) Processed() {
	close(wsp.processed)
}

type WSClient interface {
	Connect() error
	Receive() <-chan []byte
	ReceiveExt() <-chan *WSPayload
	URL() string
	SetURL(url string)
	SetHeader(header, value string)
	Send(ctx context.Context, message []byte) error
	Close()
}

type wsClient struct {
	ctx                  context.Context
	headers              http.Header
	url                  string
	backgroundConnect    bool
	initialRetryAttempts int
	wsdialer             *websocket.Dialer
	current              *wsConnection
	connRetry            retry.Retry
	closed               bool
	useReceiveExt        bool
	receive              chan []byte
	receiveExt           chan *WSPayload
	send                 chan []byte
	bgConnCancelCtx      context.CancelFunc
	bgConnDone           chan struct{}
	closing              chan struct{}
	beforeConnect        WSPreConnectHandler
	afterConnect         WSPostConnectHandler
	beforeDisconnect     WSPreDisconnectHandler
	disableReconnect     bool
	heartbeatInterval    time.Duration
	connCycleInterval    time.Duration
	connCycleQuiesce     time.Duration
	stateMux             sync.Mutex // guards closed, current, bgConnDone and bgConnCancelCtx
	rateLimiter          *rate.Limiter
}

// WSPreConnectHandler will be called before every connect/reconnect. Any error returned will prevent the websocket from connecting.
type WSPreConnectHandler func(ctx context.Context, w WSClient) error

// WSPostConnectHandler will be called after every connect/reconnect. Can send data over ws, but must not block listening for data on the ws.
// Note: During auto-cycle this is called on the new connection, after the WSPreDisconnectHandler is called on the old one, but before the old connection is closed.
type WSPostConnectHandler func(ctx context.Context, w WSClient) error

// WSPreDisconnectHandler is called before a graceful close, to allow cleanup (such as unsubscribe):
//   - When closed explicitly
//   - When cycling the connection (after the new connection is established, before post-connect is called)
type WSPreDisconnectHandler func(ctx context.Context, w WSClient) error

// New creates a new outbound client that can be connected to a remote server.
// ** Recommend using NewWithConfig directly **
func New(ctx context.Context, config *WSConfig, beforeConnect WSPreConnectHandler, afterConnect WSPostConnectHandler) (WSClient, error) {
	conf := *config // copy, so we don't modify the supplied config with the handler overrides
	conf.PreConnectHandler = beforeConnect
	conf.PostConnectHandler = afterConnect
	return NewWithConfig(ctx, &conf)
}

// NewWithConfig creates a new outbound WebSocket client with configuration,
// including lifecycle hooks
func NewWithConfig(ctx context.Context, config *WSConfig) (WSClient, error) {
	l := log.L(ctx)
	wsURL, err := buildWSUrl(ctx, config)
	if err != nil {
		return nil, err
	}

	wsDialer := &websocket.Dialer{
		ReadBufferSize:   config.ReadBufferSize,
		WriteBufferSize:  config.WriteBufferSize,
		TLSClientConfig:  config.TLSClientConfig,
		HandshakeTimeout: config.ConnectionTimeout,
	}
	// Route the TCP connection through the configured dialer so the custom DNS resolver and
	// SSRF egress guard apply (TLS is still layered on top by gorilla via TLSClientConfig).
	if config.NetDialer != nil {
		wsDialer.NetDialContext = config.NetDialer.DialContext
	}

	w := &wsClient{
		ctx:      ctx,
		url:      wsURL,
		wsdialer: wsDialer,
		connRetry: retry.Retry{
			InitialDelay: config.InitialDelay,
			MaximumDelay: config.MaximumDelay,
			Factor:       config.DelayFactor,
		},
		backgroundConnect:    config.BackgroundConnect,
		initialRetryAttempts: config.InitialConnectAttempts,
		headers:              make(http.Header),
		send:                 make(chan []byte),
		closing:              make(chan struct{}),
		beforeConnect:        config.PreConnectHandler,
		afterConnect:         config.PostConnectHandler,
		beforeDisconnect:     config.PreDisconnectHandler,
		heartbeatInterval:    config.HeartbeatInterval,
		connCycleInterval:    config.ConnectionCycleInterval,
		connCycleQuiesce:     config.ConnectionCycleQuiesceTime,
		useReceiveExt:        config.ReceiveExt,
		disableReconnect:     config.DisableReconnect,
		rateLimiter:          ffresty.GetRateLimiter(config.ThrottleRequestsPerSecond, config.ThrottleBurst),
	}
	if w.connCycleInterval > 0 && w.disableReconnect {
		l.Warnf("WS %s connection cycling configured, but inactive as reconnect is disabled", w.url)
	}
	w.setupReceiveChannel()
	for k, v := range config.HTTPHeaders {
		if vs, ok := v.(string); ok {
			w.headers.Set(k, vs)
		}
	}
	authUsername := config.AuthUsername
	authPassword := config.AuthPassword
	if authUsername != "" && authPassword != "" {
		w.headers.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", authUsername, authPassword)))))
	}

	go func() {
		select {
		case <-ctx.Done():
			l.Tracef("WS %s closing due to canceled context", w.url)
			w.Close()
		case <-w.closing:
			l.Tracef("WS %s closing", w.url)
		}
	}()

	return w, nil
}

// Wrap an existing connection (including an inbound server connection) with heartbeating and throttling.
// No reconnect functions are supported when wrapping an existing connection like this, but the supplied
// callback will be invoked when the connection closes (allowing cleanup/tracking).
func Wrap(ctx context.Context, config WSWrapConfig, wsconn *websocket.Conn, onClose func()) WSClient {
	w := &wsClient{
		ctx:               ctx,
		url:               wsconn.LocalAddr().String(),
		disableReconnect:  true,
		heartbeatInterval: config.HeartbeatInterval,
		rateLimiter:       ffresty.GetRateLimiter(config.ThrottleRequestsPerSecond, config.ThrottleBurst),
		useReceiveExt:     config.ReceiveExt,
		send:              make(chan []byte),
		closing:           make(chan struct{}),
	}
	w.setupReceiveChannel()
	c := w.newConnection(wsconn)
	close(c.promoted) // sole connection - immediately the consumer of the shared send channel
	w.current = c
	log.L(ctx).Infof("WS %s wrapped", w.url)
	go func() {
		w.receiveReconnectLoop()
		onClose()
	}()
	return w
}

func (w *wsClient) setupReceiveChannel() {
	if w.useReceiveExt {
		w.receiveExt = make(chan *WSPayload)
	} else {
		w.receive = make(chan []byte)
	}
}

func (w *wsClient) Connect() error {

	// Initiate background connection option if configured (locks state on the stateMux)
	if w.startBackgroundConnect() {
		return nil
	}

	// Otherwise the initial connection occurs in the foreground
	return w.initialConnect()
}

func (w *wsClient) startBackgroundConnect() bool {
	w.stateMux.Lock()
	defer w.stateMux.Unlock()

	if !w.backgroundConnect || w.bgConnDone != nil {
		return false // foreground initial connection mode
	}

	bgConnDone := make(chan struct{}) // local var to use in go routine
	w.bgConnDone = bgConnDone
	w.ctx, w.bgConnCancelCtx = context.WithCancel(w.ctx)
	go func() {
		defer close(bgConnDone)
		err := w.initialConnect()
		if err != nil {
			// Retry means we only reach here if the context closes before initial connection
			log.L(w.ctx).Errorf("Connection to WebSocket %s was never established before shutdown: %s", w.url, err)
		}
	}()
	return true
}

func (w *wsClient) initialConnect() error {
	if err := w.connect(true); err != nil {
		return err
	}
	go w.receiveReconnectLoop()
	return nil
}

func (w *wsClient) Close() {
	// Run the pre-disconnect handler for an orderly close of the current connection, before
	// we start closing - the same handler that runs on a connection cycle, so pre-close
	// processing lives in one place. The claim ensures at most one invocation per connection
	// (a re-entrant or concurrent Close proceeds straight to closing), and any error just
	// means we fall back to relying on server-side cleanup.
	if w.beforeDisconnect != nil {
		if c := w.currentConn(); c != nil && !w.isClosed() && c.claimPreDisconnect() {
			if err := w.beforeDisconnect(w.ctx, w.boundTo(c)); err != nil {
				log.L(w.ctx).Warnf("WS %s pre-disconnect handler failed in close: %s", w.url, err)
			}
		}
	}

	c, bgConnDone, bgConnCancelCtx, alreadyClosed := w.markClosed()
	if alreadyClosed {
		return
	}
	if c != nil {
		c.closeConn()
	}
	if bgConnDone != nil {
		// Cancel the background connect routine and wait for it to exit. Note we must not
		// be holding stateMux while we wait, as that routine takes it via isClosed()
		bgConnCancelCtx()
		<-bgConnDone
	}
}

// markClosed transitions the client to closed exactly once, returning the resources the
// caller must then clean up outside of the lock.
// Note an old connection mid-quiesce during a cycle is not returned here (handled by cycleConnection)
func (w *wsClient) markClosed() (c *wsConnection, bgConnDone chan struct{}, bgConnCancelCtx context.CancelFunc, alreadyClosed bool) {
	w.stateMux.Lock()
	defer w.stateMux.Unlock()

	if w.closed {
		return nil, nil, nil, true
	}
	w.closed = true
	close(w.closing)

	// bgConnCancelCtx+bgConnDone are both set as a pair in the stateMux, so if one is non-nil they both are
	c, bgConnDone, bgConnCancelCtx = w.current, w.bgConnDone, w.bgConnCancelCtx
	w.bgConnDone = nil
	return c, bgConnDone, bgConnCancelCtx, false
}

// checks the closed var under the stateMux
func (w *wsClient) isClosed() bool {
	w.stateMux.Lock()
	defer w.stateMux.Unlock()
	return w.closed
}

// currentConn gets the current connection under the stateMux
func (w *wsClient) currentConn() *wsConnection {
	w.stateMux.Lock()
	defer w.stateMux.Unlock()
	return w.current
}

// called when we've just connected a new underlying connection, returning false
// if the wsClient was cleaned up in the meantime - meaning the caller has an orphaned
// connection they need to close.
func (w *wsClient) setCurrentIfNotClosed(c *wsConnection) bool {
	w.stateMux.Lock()
	defer w.stateMux.Unlock()
	if w.closed {
		return false
	}
	w.current = c
	return true
}

func (w *wsClient) clearCurrentIf(c *wsConnection) {
	w.stateMux.Lock()
	defer w.stateMux.Unlock()

	if w.current == c {
		w.current = nil
	}
}

// promoteConnection atomically switches the connection from old to newC during a cycle
func (w *wsClient) promoteConnection(old, newC *wsConnection) bool {
	w.stateMux.Lock()
	defer w.stateMux.Unlock()
	if w.closed {
		return false
	}
	w.current = newC
	close(old.demoted)
	close(newC.promoted)
	return true
}

func (w *wsClient) Receive() <-chan []byte {
	return w.receive
}

// Must set ReceiveExt on the WSConfig to use this
func (w *wsClient) ReceiveExt() <-chan *WSPayload {
	return w.receiveExt
}

func (w *wsClient) URL() string {
	return w.url
}

func (w *wsClient) SetURL(url string) {
	w.url = url
}

func (w *wsClient) SetHeader(header, value string) {
	w.headers.Set(header, value)
}

func (w *wsClient) waitRateLimiter(ctx context.Context) error {
	if w.rateLimiter != nil {
		// Wait for permission to proceed with the request
		return w.rateLimiter.Wait(ctx)
	}
	return nil
}

func (w *wsClient) Send(ctx context.Context, message []byte) error {
	if err := w.waitRateLimiter(ctx); err != nil {
		return err
	}
	// Send
	for {
		// The sendDone of the current connection guards against blocking forever when that
		// connection's sender loop has exited - needed because the receiver can actually
		// call the sender indirectly on reconnect, so if the sender loop fails the
		// receiver can get blocked
		var sendDone chan []byte
		c := w.currentConn()
		if c != nil {
			sendDone = c.sendDone
		}
		select {
		case w.send <- message:
			return nil
		case <-ctx.Done():
			return i18n.NewError(ctx, i18n.MsgWSSendTimedOut)
		case <-sendDone:
			if w.currentConn() != c {
				continue // the connection was replaced under us (reconnect/cycle) - retry against the new one
			}
			return i18n.NewError(ctx, i18n.MsgWSClosing)
		case <-w.closing:
			return i18n.NewError(ctx, i18n.MsgWSClosing)
		}
	}
}

// connBoundClient is the WSClient facade supplied to the connect/disconnect handlers -
// its Send() is bound to one specific connection (as is promised during a cycle).
type connBoundClient struct {
	*wsClient
	c *wsConnection
}

func (bc *connBoundClient) Send(ctx context.Context, message []byte) error {
	if err := bc.waitRateLimiter(ctx); err != nil {
		return err
	}
	bs := &trackedSend{message: message, sent: make(chan bool, 1)}
	select {
	case bc.c.send <- bs:
		// Handed off - now wait for the write to complete, so a pre-disconnect handler's
		// messages are on the wire before the connection is closed behind it
		select {
		case ok := <-bs.sent:
			if !ok {
				return i18n.NewError(ctx, i18n.MsgWSClosing)
			}
			return nil
		case <-ctx.Done():
			return i18n.NewError(ctx, i18n.MsgWSSendTimedOut)
		}
	case <-ctx.Done():
		return i18n.NewError(ctx, i18n.MsgWSSendTimedOut)
	case <-bc.c.sendDone:
		return i18n.NewError(ctx, i18n.MsgWSClosing)
	case <-bc.closing:
		return i18n.NewError(ctx, i18n.MsgWSClosing)
	}
}

func (w *wsClient) boundTo(c *wsConnection) WSClient {
	return &connBoundClient{wsClient: w, c: c}
}

func buildWSUrl(ctx context.Context, config *WSConfig) (string, error) {
	if config.WebSocketURL != "" {
		u, err := url.Parse(config.WebSocketURL)
		if err != nil || !strings.HasPrefix(u.Scheme, "ws") {
			return "", i18n.WrapError(ctx, err, i18n.MsgInvalidWebSocketURL, config.WebSocketURL)
		}
		return u.String(), nil
	}

	u, err := url.Parse(config.HTTPURL)
	if err != nil {
		return "", i18n.WrapError(ctx, err, i18n.MsgInvalidURL, config.HTTPURL)
	}
	if config.WSKeyPath != "" {
		u.Path = config.WSKeyPath
	}
	if u.Scheme == "http" {
		u.Scheme = "ws"
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	}
	if config.AuthUsername == "" && config.AuthPassword == "" && u.User != nil {
		config.AuthUsername = u.User.Username()
		config.AuthPassword, _ = u.User.Password()
	}
	u.User = nil
	return u.String(), nil
}

// dialConnectionAttempt makes a single connect attempt (including the beforeConnect handler).
// Does not start send/receive loops, or set it as the active connection.
func (w *wsClient) dialConnectionAttempt(attempt int) (*wsConnection, error) {
	l := log.L(w.ctx)
	if w.beforeConnect != nil {
		if err := w.beforeConnect(w.ctx, w); err != nil {
			l.Warnf("WS %s connect attempt %d failed in beforeConnect", w.url, attempt)
			return nil, err
		}
	}

	conn, res, err := w.wsdialer.DialContext(w.ctx, w.url, w.headers)
	if err != nil {
		errMsg := err.Error()
		var status = -1
		if res != nil {
			b, readErr := io.ReadAll(res.Body)
			res.Body.Close()
			if readErr == nil && len(b) > 0 {
				// The info we need is what the server returned and the status
				errMsg = string(b)
			}
			status = res.StatusCode
		}
		l.Warnf("WS %s connect attempt %d failed [%d]: %s", w.url, attempt, status, errMsg)
		return nil, i18n.WrapError(w.ctx, err, i18n.MsgWSConnectFailed)
	}
	return w.newConnection(conn), nil
}

func (w *wsClient) connect(initial bool) error {
	l := log.L(w.ctx)
	l.Debugf("WS %s connecting, isInitial: %t", w.url, initial)
	return w.connRetry.DoCustomLog(w.ctx, func(attempt int) (retry bool, err error) {
		if w.isClosed() {
			l.Errorf("WS %s is closed, no retry will be attempted", w.url)
			return false, i18n.NewError(w.ctx, i18n.MsgWSClosing)
		}
		l.Debugf("WS %s connect attempt %d", w.url, attempt)
		retry = w.backgroundConnect || !initial || attempt < w.initialRetryAttempts
		c, err := w.dialConnectionAttempt(attempt)
		if err != nil {
			return retry, err
		}
		if !w.setCurrentIfNotClosed(c) {
			c.closeConn() // we have to clean up the orphan we just created
			return false, i18n.NewError(w.ctx, i18n.MsgWSClosing)
		}
		l.Debugf("WS %s connect attempt %d succeeded", w.url, attempt)
		close(c.promoted) // sole connection - immediately the consumer of the shared send channel
		l.Infof("WS %s connected", w.url)
		return false, nil
	})
}

func (w *wsClient) receiveReconnectLoop() {
	l := log.L(w.ctx)
	if w.useReceiveExt {
		defer close(w.receiveExt)
	} else {
		defer close(w.receive)
	}

	// Connection cycling proactively replaces the connection on a regular interval, when
	// enabled. The timer restarts after the completion of each cycle (the end of the
	// quiesce period), and after any reconnect due to a connection error.
	var cycleC <-chan time.Time
	var cycleTimer *time.Timer
	cyclingEnabled := w.connCycleInterval > 0 && !w.disableReconnect
	if cyclingEnabled {
		cycleTimer = time.NewTimer(w.connCycleInterval)
		defer cycleTimer.Stop()
		cycleC = cycleTimer.C
	}
	resetCycleTimer := func() {
		if cyclingEnabled {
			cycleTimer.Reset(w.connCycleInterval)
		}
	}

	for !w.isClosed() {
		// Start the sender, letting it close without blocking sending a notification on the sendDone
		c := w.currentConn()
		c.startSender()

		// Call the reconnect processor, bound to this connection
		var err error
		if w.afterConnect != nil {
			err = w.afterConnect(w.ctx, w.boundTo(c))
			l.Debugf("WS %s afterConnect (error: %v)", w.url, err)
		}

		if err == nil {
			c.startReader()
		connected:
			for {
				select {
				case <-c.readDone:
					// The reader exited - a connection error, server close, or Close()
					w.teardownConnection(c)
					w.clearCurrentIf(c)
					l.Debugf("WS %s reset the connection", w.url)
					break connected
				case <-cycleC:
					if newC, ok := w.cycleConnection(c); ok {
						c = newC // adopt the new connection - its send/read loops are already running
					}
					// if !ok the client is closing - the readDone/isClosed checks handle the exit
					resetCycleTimer()
				}
			}
		} else {
			// Ensure the connection and its sender are fully cleaned up before we reconnect
			w.teardownConnection(c)
			w.clearCurrentIf(c)
		}

		if w.disableReconnect {
			l.Infof("WS %s exiting (disableReconnect)", w.url)
			return
		}

		// Go into reconnect
		if !w.isClosed() {
			err = w.connect(false)
			if err != nil {
				l.Errorf("WS %s exiting due to connect error: %v", w.url, err)
				return
			}
			resetCycleTimer()
		}
	}
}

// cycleConnection runs on the receiveReconnectLoop goroutine when a cycle is due.
// It does not return until either the old connection is fully torn down, or the client is
// closing. This ensures we never have more than two connections.
func (w *wsClient) cycleConnection(old *wsConnection) (*wsConnection, bool) {
	l := log.L(w.ctx)
	l.Debugf("WS %s connection cycle starting", w.url)

	// Establish the new connection - with infinite retry (like reconnect), keeping the
	// old connection fully active (sending, receiving and heartbeating) throughout.
	var newC *wsConnection
	err := w.connRetry.DoCustomLog(w.ctx, func(attempt int) (retry bool, err error) {
		if w.isClosed() {
			return false, i18n.NewError(w.ctx, i18n.MsgWSClosing)
		}
		c, err := w.dialConnectionAttempt(attempt)
		if err != nil {
			return true, err // the old connection remains active while we retry
		}
		c.startSender() // services connection-bound sends from the hooks (not yet promoted)

		// The pre-disconnect handler gets to run on cycle, as well as close.
		// This happens before the quiesce cycle, so things like subscriptions are only active
		// on a single connection. In-flight request/reply exchanges can continue on the old connection.
		if w.beforeDisconnect != nil && old.claimPreDisconnect() {
			if pdErr := w.beforeDisconnect(w.ctx, w.boundTo(old)); pdErr != nil {
				l.Warnf("WS %s pre-disconnect handler failed (continuing connection cycle): %s", w.url, pdErr)
			}
		}

		// Call the connect processor against the new connection
		if w.afterConnect != nil {
			if acErr := w.afterConnect(w.ctx, w.boundTo(c)); acErr != nil {
				l.Warnf("WS %s connection cycle attempt %d failed in afterConnect: %s", w.url, attempt, acErr)
				w.teardownConnection(c)
				return true, acErr
			}
		}
		newC = c
		return false, nil
	})
	if err != nil {
		// Only reachable when the client is closing / the context is cancelled
		return nil, false
	}

	// Start reading, and atomically switch all new sends over to the new connection
	newC.startReader()
	if !w.promoteConnection(old, newC) {
		// The client was closed while we were cycling - clean up the orphaned new connection
		w.teardownConnection(newC)
		return nil, false
	}

	// Quiesce period - the old connection continues to deliver any in-flight inbound
	// messages to the receive channel, before we close it.
	old.setQuiesceDeadline(w.connCycleQuiesce)
	l.Infof("WS %s connection cycled, quiescing old connection for %s", w.url, w.connCycleQuiesce)
	quiesce := time.NewTimer(w.connCycleQuiesce)
	defer quiesce.Stop()
	select {
	case <-quiesce.C:
	case <-old.readDone: // the old connection failed during quiesce - just close it early
	case <-w.closing: // Close() handles the current (new) connection - we clean up the old one below
	}
	w.teardownConnection(old)
	return newC, true
}
