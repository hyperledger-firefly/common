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
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
)

// wsConnection holds all the state for a single underlying WebSocket connection.
// Normally a wsClient has exactly one of these at a time, but during a planned
// connection cycle (connectionCycleInterval) two coexist briefly - the old
// connection quiescing while the new connection takes over.
type wsConnection struct {
	w    *wsClient
	conn *websocket.Conn

	send          chan *trackedSend // connection-bound sends (used by the hook facade)
	sendDone      chan []byte       // closed by this connection's sendLoop on exit
	receiverDone  chan struct{}     // closed by teardown, telling the sendLoop to exit
	readDone      chan struct{}     // closed when the read loop goroutine returns
	promoted      chan struct{}     // closed when this connection becomes the consumer of the shared send channel
	demoted       chan struct{}     // closed when this connection stops being the current one (connection cycling)
	senderStarted bool              // only accessed on the orchestrating goroutine (receiveReconnectLoop)
	readerStarted bool              // only accessed on the orchestrating goroutine (receiveReconnectLoop)
	closeOnce     sync.Once
	teardownOnce  sync.Once

	// The preDisconnect handler runs at most once per connection - claimed either by a
	// connection cycle retiring this connection, or by Close() (whichever happens first)
	preDisconnected atomic.Bool

	// Heartbeat state is per-connection, so that during a connection cycle a pong on the
	// old connection cannot affect the new connection's ping bookkeeping or read deadline
	heartbeatMux      sync.Mutex
	activePingSent    *time.Time
	lastPingCompleted time.Time
	quiesceDeadline   time.Time // set when the connection is demoted during a connection cycle
}

// trackedSend is a send on this connection, with a success/failure notification,
// so the caller can block until the send is either on the wire or confirmed as failed.
type trackedSend struct {
	message []byte
	sent    chan bool // buffered(1) - receives true if the write succeeded
}

func (w *wsClient) newConnection(conn *websocket.Conn) *wsConnection {
	c := &wsConnection{
		w:            w,
		conn:         conn,
		send:         make(chan *trackedSend),
		sendDone:     make(chan []byte, 1),
		receiverDone: make(chan struct{}),
		readDone:     make(chan struct{}),
		promoted:     make(chan struct{}),
		demoted:      make(chan struct{}),
	}
	c.pongReceivedOrReset(false)
	conn.SetPongHandler(c.pongHandler)
	return c
}

func (c *wsConnection) startSender() {
	c.senderStarted = true
	go c.sendLoop()
}

func (c *wsConnection) startReader() {
	c.readerStarted = true
	go func() {
		defer close(c.readDone)
		if c.w.useReceiveExt {
			c.readLoopExt()
		} else {
			c.readLoop()
		}
	}()
}

// claimPreDisconnect returns true for exactly one caller per connection, electing it to
// run the preDisconnect handler for this connection
func (c *wsConnection) claimPreDisconnect() bool {
	return c.preDisconnected.CompareAndSwap(false, true)
}

// closeConn closes the underlying connection exactly once (teardown, quiesce completion,
// and Close() can all race to do this)
func (c *wsConnection) closeConn() {
	c.closeOnce.Do(func() {
		if err := c.conn.Close(); err != nil {
			log.L(c.w.ctx).Warnf("WS %s ignoring websocket connection close error: %v", c.w.url, err)
		}
	})
}

// teardownConnection fully stops a connection exactly once, closing the underlying
// connection and blocking until any started send/read loops have exited - so after
// this returns nothing can be delivered from this connection to the receive channel.
func (w *wsClient) teardownConnection(c *wsConnection) {
	c.teardownOnce.Do(func() {
		close(c.receiverDone) // tells the sendLoop to exit
		c.closeConn()         // unblocks a reader blocked in ReadMessage/NextReader
		if c.readerStarted {
			// A reader blocked delivering to the receive channel is unblocked by the close
			// of sendDone (from the sendLoop exiting on receiverDone above)
			<-c.readDone
		}
		if c.senderStarted {
			<-c.sendDone
		}
	})
}

func (c *wsConnection) readLoop() {
	w := c.w
	l := log.L(w.ctx)
	for {
		mt, message, err := c.conn.ReadMessage()
		if err != nil {
			// We treat this as informational, as it's normal for the client to disconnect here
			l.Infof("WS %s closed: %s", w.url, err)
			return
		}

		// Pass the message to the consumer
		l.Tracef("WS %s read (mt=%d): %s", w.url, mt, message)
		select {
		case <-c.sendDone:
			l.Debugf("WS %s closing reader after send error", w.url)
			return
		case w.receive <- message:
		}
	}
}

func (c *wsConnection) readLoopExt() {
	w := c.w
	l := log.L(w.ctx)
	for {
		// We set a deadline for twice the heartbeat interval - note we bump this on pong
		if deadline, hasDeadline := c.nextReadDeadline(); hasDeadline {
			_ = c.conn.SetReadDeadline(deadline)
		}

		mt, r, err := c.conn.NextReader()
		if err != nil {
			// We treat this as informational, as it's normal for the client to disconnect here
			l.Infof("WS %s closed: %s", w.url, err)
			return
		}

		// Pass the message to the consumer
		l.Tracef("WS %s read (mt=%d)", w.url, mt)
		payload := NewWSPayload(mt, r)
		select {
		case <-c.sendDone:
			l.Debugf("WS %s closing reader after send error (waiting for data)", w.url)
			return
		case w.receiveExt <- payload:
		}
		select {
		case <-payload.processed:
			// It's the callers responsibility to ensure they call done on this before we can get the next payload
		case <-c.sendDone:
			l.Debugf("WS %s closing reader after send error (waiting for processing of data by client)", w.url)
			return
		}
	}
}

func (c *wsConnection) sendLoop() {
	w := c.w
	l := log.L(w.ctx)
	defer close(c.sendDone)

	var sharedSend chan []byte // nil (never selected) until this connection is promoted
	promoted := c.promoted
	demoted := c.demoted
	isDemoted := false
	disconnecting := false
	for !disconnecting {
		timeoutContext, timeoutCancel := c.heartbeatTimeout(w.ctx, isDemoted)

		select {
		case message := <-sharedSend:
			disconnecting = c.writeText(message)
		case bs := <-c.send:
			disconnecting = c.writeText(bs.message)
			bs.sent <- !disconnecting
		case <-promoted:
			// We are now the current connection - consume the shared send channel
			sharedSend = w.send
			promoted = nil
		case <-demoted:
			// A connection cycle has replaced us - stop consuming the shared send
			// channel, and stop heartbeating (we just quiesce until closed)
			sharedSend = nil
			demoted = nil
			isDemoted = true
		case <-timeoutContext.Done():
			if err := c.heartbeatCheck(); err != nil {
				l.Errorf("WS %s closing: %s", w.url, err)
				disconnecting = true
			} else {
				l.Debugf("WS %s send heartbeat ping", w.url)
				if err := c.conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
					l.Errorf("WS %s heartbeat send failed: %s", w.url, err)
					disconnecting = true
				}
			}
		case <-c.receiverDone:
			l.Debugf("WS %s send loop exiting", w.url)
			disconnecting = true
		}

		timeoutCancel()
	}
}

func (c *wsConnection) writeText(message []byte) (disconnecting bool) {
	l := log.L(c.w.ctx)
	l.Tracef("WS sending: %s", message)
	if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
		l.Errorf("WS %s send failed: %s", c.w.url, err)
		return true
	}
	return false
}

func (c *wsConnection) pongHandler(_ string) error {
	c.pongReceivedOrReset(true)
	return nil
}

func (c *wsConnection) pongReceivedOrReset(isPong bool) {
	c.heartbeatMux.Lock()
	if isPong && c.activePingSent != nil {
		log.L(c.w.ctx).Debugf("WS %s heartbeat completed (pong) after %.2fms", c.w.url, float64(time.Since(*c.activePingSent))/float64(time.Millisecond))
	}
	c.lastPingCompleted = time.Now() // in new connection case we still want to consider now the time we completed the ping
	c.activePingSent = nil
	c.heartbeatMux.Unlock()

	// We set a deadline for twice the heartbeat interval
	if deadline, hasDeadline := c.nextReadDeadline(); hasDeadline {
		_ = c.conn.SetReadDeadline(deadline)
	}
}

// nextReadDeadline returns the deadline to use for the next read (heartbeat or regular).
// Caller must set it onto the c.conn to activate it.
func (c *wsConnection) nextReadDeadline() (deadline time.Time, hasDeadline bool) {
	c.heartbeatMux.Lock()
	defer c.heartbeatMux.Unlock()

	if !c.quiesceDeadline.IsZero() {
		return c.quiesceDeadline, true // quiesce deadline wins over heartbeat
	}
	if c.w.heartbeatInterval > 0 {
		return time.Now().Add(2 * c.w.heartbeatInterval), true
	}
	return time.Time{}, false
}

// setQuiesceDeadline calculates and applies the quiesce deadline for future reads
func (c *wsConnection) setQuiesceDeadline(quiesceTime time.Duration) {
	c.heartbeatMux.Lock()
	c.quiesceDeadline = time.Now().Add(quiesceTime + 500*time.Millisecond)
	c.heartbeatMux.Unlock()

	if deadline, hasDeadline := c.nextReadDeadline(); hasDeadline {
		_ = c.conn.SetReadDeadline(deadline)
	}
}

func (c *wsConnection) heartbeatCheck() error {
	c.heartbeatMux.Lock()
	defer c.heartbeatMux.Unlock()

	if c.activePingSent != nil {
		return i18n.NewError(c.w.ctx, i18n.MsgWSHeartbeatTimeout, float64(time.Since(*c.activePingSent))/float64(time.Millisecond))
	}
	log.L(c.w.ctx).Debugf("WS %s heartbeat timer popped (ping) after %.2fms", c.w.url, float64(time.Since(c.lastPingCompleted))/float64(time.Millisecond))
	now := time.Now()
	c.activePingSent = &now
	return nil
}

func (c *wsConnection) heartbeatTimeout(ctx context.Context, demoted bool) (context.Context, context.CancelFunc) {
	if c.w.heartbeatInterval > 0 && !demoted {
		c.heartbeatMux.Lock()
		baseTime := c.lastPingCompleted
		if c.activePingSent != nil {
			// We're waiting for a pong
			baseTime = *c.activePingSent
		}
		waitTime := c.w.heartbeatInterval - time.Since(baseTime) // if negative, will pop immediately
		c.heartbeatMux.Unlock()
		return context.WithTimeout(ctx, waitTime)
	}
	return context.WithCancel(ctx)
}
