// Copyright © 2025 Kaleido, Inc.
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
	"sync"

	"github.com/gorilla/websocket"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
)

// The Protocol interface layers a protocol on top of raw websockets, that allows the server side to:
//   - Model the concept of multiple "streams" on a single WebSocket
//   - Block until 1 or more connections are available that have "started" a particular stream
//   - Send a broadcast to all connections on a stream
//   - Send a single payload to a single selected connection on a stream, and wait for an "ack" back
//     from that specific websocket connection (or that websocket connection to disconnect)
//
// NOTE: This replaces a previous WebSocketChannels interface, which started its life in 2018
//
//	and attempted to solve the above problem set in a different way that had a challenging timing issue.
type Protocol interface {
	// Broadcast performs best-effort delivery to all connections currently active on the specified stream
	Broadcast(ctx context.Context, stream string, payload interface{})

	// NextRoundTrip blocks until at least one connection is started on the stream, and then
	// returns an interface that can be used to send a payload to exactly one of the attached
	// connections, and receive an ack/error from just the one connection that was picked.
	// - Returns an error if the context is closed.
	RoundTrip(ctx context.Context, stream string, payload WSBatch) (*WebSocketCommandMessage, error)
}

// WSBatch is any serializable structure that contains the batch header
type WSBatch interface {
	GetBatchHeader() *BatchHeader
}

type BatchHeader struct {
	BatchNumber int64  `json:"batchNumber,omitempty"` // In the case of broadcast mode we can omitempty as it's not needed
	Stream      string `json:"stream"`
}

// WebSocketServer is the full server interface with the init call
type WebSocketServer interface {
	Protocol
	Handler(w http.ResponseWriter, r *http.Request)
	Close()
}

type streamState struct {
	wlmCounter int64
	inflight   *roundTrip
	conns      []*webSocketConnection
}

type webSocketServer struct {
	ctx             context.Context
	conf            WebSocketServerConfig
	streamMap       map[string]*streamState
	streamMapChange chan struct{}
	mux             sync.Mutex
	upgrader        *websocket.Upgrader
	connections     map[string]*webSocketConnection
}

// NewWebSocketServer create a new server with a simplified interface
func NewWebSocketServer(bgCtx context.Context, config *WebSocketServerConfig) WebSocketServer {
	s := &webSocketServer{
		ctx:             bgCtx,
		connections:     make(map[string]*webSocketConnection),
		streamMap:       make(map[string]*streamState),
		streamMapChange: make(chan struct{}),
		conf:            *config,
		upgrader: &websocket.Upgrader{
			ReadBufferSize:  int(config.ReadBufferSize),
			WriteBufferSize: int(config.WriteBufferSize),
		},
	}
	return s
}

func (s *webSocketServer) Handler(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.L(s.ctx).Errorf("WebSocket upgrade failed: %s", err)
		return
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	c := newConnection(s.ctx, s, conn)
	s.connections[c.id] = c
}

func (s *webSocketServer) connectionClosed(c *webSocketConnection) {
	s.mux.Lock()
	defer s.mux.Unlock()
	delete(s.connections, c.id)
	for _, ss := range s.streamMap {
		// Remove the connection
		newSSConns := make([]*webSocketConnection, 0, len(ss.conns))
		for _, ssConn := range ss.conns {
			if ssConn.id != c.id {
				newSSConns = append(newSSConns, ssConn)
			}
		}
		ss.conns = newSSConns
	}
	close(s.streamMapChange)
	s.streamMapChange = make(chan struct{})
}

func (s *webSocketServer) Close() {
	s.mux.Lock()
	conns := make([]*webSocketConnection, 0, len(s.connections))
	for _, c := range s.connections {
		conns = append(conns, c)
	}
	s.mux.Unlock()
	for _, c := range conns {
		c.close()
	}
}

func (s *webSocketServer) Broadcast(ctx context.Context, stream string, payload interface{}) {
	s.mux.Lock()
	ss := s.streamMap[stream]
	if ss == nil {
		ss = &streamState{}
		s.streamMap[stream] = ss
	}
	conns := make([]*webSocketConnection, len(ss.conns))
	copy(conns, ss.conns)
	s.mux.Unlock()
	for _, c := range conns {
		select {
		case c.send <- payload:
		case <-c.closing:
			// This isn't an error, just move on
			log.L(ctx).Warnf("broadcast failed to closing connection '%s'", c.id)
		}
	}
}

type roundTrip struct {
	ss          *streamState
	conn        *webSocketConnection
	batchNumber int64
	done        chan struct{}
	err         error
	response    *WebSocketCommandMessage
}

func (s *webSocketServer) RoundTrip(ctx context.Context, stream string, payload WSBatch) (*WebSocketCommandMessage, error) {
	var rt *roundTrip
	err := s.waitStreamConnections(ctx, stream, func(ss *streamState) error {
		// If there's an inflight already, that's an error - the caller is required to call NextRoundTrip sequentially,
		// and always handle the cleanup of the RoundTripper
		if ss.inflight != nil {
			return i18n.NewError(s.ctx, i18n.MsgWebSocketBatchInflight, stream, ss.inflight.batchNumber, ss.inflight.conn.id)
		}
		// Do a round-robbin pick of one of the connections
		conn := ss.conns[ss.wlmCounter%int64(len(ss.conns))]
		ss.wlmCounter++
		// The wlmCounter is used as the batch number in the payload
		payload.GetBatchHeader().BatchNumber = ss.wlmCounter
		payload.GetBatchHeader().Stream = stream
		rt = &roundTrip{
			ss:          ss,
			conn:        conn,
			batchNumber: ss.wlmCounter, // batch number increments in this library
			done:        make(chan struct{}),
		}
		ss.inflight = rt
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Ensure we clean up before returning, including in error cases
	defer func() {
		s.mux.Lock()
		rt.ss.inflight = nil
		s.mux.Unlock()
	}()

	// Send the payload to initiate the exchange
	select {
	case rt.conn.send <- payload:
	case <-rt.conn.closing:
		// handle connection closure while waiting to send
		return nil, i18n.NewError(s.ctx, i18n.MsgWebSocketClosed, rt.conn.id)
	case <-ctx.Done():
		// ... or stop/reset of stream, or server shutdown
		return nil, i18n.NewError(s.ctx, i18n.MsgContextCanceled)
	}

	// Set the processing timeout to wait for batch acknowledgement
	ctxWithTimeout, cancelTimeout := context.WithTimeout(ctx, s.conf.AckTimeout)
	defer cancelTimeout()

	// Wait for the response
	select {
	case <-rt.done:
	case <-rt.conn.closing:
		// handle connection closure while waiting for ack
		return nil, i18n.NewError(s.ctx, i18n.MsgWebSocketClosed, rt.conn.id)
	case <-ctxWithTimeout.Done():
		// ... or time out, stop/reset of stream, or server shutdown
		return nil, i18n.NewError(s.ctx, i18n.MsgWebSocketRoundTripTimeout)
	}
	return rt.response, rt.err
}

func (s *webSocketServer) completeRoundTrip(stream string, msg *WebSocketCommandMessage, err error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	ss := s.streamMap[stream]
	if ss == nil || ss.inflight == nil {
		log.L(s.ctx).Warnf("Received spurious ack for batchNumber=%d while no batch in-flight for stream '%s'", msg.BatchNumber, stream)
		return
	}
	rt := ss.inflight
	// We accept batchNumber: 0 (omitted) as an nack/ack for the last thing sent
	if msg.BatchNumber > 0 && rt.batchNumber != msg.BatchNumber {
		log.L(s.ctx).Warnf("Received spurious ack for batchNumber=%d while batchNumber=%d in-flight for stream '%s'", msg.BatchNumber, rt.batchNumber, stream)
		return
	}
	rt.response = msg
	rt.err = err
	close(rt.done)
	// We are NOT responsible for clearing ss.inflight - that is the RoundTrip() function's job
}

// waits until at least one connection is started on the requested stream, and returns an
// snapshot list of all connections on that stream.
func (s *webSocketServer) waitStreamConnections(ctx context.Context, stream string, lockedCallback func(ss *streamState) error) error {
	for {
		// check if there are connections
		s.mux.Lock()
		streamMapChange := s.streamMapChange
		ss := s.streamMap[stream]
		if ss != nil && len(ss.conns) > 0 {
			err := lockedCallback(ss)
			s.mux.Unlock()
			return err
		}
		s.mux.Unlock()
		select {
		case <-streamMapChange:
		case <-ctx.Done():
			return i18n.NewError(ctx, i18n.MsgContextCanceled)
		}
	}
}

func (s *webSocketServer) streamStarted(c *webSocketConnection, stream string) {
	// Track that this connection is interested in this stream
	s.mux.Lock()
	defer s.mux.Unlock()
	ss := s.streamMap[stream]
	if ss == nil {
		ss = &streamState{}
		s.streamMap[stream] = ss
	}
	// Ignore duplicate starts on the same connection
	found := false
	for _, existing := range ss.conns {
		if existing.id == c.id {
			found = true
		}
	}
	if !found {
		ss.conns = append(ss.conns, c)
	}
	// Notify anyone waiting for a connection, and setup the next waiter
	close(s.streamMapChange)
	s.streamMapChange = make(chan struct{})
}
