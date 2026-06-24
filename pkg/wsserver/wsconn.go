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

package wsserver

import (
	"context"
	"strings"
	"sync"

	ws "github.com/gorilla/websocket"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
)

type webSocketConnection struct {
	ctx      context.Context
	id       string
	server   *webSocketServer
	conn     *ws.Conn
	closeMux sync.Mutex
	closed   bool
	send     chan interface{}
	closing  chan struct{}
}

type WebSocketCommandMessage struct {
	Type        string `json:"type,omitempty"`
	Stream      string `json:"stream,omitempty"` // name of the event stream
	Message     string `json:"message,omitempty"`
	BatchNumber int64  `json:"batchNumber,omitempty"`
}

func newConnection(bgCtx context.Context, server *webSocketServer, conn *ws.Conn) *webSocketConnection {
	id := fftypes.NewUUID().String()
	wsc := &webSocketConnection{
		ctx:     log.WithLogFields(bgCtx, "wsc", id),
		id:      id,
		server:  server,
		conn:    conn,
		send:    make(chan interface{}),
		closing: make(chan struct{}),
	}
	go wsc.listen()
	go wsc.sender()
	return wsc
}

func (c *webSocketConnection) close() {
	c.closeMux.Lock()
	if !c.closed {
		c.closed = true
		c.conn.Close()
		close(c.closing)
	}
	c.closeMux.Unlock()

	c.server.connectionClosed(c)
	log.L(c.ctx).Infof("Disconnected")
}

func (c *webSocketConnection) sender() {
	defer c.close()
	for {
		select {
		case payload := <-c.send:
			if err := c.conn.WriteJSON(payload); err != nil {
				log.L(c.ctx).Errorf("Send failed - closing connection: %s", err)
				return
			}
		case <-c.closing:
			log.L(c.ctx).Infof("Closing")
			return
		}
	}
}

func (c *webSocketConnection) listen() {
	defer c.close()
	log.L(c.ctx).Infof("Connected")
	for {
		var msg WebSocketCommandMessage
		err := c.conn.ReadJSON(&msg)
		if err != nil {
			log.L(c.ctx).Errorf("Error: %s", err)
			return
		}
		log.L(c.ctx).Tracef("Received: %+v", msg)
		switch strings.ToLower(msg.Type) {
		case "start":
			c.server.streamStarted(c, msg.Stream)
		case "ack":
			c.server.completeRoundTrip(msg.Stream, &msg, nil)
		case "error", "nack":
			c.server.completeRoundTrip(msg.Stream, &msg, i18n.NewError(c.ctx, i18n.MsgWSErrorFromClient, msg.Message))
		default:
			log.L(c.ctx).Errorf("Unexpected message type: %+v", msg)
		}
	}
}
