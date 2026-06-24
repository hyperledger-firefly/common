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

package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"sync"

	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/eventstreams"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/wsserver"
)

// Run from root of project with:
// go run ./examples/ffpubsub.go
func main() {
	ctx := context.Background()

	// Set ourselves up using an im-memory DB
	mgr, ims, done := setup(ctx)
	defer done()

	// This demo:  Just create a single event stream
	// Real world: You would create ffapi.Route objects for the CRUD actions on mgr
	_, err := mgr.UpsertStream(ctx, "demo", &eventstreams.GenericEventStream{})
	assertNoError(err)

	// Read lines from standard in, and pass them to all active routines
	log.L(ctx).Infof("Waiting for lines to publish on stdin...")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		ims.newMessages.L.Lock()
		index := len(ims.messages)
		ims.messages = append(ims.messages, scanner.Text())
		ims.newMessages.Broadcast()
		ims.newMessages.L.Unlock()
		log.L(ctx).Infof("Published %.12d", index)
	}
}

type pubSubESManager = eventstreams.Manager[*eventstreams.GenericEventStream]

type pubSubMessage struct {
	Message string `json:"message"`
}

// The most trivial data store
type inMemoryStream struct {
	messages    []string
	newMessages sync.Cond
}

func (ims *inMemoryStream) NewID() string {
	return fftypes.NewUUID().String()
}

func (ims *inMemoryStream) Validate(_ context.Context, _ *eventstreams.GenericEventStream) error {
	return nil // no config defined in pubSubConfig to validate
}

func (ims *inMemoryStream) WithRuntimeStatus(spec *eventstreams.GenericEventStream, status eventstreams.EventStreamStatus, stats *eventstreams.EventStreamStatistics) *eventstreams.GenericEventStream {
	return spec.WithRuntimeStatus(status, stats)
}

func (ims *inMemoryStream) Run(_ context.Context, _ *eventstreams.GenericEventStream, checkpointSequenceID string, deliver eventstreams.Deliver[pubSubMessage]) (err error) {
	var index int
	if checkpointSequenceID != "" {
		index, err = strconv.Atoi(checkpointSequenceID)
		if err != nil {
			return fmt.Errorf("bad checkpoint '%s': %s", checkpointSequenceID, err)
		}
	}
	for {
		// Get the next message
		var nextMsg string
		ims.newMessages.L.Lock()
		for nextMsg == "" {
			ims.newMessages.Wait()
			if index <= len(ims.messages)-1 {
				nextMsg = ims.messages[index]
				index++
			}
		}
		ims.newMessages.L.Unlock()

		// Deliver it
		if deliver([]*eventstreams.Event[pubSubMessage]{
			{
				EventCommon: eventstreams.EventCommon{
					Topic:      "topic1",
					SequenceID: fmt.Sprintf("%.12d", index),
				},
				Data: &pubSubMessage{
					Message: nextMsg,
				},
			},
		}) == eventstreams.Exit {
			return nil
		}
	}
}

func setup(ctx context.Context) (pubSubESManager, *inMemoryStream, func()) {
	// Use SQLite in-memory DB
	conf := config.RootSection("ffpubsub")
	eventstreams.InitConfig(conf)
	wsConf := conf.SubSection("ws")
	wsserver.InitConfig(wsConf)
	dbConf := conf.SubSection("sqlite")
	dbsql.InitSQLiteConfig(dbConf)
	dbConf.Set(dbsql.SQLConfMigrationsAuto, true)
	dbConf.Set(dbsql.SQLConfDatasourceURL, "file::memory:")
	dbConf.Set(dbsql.SQLConfMigrationsAuto, true)
	dbConf.Set(dbsql.SQLConfMigrationsDirectory, "./test/es_demo_migrations")
	dbConf.Set(dbsql.SQLConfMaxConnections, 1)

	sql, err := dbsql.NewSQLiteProvider(ctx, dbConf)
	assertNoError(err)

	wsServer := wsserver.NewWebSocketServer(ctx, wsserver.GenerateConfig(wsConf))
	server := httptest.NewServer(http.HandlerFunc(wsServer.Handler))
	u, err := url.Parse(server.URL)
	assertNoError(err)
	u.Scheme = "ws"
	log.L(ctx).Infof("Running on: %s", u)

	p := eventstreams.NewGenericEventStreamPersistence(sql, dbsql.UUIDValidator)
	c := eventstreams.GenerateConfig[*eventstreams.GenericEventStream, pubSubMessage](ctx)
	ims := &inMemoryStream{
		messages:    []string{},
		newMessages: *sync.NewCond(new(sync.Mutex)),
	}
	mgr, err := eventstreams.NewEventStreamManager[*eventstreams.GenericEventStream, pubSubMessage](ctx, c, p, wsServer, ims)
	assertNoError(err)
	return mgr, ims, func() {
		log.L(ctx).Infof("Shutting down")
		server.Close()
		p.Close()
	}
}

// trivial error handling in this example
func assertNoError(err error) {
	if err != nil {
		panic(err)
	}
}
