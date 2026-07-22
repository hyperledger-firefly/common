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

package eventstreams

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
)

type Persistence[CT EventStreamSpec] interface {
	EventStreams() dbsql.CRUD[CT]
	Checkpoints() dbsql.CRUD[*EventStreamCheckpoint]
	IDValidator() IDValidator
	Close()
}

var CheckpointFilters = &ffapi.QueryFields{
	"id":         &ffapi.StringField{},
	"created":    &ffapi.TimeField{},
	"updated":    &ffapi.TimeField{},
	"sequenceid": &ffapi.StringField{},
}

type IDValidator func(ctx context.Context, idStr string) error

// This is a base object, and set of filters, that you can use if:
// - You are happy exposing all the built-in types of consumer (webhooks/websockets)
// - You do not need to extend the configuration in any way
// - You are happy using UUIDs for your IDs per dbsql.ResourceBase semantics
//
// A pre-built persistence library is provided, and sample migrations, that work
// with this structure.
//
// The design of the generic is such that you can start with the generic structure,
// and then move to your own structure later if you want to add more fields.
//
// When you are ready to extend, you need to:
//  1. Copy the GenericEventStream source into your own repo, and rename it appropriately.
//     Then you can add your extra configuration fields to it.
//  2. Copy the EventStreams() and Checkpoints() CRUD factories into your own repo,
//     and extend them with additional columns etc. as you see fit.
type GenericEventStream struct {
	dbsql.ResourceBase
	Type *EventStreamType `ffstruct:"eventstream" json:"type,omitempty" ffenum:"estype"`
	EventStreamSpecFields
	Webhook    *WebhookConfig         `ffstruct:"eventstream" json:"webhook,omitempty"`
	WebSocket  *WebSocketConfig       `ffstruct:"eventstream" json:"websocket,omitempty"`
	Statistics *EventStreamStatistics `ffstruct:"EventStream" json:"statistics,omitempty"`
}

var GenericEventStreamFilters = &ffapi.QueryFields{
	"id":                &ffapi.StringField{},
	"created":           &ffapi.TimeField{},
	"updated":           &ffapi.TimeField{},
	"name":              &ffapi.StringField{},
	"status":            &ffapi.StringField{},
	"type":              &ffapi.StringField{},
	"topicfilter":       &ffapi.StringField{},
	"initialsequenceid": &ffapi.StringField{},
}

func (ges *GenericEventStream) SetID(s string) {
	ges.ID = fftypes.MustParseUUID(s)
}

func (ges *GenericEventStream) IsNil() bool {
	return ges == nil
}

func (ges *GenericEventStream) ESFields() *EventStreamSpecFields {
	return &ges.EventStreamSpecFields
}

func (ges *GenericEventStream) ESType() EventStreamType {
	if ges.Type == nil {
		ges.Type = &EventStreamTypeWebSocket
	}
	return *ges.Type
}

func (ges *GenericEventStream) WebhookConf() *WebhookConfig {
	if ges.Webhook == nil {
		ges.Webhook = &WebhookConfig{}
	}
	return ges.Webhook
}

func (ges *GenericEventStream) WebSocketConf() *WebSocketConfig {
	if ges.WebSocket == nil {
		ges.WebSocket = &WebSocketConfig{}
	}
	return ges.WebSocket
}

func (ges *GenericEventStream) WithRuntimeStatus(status EventStreamStatus, stats *EventStreamStatistics) *GenericEventStream {
	newGES := &GenericEventStream{
		ResourceBase:          ges.ResourceBase,
		Type:                  ges.Type,
		EventStreamSpecFields: ges.EventStreamSpecFields,
		Webhook:               ges.Webhook,
		WebSocket:             ges.WebSocket,
		Statistics:            stats,
	}
	newGES.Status = &status
	return newGES
}

// NewGenericEventStreamPersistence is a helper that builds persistence with no extra config
// Users of this package can use this in cases where they do not have any additional configuration
// that needs to be persisted, and are happy using dbsql.ResourceBase for IDs.
func NewGenericEventStreamPersistence(db *dbsql.Database, idValidator IDValidator) Persistence[*GenericEventStream] {
	return &esPersistence[*GenericEventStream]{
		db:          db,
		idValidator: idValidator,
	}
}

type esPersistence[CT any] struct {
	db          *dbsql.Database
	idValidator IDValidator
}

func (p *esPersistence[CT]) IDValidator() IDValidator {
	return p.idValidator
}

func (p *esPersistence[CT]) EventStreams() dbsql.CRUD[*GenericEventStream] {
	return &dbsql.CrudBase[*GenericEventStream]{
		DB:    p.db,
		Table: "eventstreams",
		Columns: []string{
			dbsql.ColumnID,
			dbsql.ColumnCreated,
			dbsql.ColumnUpdated,
			"name",
			"status",
			"type",
			"initial_sequence_id",
			"topic_filter",
			"error_handling",
			"batch_size",
			"batch_timeout",
			"retry_timeout",
			"blocked_retry_delay",
			"webhook_config",
			"websocket_config",
		},
		FilterFieldMap: map[string]string{
			"initialsequenceid": "initial_sequence_id",
			"topicfilter":       "topic_filter",
		},
		NilValue:     func() *GenericEventStream { return nil },
		NewInstance:  func() *GenericEventStream { return &GenericEventStream{} },
		ScopedFilter: func() sq.Eq { return sq.Eq{} },
		EventHandler: nil, // set below
		NameField:    "name",
		QueryFactory: GenericEventStreamFilters,
		IDValidator:  p.idValidator,
		GetFieldPtr: func(inst *GenericEventStream, col string) interface{} {
			switch col {
			case dbsql.ColumnID:
				return &inst.ID
			case dbsql.ColumnCreated:
				return &inst.Created
			case dbsql.ColumnUpdated:
				return &inst.Updated
			case "name":
				return &inst.Name
			case "status":
				return &inst.Status
			case "type":
				return &inst.Type
			case "initial_sequence_id":
				return &inst.InitialSequenceID
			case "topic_filter":
				return &inst.TopicFilter
			case "error_handling":
				return &inst.ErrorHandling
			case "batch_size":
				return &inst.BatchSize
			case "batch_timeout":
				return &inst.BatchTimeout
			case "retry_timeout":
				return &inst.RetryTimeout
			case "blocked_retry_delay":
				return &inst.BlockedRetryDelay
			case "webhook_config":
				return &inst.Webhook
			case "websocket_config":
				return &inst.WebSocket
			}
			return nil
		},
	}
}

func (p *esPersistence[CT]) Checkpoints() dbsql.CRUD[*EventStreamCheckpoint] {
	return &dbsql.CrudBase[*EventStreamCheckpoint]{
		DB:    p.db,
		Table: "es_checkpoints",
		Columns: []string{
			dbsql.ColumnID,
			dbsql.ColumnCreated,
			dbsql.ColumnUpdated,
			"sequence_id",
		},
		FilterFieldMap: map[string]string{
			"sequenceid": "sequence_id",
		},
		NilValue:     func() *EventStreamCheckpoint { return nil },
		NewInstance:  func() *EventStreamCheckpoint { return &EventStreamCheckpoint{} },
		ScopedFilter: func() sq.Eq { return sq.Eq{} },
		EventHandler: nil, // set below
		QueryFactory: CheckpointFilters,
		IDValidator:  p.idValidator, // checkpoints share the ID of the eventstream
		GetFieldPtr: func(inst *EventStreamCheckpoint, col string) interface{} {
			switch col {
			case dbsql.ColumnID:
				return &inst.ID
			case dbsql.ColumnCreated:
				return &inst.Created
			case dbsql.ColumnUpdated:
				return &inst.Updated
			case "sequence_id":
				return &inst.SequenceID
			}
			return nil
		},
	}
}

func (p *esPersistence[CT]) Close() {
	p.db.Close()
}
