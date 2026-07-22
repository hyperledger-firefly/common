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
	"encoding/json"

	"github.com/hyperledger-firefly/common/pkg/wsserver"
)

const MessageTypeEventBatch = "event_batch"

type EventBatch[DataType any] struct {
	wsserver.BatchHeader
	Type   string             `json:"type"`   // always MessageTypeEventBatch (for consistent WebSocket flow control)
	Events []*Event[DataType] `json:"events"` // an array of events allows efficient batch acknowledgment
}

func (eb *EventBatch[DataType]) GetBatchHeader() *wsserver.BatchHeader {
	return &eb.BatchHeader
}

type Event[DataType any] struct {
	EventCommon
	// Data can be anything to deliver for the event - must be JSON marshalable.
	// Will be flattened into the struct.
	// Can define topic and/or sequenceId, but these will overridden with EventCommon strings in the JSON serialization.
	Data *DataType `json:"-"`
}

type EventCommon struct {
	Topic      string `json:"topic,omitempty"` // describes the sub-stream of events (optional) allowing sever-side event filtering (regexp)
	SequenceID string `json:"sequenceId"`      // deterministic ID for the event, that must be alpha-numerically orderable within the stream (numbers must be left-padded hex/decimal strings for ordering)
}

func (e *Event[DataType]) UnmarshalJSON(b []byte) error {
	*e = Event[DataType]{}
	err := json.Unmarshal(b, &e.EventCommon)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &e.Data)
}

func (e Event[DataType]) MarshalJSON() ([]byte, error) {
	dataJSON, err := json.Marshal(e.Data)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	_ = json.Unmarshal(dataJSON, &m)
	m["topic"] = e.Topic
	m["sequenceId"] = e.SequenceID
	return json.Marshal(m)
}
