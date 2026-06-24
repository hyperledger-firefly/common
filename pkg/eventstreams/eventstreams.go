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
	"database/sql"
	"database/sql/driver"
	"regexp"
	"sync"

	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/common/pkg/retry"
)

type EventStreamType = fftypes.FFEnum

var (
	EventStreamTypeWebhook   = fftypes.FFEnumValue("estype", "webhook")
	EventStreamTypeWebSocket = fftypes.FFEnumValue("estype", "websocket")
)

type ErrorHandlingType = fftypes.FFEnum

var (
	ErrorHandlingTypeBlock = fftypes.FFEnumValue("ehtype", "block")
	ErrorHandlingTypeSkip  = fftypes.FFEnumValue("ehtype", "skip")
)

type DispatchStatus = fftypes.FFEnum

var (
	DispatchStatusDispatching = fftypes.FFEnumValue("edstatus", "dispatching")
	DispatchStatusRetrying    = fftypes.FFEnumValue("edstatus", "retrying")
	DispatchStatusBlocked     = fftypes.FFEnumValue("edstatus", "blocked")
	DispatchStatusComplete    = fftypes.FFEnumValue("edstatus", "complete")
	DispatchStatusSkipped     = fftypes.FFEnumValue("edstatus", "skipped")
)

type EventStreamStatus = fftypes.FFEnum

var (
	EventStreamStatusStarted         = fftypes.FFEnumValue("esstatus", "started")
	EventStreamStatusStopped         = fftypes.FFEnumValue("esstatus", "stopped")
	EventStreamStatusDeleted         = fftypes.FFEnumValue("esstatus", "deleted")
	EventStreamStatusStopping        = fftypes.FFEnumValue("esstatus", "stopping")         // not persisted
	EventStreamStatusStoppingDeleted = fftypes.FFEnumValue("esstatus", "stopping_deleted") // not persisted
	EventStreamStatusUnknown         = fftypes.FFEnumValue("esstatus", "unknown")          // not persisted
)

type LifecyclePhase int

const (
	LifecyclePhasePreInsertValidation LifecyclePhase = iota // on user-supplied context, prior to inserting to DB
	LifecyclePhaseStarting                                  // while initializing for startup (so all defaults should be resolved)
)

// Let's us check that the config serializes
type DBSerializable interface {
	sql.Scanner
	driver.Valuer
}

type EventStreamSpec interface {
	dbsql.Resource
	SetID(s string)
	ESFields() *EventStreamSpecFields
	ESType() EventStreamType         // separated from fields to allow choice on restrictions
	WebhookConf() *WebhookConfig     // can return nil if Webhooks not supported
	WebSocketConf() *WebSocketConfig // can return nil if WebSockets not supported
	IsNil() bool                     // needed as quirk of using interfaces with generics
}

type EventStreamSpecFields struct {
	Name              *string            `ffstruct:"eventstream" json:"name,omitempty"`
	Status            *EventStreamStatus `ffstruct:"eventstream" json:"status,omitempty"`
	InitialSequenceID *string            `ffstruct:"eventstream" json:"initialSequenceId,omitempty"`
	TopicFilter       *string            `ffstruct:"eventstream" json:"topicFilter,omitempty"`

	ErrorHandling     *ErrorHandlingType  `ffstruct:"eventstream" json:"errorHandling"`
	BatchSize         *int                `ffstruct:"eventstream" json:"batchSize"`
	BatchTimeout      *fftypes.FFDuration `ffstruct:"eventstream" json:"batchTimeout"`
	RetryTimeout      *fftypes.FFDuration `ffstruct:"eventstream" json:"retryTimeout"`
	BlockedRetryDelay *fftypes.FFDuration `ffstruct:"eventstream" json:"blockedRetryDelay"`

	topicFilterRegexp *regexp.Regexp
}

type EventStreamStatistics struct {
	StartTime            *fftypes.FFTime `ffstruct:"EventStreamStatistics" json:"startTime"`
	LastDispatchTime     *fftypes.FFTime `ffstruct:"EventStreamStatistics" json:"lastDispatchTime"`
	LastDispatchAttempts int             `ffstruct:"EventStreamStatistics" json:"lastDispatchAttempts,omitempty"`
	LastDispatchFailure  string          `ffstruct:"EventStreamStatistics" json:"lastDispatchFailure,omitempty"`
	LastDispatchStatus   DispatchStatus  `ffstruct:"EventStreamStatistics" json:"lastDispatchComplete"`
	HighestDetected      string          `ffstruct:"EventStreamStatistics" json:"highestDetected"`
	HighestDispatched    string          `ffstruct:"EventStreamStatistics" json:"highestDispatched"`
	Checkpoint           string          `ffstruct:"EventStreamStatistics" json:"checkpoint"`
}

type EventStreamCheckpoint struct {
	ID         *string         `ffstruct:"EventStreamCheckpoint" json:"id"`
	Created    *fftypes.FFTime `ffstruct:"EventStreamCheckpoint" json:"created"`
	Updated    *fftypes.FFTime `ffstruct:"EventStreamCheckpoint" json:"updated"`
	SequenceID *string         `ffstruct:"EventStreamCheckpoint" json:"sequenceId,omitempty"`
}

func (esc *EventStreamCheckpoint) GetID() string {
	if esc.ID == nil {
		return ""
	}
	return *esc.ID
}

func (esc *EventStreamCheckpoint) SetCreated(t *fftypes.FFTime) {
	esc.Created = t
}

func (esc *EventStreamCheckpoint) SetUpdated(t *fftypes.FFTime) {
	esc.Updated = t
}

type Dispatcher[DT any] interface {
	AttemptDispatch(ctx context.Context, attempt int, events *EventBatch[DT]) error
}

func checkSet[T any](ctx context.Context, storeDefaults bool, fieldName string, fieldPtr **T, defValue T, check func(v T) bool) error {
	var v T
	if *fieldPtr != nil {
		v = **fieldPtr
	} else {
		if storeDefaults {
			*fieldPtr = &defValue
		}
		v = defValue
	}
	log.L(ctx).Tracef("Resolved value '%v' for field '%s'", v, fieldName)
	if !check(v) {
		return i18n.NewError(ctx, i18n.MsgInvalidValue, v, fieldName)
	}
	return nil
}

// validate checks all the field values, once combined with defaults.
// Optionally it stores the defaults back on the structure, to ensure no nil fields.
// - When using at runtime: true, so later code doesn't need to worry about nil checks / defaults
// - When storing to the DB: false, so defaults can be applied dynamically
func (esm *esManager[CT, DT]) validateStream(ctx context.Context, spec CT, phase LifecyclePhase) (factory DispatcherFactory[CT, DT], err error) {
	esc := spec.ESFields()
	if esc.Name == nil {
		return nil, i18n.NewError(ctx, i18n.MsgMissingRequiredField, "name")
	}
	if esc.TopicFilter != nil {
		fullMatchFilter := `^` + *esc.TopicFilter + `$`
		if esc.topicFilterRegexp, err = regexp.Compile(fullMatchFilter); err != nil {
			return nil, i18n.NewError(ctx, i18n.MsgESInvalidTopicFilterRegexp, fullMatchFilter, err)
		}
	}
	defaults := esm.config.Defaults
	if err := esm.runtime.Validate(ctx, spec); err != nil {
		return nil, err
	}
	err = fftypes.ValidateFFNameField(ctx, *esc.Name, "name")
	setDefaults := phase == LifecyclePhaseStarting
	if err == nil {
		err = checkSet(ctx, setDefaults, "status", &esc.Status, EventStreamStatusStarted, func(v fftypes.FFEnum) bool { return fftypes.FFEnumValid(ctx, "esstatus", v) })
	}
	if err == nil {
		err = checkSet(ctx, setDefaults, "batchSize", &esc.BatchSize, defaults.BatchSize, func(v int) bool { return v > 0 })
	}
	if err == nil {
		err = checkSet(ctx, setDefaults, "batchTimeout", &esc.BatchTimeout, defaults.BatchTimeout, func(v fftypes.FFDuration) bool { return v > 0 })
	}
	if err == nil {
		err = checkSet(ctx, setDefaults, "retryTimeout", &esc.RetryTimeout, defaults.RetryTimeout, func(v fftypes.FFDuration) bool { return v > 0 })
	}
	if err == nil {
		err = checkSet(ctx, setDefaults, "blockedRetryDelay", &esc.BlockedRetryDelay, defaults.BlockedRetryDelay, func(v fftypes.FFDuration) bool { return v > 0 })
	}
	if err == nil {
		err = checkSet(ctx, setDefaults, "errorHandling", &esc.ErrorHandling, defaults.ErrorHandling, func(v fftypes.FFEnum) bool { return fftypes.FFEnumValid(ctx, "ehtype", v) })
	}
	esType := spec.ESType()
	if err == nil {
		if !fftypes.FFEnumValid(ctx, "estype", esType) {
			err = i18n.NewError(ctx, i18n.MsgInvalidValue, esType, "type")
		}
	}
	if err != nil {
		return nil, err
	}
	factory = esm.dispatchers[esType]
	if factory == nil {
		return nil, i18n.NewError(ctx, i18n.MsgESInvalidType, esType)
	}
	err = factory.Validate(ctx, &esm.config, spec, esm.tlsConfigs, phase)
	if err != nil {
		return nil, err
	}
	return factory, nil
}

type eventStream[CT EventStreamSpec, DT any] struct {
	bgCtx       context.Context
	esm         *esManager[CT, DT]
	spec        CT
	mux         sync.Mutex
	action      Dispatcher[DT]
	activeState *activeStream[CT, DT]
	retry       *retry.Retry
	persistence Persistence[CT]
	stopping    chan struct{}
}

type EventStreamActions[CT any] interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Status(ctx context.Context) CT
}

func (esm *esManager[CT, DT]) initEventStream(
	spec CT,
) (es *eventStream[CT, DT], err error) {
	// Validate
	factory, err := esm.validateStream(esm.bgCtx, spec, LifecyclePhaseStarting)
	if err != nil {
		return nil, err
	}

	streamCtx := log.WithLogFields(esm.bgCtx, "eventstream", *spec.ESFields().Name)
	es = &eventStream[CT, DT]{
		bgCtx:       streamCtx,
		esm:         esm,
		spec:        spec,
		persistence: esm.persistence,
		retry:       esm.config.Retry,
	}

	es.action = factory.NewDispatcher(streamCtx, &esm.config, spec)

	log.L(es.bgCtx).Infof("Initialized Event Stream")
	if *spec.ESFields().Status == EventStreamStatusStarted {
		// Start up the stream
		es.ensureActive()
	}
	return es, nil
}

func (es *eventStream[CT, DT]) requestStop(ctx context.Context) chan struct{} {
	es.mux.Lock()
	defer es.mux.Unlock()
	// Check we're active
	activeState := es.activeState
	if activeState == nil {
		return nil
	}
	// Check if we're already stopping
	if es.stopping != nil {
		return es.stopping
	}
	persistedStatus := *es.spec.ESFields().Status
	// Cancel the active context, and create the stopping task
	activeState.cancelCtx()
	closedWhenStopped := make(chan struct{})
	es.stopping = closedWhenStopped
	go func() {
		<-activeState.eventLoopDone
		<-activeState.batchLoopDone

		// Complete an in-process delete
		if persistedStatus == EventStreamStatusDeleted {
			if err := es.persistStatus(ctx, persistedStatus); err != nil {
				log.L(ctx).Errorf("Failed to delete: %s", err)
			}
		}

		es.mux.Lock()
		defer es.mux.Unlock()
		es.activeState = nil
		es.stopping = nil
		close(closedWhenStopped)
	}()
	return closedWhenStopped
}

func (es *eventStream[CT, DT]) checkSetStatus(ctx context.Context, targetStatus *EventStreamStatus) (newRuntimeStatus EventStreamStatus, changeToPersist *EventStreamStatus, statistics *EventStreamStatistics, err error) {
	es.mux.Lock()
	defer es.mux.Unlock()

	transition := func(runtime, persited EventStreamStatus) {
		newRuntimeStatus = runtime
		es.spec.ESFields().Status = &persited
		changeToPersist = &persited
	}

	// We snap a pointer to any stats under the lock here
	if es.activeState != nil {
		statistics = &es.activeState.EventStreamStatistics
	}

	// Check valid state transitions based on the persisted status, and whether we are stopping
	switch *es.spec.ESFields().Status {
	case EventStreamStatusDeleted:
		newRuntimeStatus = EventStreamStatusDeleted
		if es.stopping != nil {
			newRuntimeStatus = EventStreamStatusStoppingDeleted
		}
		// No state changes possible from here
		if targetStatus != nil && *targetStatus != EventStreamStatusDeleted {
			err = i18n.NewError(ctx, i18n.MsgESDeleting)
		}
	case EventStreamStatusStopped:
		newRuntimeStatus = EventStreamStatusStopped
		if es.stopping != nil {
			newRuntimeStatus = EventStreamStatusStopping
		}
		// We can only stay in stopped, or go to deleted
		if targetStatus != nil {
			switch {
			case *targetStatus == EventStreamStatusStopped:
				// no change
			case *targetStatus == EventStreamStatusStarted && es.stopping == nil:
				transition(EventStreamStatusStarted, EventStreamStatusStarted) // note no starting interim runtime status
			case *targetStatus == EventStreamStatusDeleted:
				transition(EventStreamStatusStoppingDeleted, EventStreamStatusDeleted)
			default:
				err = i18n.NewError(ctx, i18n.MsgESStopping)
			}
		}
	case EventStreamStatusStarted:
		newRuntimeStatus = EventStreamStatusStarted
		// We can go anywhere
		if targetStatus != nil {
			switch *targetStatus {
			case EventStreamStatusStarted:
				// no change
			case EventStreamStatusStopped:
				transition(EventStreamStatusStopping, EventStreamStatusStopped)
			case EventStreamStatusDeleted:
				transition(EventStreamStatusStoppingDeleted, EventStreamStatusDeleted)
			}
		}
	default:
		err = i18n.NewError(ctx, i18n.MsgESInvalidPersistedStatus)
	}
	log.L(ctx).Infof("Status: %s (change=%v)", newRuntimeStatus, changeToPersist)
	return newRuntimeStatus, changeToPersist, statistics, err
}

func (es *eventStream[CT, DT]) persistStatus(ctx context.Context, targetStatus EventStreamStatus) error {
	fb := GenericEventStreamFilters.NewUpdate(ctx)
	return es.esm.persistence.EventStreams().Update(ctx, es.spec.GetID(), fb.Set("status", targetStatus))
}

func (es *eventStream[CT, DT]) stopOrDelete(ctx context.Context, targetStatus EventStreamStatus) error {
	_, newPersistedStatus, _, err := es.checkSetStatus(ctx, &targetStatus)
	if err != nil {
		return err
	}
	if newPersistedStatus != nil {
		if err := es.persistStatus(ctx, *newPersistedStatus); err != nil {
			return err
		}
	}
	return es.suspend(ctx)
}

func (es *eventStream[CT, DT]) suspend(ctx context.Context) error {
	// initiate a stop, if we're started
	stopping := es.requestStop(ctx)
	if stopping != nil {
		// wait for the stop to complete
		select {
		case <-stopping:
		case <-ctx.Done():
			return i18n.NewError(ctx, i18n.MsgESContextCancelledWaitingStop)
		}
	}
	return nil
}

func (es *eventStream[CT, DT]) ensureActive() {
	// Caller responsible for checking state transitions before invoking
	es.mux.Lock()
	defer es.mux.Unlock()
	if es.stopping == nil && es.activeState == nil {
		es.activeState = es.newActiveStream()
	}
}

func (es *eventStream[CT, DT]) stop(ctx context.Context) error {
	return es.stopOrDelete(ctx, EventStreamStatusStopped)
}

func (es *eventStream[CT, DT]) delete(ctx context.Context) error {
	return es.stopOrDelete(ctx, EventStreamStatusDeleted)
}

func (es *eventStream[CT, DT]) start(ctx context.Context) error {
	startedStatus := EventStreamStatusStarted
	_, newPersistedStatus, _, err := es.checkSetStatus(ctx, &startedStatus)
	if err != nil {
		return err
	}
	if newPersistedStatus != nil {
		if err := es.persistStatus(ctx, *newPersistedStatus); err != nil {
			return err
		}
	}
	es.ensureActive()
	return nil
}

func (es *eventStream[CT, DT]) WithStatus(ctx context.Context) CT {
	status, _, statistics, _ := es.checkSetStatus(ctx, nil)
	return es.esm.runtime.WithRuntimeStatus(es.spec, status, statistics)
}
