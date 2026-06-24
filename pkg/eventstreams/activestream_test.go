// Copyright © 2023 Kaleido, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCheckpointContextClose(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t, func(mdb *mockPersistence) {
		mdb.checkpoints.On("GetByID", mock.Anything, mock.Anything).Return((*EventStreamCheckpoint)(nil), fmt.Errorf("pop"))
	})
	defer done()

	as := &activeStream[*GenericEventStream, testData]{
		eventStream: es,
	}
	as.ctx, as.cancelCtx = context.WithCancel(ctx)

	as.cancelCtx()
	_, err := as.loadCheckpoint()
	assert.Regexp(t, "FF00154", err)
}

func TestRunSourceLoopDone(t *testing.T) {
	ctx, es, mes, done := newTestEventStream(t)
	defer done()

	as := &activeStream[*GenericEventStream, testData]{
		eventStream: es,
	}
	as.ctx, as.cancelCtx = context.WithCancel(ctx)

	mes.run = func(ctx context.Context, es *GenericEventStream, checkpointSequenceId string, deliver Deliver[testData]) error {
		deliver(nil)
		return nil
	}

	as.cancelCtx()
	err := as.runSourceLoop("")
	assert.NoError(t, err)
}

func TestRunSourceEventsBlockedExit(t *testing.T) {
	ctx, es, mes, done := newTestEventStream(t)
	defer done()

	as := &activeStream[*GenericEventStream, testData]{
		eventStream: es,
	}
	as.ctx, as.cancelCtx = context.WithCancel(ctx)

	as.events = make(chan *Event[testData])
	mes.run = func(ctx context.Context, es *GenericEventStream, checkpointSequenceId string, deliver Deliver[testData]) error {
		deliver([]*Event[testData]{{ /* will block */ }})
		return nil
	}

	as.cancelCtx()
	err := as.runSourceLoop("")
	assert.NoError(t, err)
}

func TestBatchTimeout(t *testing.T) {
	_, es, mes, done := newTestEventStream(t, func(mdb *mockPersistence) {
		mdb.checkpoints.On("GetByID", mock.Anything, mock.Anything).Return((*EventStreamCheckpoint)(nil), nil)
		mdb.checkpoints.On("Upsert", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
	})
	defer done()

	es.spec.BatchTimeout = ptrTo(fftypes.FFDuration(1 * time.Millisecond))

	delivered := false
	mes.run = func(ctx context.Context, es *GenericEventStream, checkpointSequenceId string, deliver Deliver[testData]) error {
		if delivered {
			<-ctx.Done()
		} else {
			deliver([]*Event[testData]{{
				EventCommon: EventCommon{
					Topic:      "topic1",
					SequenceID: "111111",
				},
				Data: &testData{Field1: 11111},
			}})
			delivered = true
		}
		return nil
	}

	dispatched := make(chan struct{})
	es.action = &mockAction{
		attemptDispatch: func(ctx context.Context, attempt int, events *EventBatch[testData]) error {
			assert.Len(t, events.Events, 1)
			close(dispatched)
			<-ctx.Done()
			return nil
		},
	}

	as := es.newActiveStream()
	<-dispatched

	as.cancelCtx()
	<-as.eventLoopDone
	<-as.batchLoopDone
}

func TestQueuedCheckpointAsync(t *testing.T) {
	checkpointed := make(chan bool)
	ctx, es, _, done := newTestEventStream(t, func(mdb *mockPersistence) {
		mdb.checkpoints.On("Upsert", mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Run(func(args mock.Arguments) {
			checkpointed <- true
		})
	})
	defer done()

	as := &activeStream[*GenericEventStream, testData]{
		eventStream: es,
	}
	as.ctx, as.cancelCtx = context.WithCancel(ctx)

	as.esm.config.Checkpoints.Asynchronous = true
	as.HighestDetected = "11111"
	as.dispatchCheckpoint()
	as.HighestDetected = "22222"
	as.pushCheckpoint()

	<-checkpointed
	<-checkpointed
}

func TestQueuedCheckpointCancel(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t, func(mdb *mockPersistence) {
		mdb.checkpoints.On("Upsert", mock.Anything, mock.Anything, mock.Anything).Return(false, fmt.Errorf("pop"))
	})
	defer done()

	as := &activeStream[*GenericEventStream, testData]{
		eventStream: es,
	}
	as.ctx, as.cancelCtx = context.WithCancel(ctx)
	as.cancelCtx()

	as.HighestDetected = "11111"
	as.pushCheckpoint()
	as.checkpointRoutine()

}

func TestDispatchSkipError(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t)
	defer done()

	as := &activeStream[*GenericEventStream, testData]{
		eventStream: es,
	}
	as.ctx, as.cancelCtx = context.WithCancel(ctx)

	as.spec.RetryTimeout = ptrTo(fftypes.FFDuration(1 * time.Microsecond))
	as.spec.ErrorHandling = ptrTo(ErrorHandlingTypeSkip)
	as.action = &mockAction{
		attemptDispatch: func(ctx context.Context, attempt int, events *EventBatch[testData]) error {
			return fmt.Errorf("pop")
		},
	}

	err := as.dispatchBatch(&eventStreamBatch[testData]{
		events: []*Event[testData]{{}},
	})
	assert.NoError(t, err)

}

func TestDispatchBlockError(t *testing.T) {
	ctx, es, _, done := newTestEventStream(t)
	defer done()

	as := &activeStream[*GenericEventStream, testData]{
		eventStream:   es,
		events:        make(chan *Event[testData]),
		batchLoopDone: make(chan struct{}),
	}
	as.ctx, as.cancelCtx = context.WithCancel(ctx)

	as.spec.BatchSize = ptrTo(1)
	as.spec.RetryTimeout = ptrTo(fftypes.FFDuration(1 * time.Microsecond))
	as.spec.BlockedRetryDelay = ptrTo(fftypes.FFDuration(10 * time.Millisecond))
	as.spec.ErrorHandling = ptrTo(ErrorHandlingTypeBlock)
	callCount := 0
	calls := make(chan bool)
	as.action = &mockAction{
		attemptDispatch: func(ctx context.Context, attempt int, events *EventBatch[testData]) error {
			calls <- true
			callCount++
			if callCount > 1 {
				as.cancelCtx()
			}
			return fmt.Errorf("pop")
		},
	}

	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		as.runBatchLoop()
	}()

	as.events <- &Event[testData]{}

	<-calls
	<-calls
	<-dispatchDone

}
