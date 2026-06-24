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
	"sync"
	"time"

	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
)

type eventStreamBatch[DataType any] struct {
	events     []*Event[DataType]
	batchTimer *time.Timer
}

type activeStream[CT EventStreamSpec, DT any] struct {
	*eventStream[CT, DT]
	ctx           context.Context
	cancelCtx     func()
	filterSkipped int64
	EventStreamStatistics
	eventLoopDone chan struct{}
	batchLoopDone chan struct{}
	events        chan *Event[DT]

	checkpointLock       sync.Mutex
	dispatchedCheckpoint string
	queuedCheckpoint     string
}

func (es *eventStream[CT, DT]) newActiveStream() *activeStream[CT, DT] {
	ctx, cancelCtx := context.WithCancel(es.bgCtx)
	as := &activeStream[CT, DT]{
		eventStream: es,
		ctx:         ctx,
		cancelCtx:   cancelCtx,
		EventStreamStatistics: EventStreamStatistics{
			StartTime: fftypes.Now(),
		},
		eventLoopDone: make(chan struct{}),
		batchLoopDone: make(chan struct{}),
		events:        make(chan *Event[DT], *es.spec.ESFields().BatchSize),
	}
	go as.runEventLoop()
	go as.runBatchLoop()
	return as
}

func (as *activeStream[CT, DT]) runEventLoop() {
	defer close(as.eventLoopDone)

	// Read the last checkpoint for this stream
	checkpointSequenceID, err := as.loadCheckpoint()
	if err == nil {
		// Run the inner source read loop until it exits
		err = as.retry.Do(as.ctx, "source run loop", func(_ int) (retry bool, err error) {
			if err = as.runSourceLoop(checkpointSequenceID); err != nil {
				log.L(as.ctx).Errorf("source loop error: %s", err)
				return true, err
			}
			// the Run loop must only exit with nil error if the context is closed
			// (which we also signal with an Exit instruction)
			return false, nil
		})

	}
	// Retry will only return an error if the context is cancelled
	log.L(as.ctx).Debugf("event loop exiting (%v)", err)
}

func (as *activeStream[CT, DT]) loadCheckpoint() (sequencedID string, err error) {
	err = as.retry.Do(as.ctx, "load checkpoint", func(_ int) (retry bool, err error) {
		log.L(as.ctx).Debugf("Loading checkpoint: %s", as.spec.GetID())
		cp, err := as.persistence.Checkpoints().GetByID(as.ctx, as.spec.GetID())
		if err != nil {
			return true, err
		}
		if cp != nil && cp.SequenceID != nil {
			sequencedID = *cp.SequenceID
		} else if as.spec.ESFields().InitialSequenceID != nil {
			sequencedID = *as.spec.ESFields().InitialSequenceID
		}
		return true, err
	})
	return sequencedID, err
}

func (as *activeStream[CT, DT]) checkFilter(event *Event[DT]) bool {
	esSpec := as.spec.ESFields()
	if esSpec.topicFilterRegexp != nil {
		return esSpec.topicFilterRegexp.Match([]byte(event.Topic))
	}
	return true
}

func (as *activeStream[CT, DT]) runSourceLoop(initialCheckpointSequenceID string) error {
	// Responsibility of the source to block until events are available, or the context is closed.
	log.L(as.ctx).Infof("Initiating source with checkpoint: %s", initialCheckpointSequenceID)
	return as.esm.runtime.Run(as.ctx, as.spec, initialCheckpointSequenceID, func(events []*Event[DT]) SourceInstruction {
		log.L(as.ctx).Debugf("Received batch of %d events from source", len(events))

		// There's no direct connection between any batching used in the source routine,
		// and our batch based delivery. This is intentional - allowing separate optimization
		// of each routine for the source data store/stream.
		for _, event := range events {
			if event != nil {
				select {
				case as.events <- event:
				case <-as.ctx.Done():
					// Event stream has has shut down
					return Exit
				}
			}
		}

		// Explicitly check for done here, as the above doesn't assure we'd trigger
		select {
		case <-as.ctx.Done():
			return Exit
		default:
		}

		// Instruct the run loop to continue
		return Continue
	})

}

func (as *activeStream[CT, DT]) runBatchLoop() {
	defer close(as.batchLoopDone)

	esSpec := as.spec.ESFields()
	var batch *eventStreamBatch[DT]
	batchTimeout := time.Duration(*esSpec.BatchTimeout)
	var noBatchActive <-chan time.Time = make(chan time.Time) // never pops
	batchTimedOut := noBatchActive
	for {

		// Pull events out of the event loop, and assemble them into batches with a max + timeout
		var timedOut = false
		select {
		case <-as.ctx.Done():
			log.L(as.ctx).Debugf("batch loop done")
			return
		case <-batchTimedOut:
			timedOut = true
		case event := <-as.events:
			as.HighestDetected = event.SequenceID
			if !as.checkFilter(event) {
				as.filterSkipped++
			} else {
				if batch == nil {
					batch = &eventStreamBatch[DT]{
						batchTimer: time.NewTimer(batchTimeout),
						events:     make([]*Event[DT], 0, *esSpec.BatchSize),
					}
					batchTimedOut = batch.batchTimer.C
				}
				batch.events = append(batch.events, event)
			}
		}
		batchDispatched := false
		if batch != nil && (len(batch.events) >= *esSpec.BatchSize || timedOut) {
			// attempt dispatch (only returns err on exit)
			if err := as.dispatchBatch(batch); err != nil {
				log.L(as.ctx).Debugf("batch loop done: %s", err)
				return
			}
			batchDispatched = true
			// reset batch
			batch.batchTimer.Stop()
			batchTimedOut = noBatchActive
			batch = nil
		}
		if batchDispatched || as.filterSkipped > as.esm.config.Checkpoints.UnmatchedEventThreshold {
			// At this point we are sure that the highest detected event, is above the highest
			// acknowledged event.
			as.dispatchCheckpoint()
			// Reset our skip tracker
			as.filterSkipped = 0
		}

	}
}

func (as *activeStream[CT, DT]) dispatchCheckpoint() {
	if as.pushCheckpoint() {
		if as.esm.config.Checkpoints.Asynchronous {
			go as.checkpointRoutine() // async
		} else {
			as.checkpointRoutine() // in-line
		}
	}
}

func (as *activeStream[CT, DT]) pushCheckpoint() bool {
	as.checkpointLock.Lock()
	defer as.checkpointLock.Unlock()
	if as.dispatchedCheckpoint == "" {
		as.dispatchedCheckpoint = as.HighestDetected
		return true // we need to run the checkpoint worker
	}
	as.queuedCheckpoint = as.HighestDetected
	return false // it'll be picked up before the existing worker ends
}

func (as *activeStream[CT, DT]) popCheckpoint() string {
	as.checkpointLock.Lock()
	defer as.checkpointLock.Unlock()
	checkpointSequenceID := as.dispatchedCheckpoint
	as.dispatchedCheckpoint = as.queuedCheckpoint
	as.queuedCheckpoint = ""
	return checkpointSequenceID
}

func (as *activeStream[CT, DT]) checkpointRoutine() {
	for {
		checkpointSequenceID := as.popCheckpoint()
		if checkpointSequenceID == "" {
			return // We're done
		}
		err := as.retry.Do(as.ctx, "checkpoint", func(_ int) (retry bool, err error) {
			log.L(as.bgCtx).Debugf("Writing checkpoint id=%s sequenceID=%s", as.spec.GetID(), checkpointSequenceID)
			_, err = as.esm.persistence.Checkpoints().Upsert(as.ctx, &EventStreamCheckpoint{
				ID:         ptrTo(as.spec.GetID()), // the ID of the stream is the ID of the checkpoint
				SequenceID: &checkpointSequenceID,
			}, dbsql.UpsertOptimizationExisting)
			return true, err
		})
		if err != nil {
			// must be cancelled context
			log.L(as.ctx).Warnf("checkpoint cancelled: %s", err)
			return
		}
		// lazy write of stored checkpoint back to stats
		as.Checkpoint = checkpointSequenceID
	}
}

// performActionWithRetry performs an action, with exponential back-off retry up
// to a given threshold. Only returns error in the case that the context is closed.
func (as *activeStream[CT, DT]) dispatchBatch(batch *eventStreamBatch[DT]) (err error) {
	as.LastDispatchTime = fftypes.Now()
	as.LastDispatchFailure = ""
	as.LastDispatchAttempts = 0
	as.LastDispatchStatus = DispatchStatusDispatching
	as.HighestDispatched = batch.events[len(batch.events)-1].SequenceID
	esSpec := as.spec.ESFields()
	for {
		// Short exponential back-off retry
		err := as.retry.Do(as.ctx, "action", func(_ int) (retry bool, err error) {
			log.L(as.ctx).Debugf("Batch attempt %d dispatching. Len=%d", as.LastDispatchAttempts, len(batch.events))
			err = as.action.AttemptDispatch(as.ctx, as.LastDispatchAttempts, &EventBatch[DT]{
				Type:   MessageTypeEventBatch,
				Events: batch.events,
			})
			if err != nil {
				log.L(as.ctx).Errorf("Batch attempt %d failed. err=%s", as.LastDispatchAttempts, err)
				as.LastDispatchAttempts++
				as.LastDispatchFailure = err.Error()
				as.LastDispatchStatus = DispatchStatusRetrying
				return time.Since(*as.LastDispatchTime.Time()) < time.Duration(*esSpec.RetryTimeout), err
			}
			as.LastDispatchStatus = DispatchStatusComplete
			return false, nil
		})
		if err == nil {
			return nil
		}
		// We're in blocked retry delay
		as.LastDispatchStatus = DispatchStatusBlocked
		log.L(as.ctx).Errorf("Batch failed short retry after %.2fs secs. ErrorHandling=%s BlockedRetryDelay=%.2fs ",
			time.Since(*as.LastDispatchTime.Time()).Seconds(), *esSpec.ErrorHandling, time.Duration(*esSpec.BlockedRetryDelay).Seconds())
		if *esSpec.ErrorHandling == ErrorHandlingTypeSkip {
			// Swallow the error now we have logged it
			as.LastDispatchStatus = DispatchStatusSkipped
			return nil
		}
		select {
		case <-time.After(time.Duration(*esSpec.BlockedRetryDelay)):
		case <-as.ctx.Done():
			// Only way we exit with error, is if the context is cancelled
			return i18n.NewError(as.ctx, i18n.MsgContextCanceled)
		}
	}
}
