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

	"github.com/hyperledger-firefly/common/mocks/crudmocks"
	"github.com/hyperledger-firefly/common/pkg/config"
	"github.com/hyperledger-firefly/common/pkg/dbsql"
	"github.com/hyperledger-firefly/common/pkg/ffapi"
	"github.com/hyperledger-firefly/common/pkg/fftls"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/retry"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockEventSource struct {
	validate func(ctx context.Context, conf *GenericEventStream) error
	run      func(ctx context.Context, es *GenericEventStream, checkpointSequenceId string, deliver Deliver[testData]) error
}

func (mes *mockEventSource) NewID() string {
	return fftypes.NewUUID().String()
}

func (mes *mockEventSource) Run(ctx context.Context, es *GenericEventStream, checkpointSequenceId string, deliver Deliver[testData]) error {
	return mes.run(ctx, es, checkpointSequenceId, deliver)
}

func (mes *mockEventSource) Validate(ctx context.Context, conf *GenericEventStream) error {
	return mes.validate(ctx, conf)
}

func (mes *mockEventSource) WithRuntimeStatus(spec *GenericEventStream, status EventStreamStatus, stats *EventStreamStatistics) *GenericEventStream {
	return spec.WithRuntimeStatus(status, stats)
}

type mockAction struct {
	attemptDispatch func(ctx context.Context, attempt int, events *EventBatch[testData]) error
}

func (ma *mockAction) AttemptDispatch(ctx context.Context, attempt int, events *EventBatch[testData]) error {
	return ma.attemptDispatch(ctx, attempt, events)
}

type mockPersistence struct {
	eventStreams *crudmocks.CRUD[*GenericEventStream]
	checkpoints  *crudmocks.CRUD[*EventStreamCheckpoint]
}

func (mp *mockPersistence) EventStreams() dbsql.CRUD[*GenericEventStream] {
	return mp.eventStreams
}
func (mp *mockPersistence) Checkpoints() dbsql.CRUD[*EventStreamCheckpoint] {
	return mp.checkpoints
}
func (mp *mockPersistence) IDValidator() IDValidator {
	return dbsql.UUIDValidator
}
func (mp *mockPersistence) Close() {}

func newMockESManager(t *testing.T, extraSetup ...func(mp *mockPersistence)) (context.Context, *esManager[*GenericEventStream, testData], *mockEventSource, func()) {
	logrus.SetLevel(logrus.DebugLevel)

	mp := &mockPersistence{
		eventStreams: crudmocks.NewCRUD[*GenericEventStream](t),
		checkpoints:  crudmocks.NewCRUD[*EventStreamCheckpoint](t),
	}
	mp.eventStreams.On("GetQueryFactory").Return(GenericEventStreamFilters).Maybe()

	ctx, cancelCtx := context.WithCancel(context.Background())
	config.RootConfigReset()
	conf := config.RootSection("ut")
	dbConf := conf.SubSection("db")
	esConf := conf.SubSection("eventstreams")
	dbsql.InitSQLiteConfig(dbConf)
	InitConfig(esConf)

	CheckpointsConfig.Set(ConfigCheckpointsAsynchronous, false)
	for _, fn := range extraSetup {
		fn(mp)
	}

	mes := &mockEventSource{
		validate: func(ctx context.Context, conf *GenericEventStream) error { return nil },
		run: func(ctx context.Context, es *GenericEventStream, checkpointSequenceId string, deliver Deliver[testData]) error {
			<-ctx.Done()
			return nil
		},
	}
	mgr, err := NewEventStreamManager[*GenericEventStream, testData](ctx, GenerateConfig[*GenericEventStream, testData](ctx), mp, nil, mes)
	assert.NoError(t, err)

	return ctx, mgr.(*esManager[*GenericEventStream, testData]), mes, func() {
		mgr.Close(ctx)
		cancelCtx()
	}
}

func TestNewManagerFailBadTLS(t *testing.T) {
	_, err := NewEventStreamManager[*GenericEventStream, testData](context.Background(), &Config[*GenericEventStream, testData]{
		Retry: &retry.Retry{},
		TLSConfigs: map[string]*fftls.Config{
			"tls0": {
				Enabled: true,
				CAFile:  t.TempDir(),
			},
		},
	}, nil, nil, &mockEventSource{})
	assert.Regexp(t, "FF00153", err)

}

func TestNewManagerBadConfState(t *testing.T) {
	_, err := NewEventStreamManager[*GenericEventStream, testData](context.Background(), &Config[*GenericEventStream, testData]{}, nil, nil, &mockEventSource{})
	assert.Regexp(t, "FF00237", err)
}

func TestInitFail(t *testing.T) {
	mp := &mockPersistence{
		eventStreams: crudmocks.NewCRUD[*GenericEventStream](t),
		checkpoints:  crudmocks.NewCRUD[*EventStreamCheckpoint](t),
	}
	mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, fmt.Errorf("pop"))
	ctx := context.Background()
	InitConfig(config.RootSection("ut"))
	_, err := NewEventStreamManager[*GenericEventStream, testData](ctx, GenerateConfig[*GenericEventStream, testData](ctx), mp, nil, nil)
	assert.Regexp(t, "pop", err)
}

func TestInitWithStreams(t *testing.T) {
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStarted),
		},
	}
	_, _, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.checkpoints.On("GetByID", mock.Anything, es.GetID()).Return((*EventStreamCheckpoint)(nil), nil)
	})
	defer done()

}

func TestInitWithStreamsCleanupFail(t *testing.T) {
	mp := &mockPersistence{
		eventStreams: crudmocks.NewCRUD[*GenericEventStream](t),
		checkpoints:  crudmocks.NewCRUD[*EventStreamCheckpoint](t),
	}
	ctx := context.Background()
	InitConfig(config.RootSection("ut"))
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusDeleted),
		},
	}
	mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{es}, &ffapi.FilterResult{}, nil).Once()
	mp.eventStreams.On("Delete", mock.Anything, es.GetID()).Return(fmt.Errorf("pop"))
	_, err := NewEventStreamManager[*GenericEventStream, testData](ctx, GenerateConfig[*GenericEventStream, testData](ctx), mp, nil, nil)
	assert.Regexp(t, "pop", err)
}

func TestInitWithStreamsInitFail(t *testing.T) {
	mp := &mockPersistence{
		eventStreams: crudmocks.NewCRUD[*GenericEventStream](t),
		checkpoints:  crudmocks.NewCRUD[*EventStreamCheckpoint](t),
	}
	ctx := context.Background()
	InitConfig(config.RootSection("ut"))
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStarted),
		},
	}
	mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{es}, &ffapi.FilterResult{}, nil).Once()
	_, err := NewEventStreamManager[*GenericEventStream, testData](ctx, GenerateConfig[*GenericEventStream, testData](ctx), mp, nil, &mockEventSource{
		validate: func(ctx context.Context, conf *GenericEventStream) error {
			return fmt.Errorf("pop")
		},
	})
	assert.Regexp(t, "pop", err)
}

func TestUpsertStreamByNameDeleted(t *testing.T) {
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetByName", mock.Anything, "stream1").Return(es, nil)
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
	})
	defer done()

	esm.getStream(es.GetID()).spec.Status = ptrTo(EventStreamStatusDeleted)
	_, err := esm.UpsertStream(ctx, "stream1", es)
	assert.Regexp(t, "FF00236", err)

}

func TestUpsertStreamByNameFailLookup(t *testing.T) {
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetByName", mock.Anything, "stream1").Return((*GenericEventStream)(nil), fmt.Errorf("pop"))
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
	})
	defer done()

	esm.getStream(es.GetID()).spec.Status = ptrTo(EventStreamStatusDeleted)
	_, err := esm.UpsertStream(ctx, "stream1", es)
	assert.Regexp(t, "pop", err)

}

func TestUpsertStreamByIDDeleted(t *testing.T) {
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
	})
	defer done()

	esm.getStream(es.GetID()).spec.Status = ptrTo(EventStreamStatusDeleted)
	_, err := esm.UpsertStream(ctx, es.GetID(), es)
	assert.Regexp(t, "FF00236", err)

}

func TestUpsertStreamBadUpdate(t *testing.T) {
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
	})
	defer done()

	newES := *es
	newES.Name = nil
	_, err := esm.UpsertStream(ctx, "", &newES)
	assert.Regexp(t, "FF00112", err)

}

func TestUpsertStreamUpsertFail(t *testing.T) {
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("Upsert", mock.Anything, mock.Anything, dbsql.UpsertOptimizationExisting).Return(false, fmt.Errorf("pop")).Once()
	})
	defer done()

	_, err := esm.UpsertStream(ctx, "", es)
	assert.Regexp(t, "pop", err)

}

func TestUpsertReInitExistingFailTimeout(t *testing.T) {
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
	})
	done()

	existing := &eventStream[*GenericEventStream, testData]{
		activeState: &activeStream[*GenericEventStream, testData]{},
		stopping:    make(chan struct{}),
	}
	err := esm.reInit(ctx, es, existing)
	assert.Regexp(t, "FF00229", err)

}

func TestUpsertReInitExistingFailInit(t *testing.T) {
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(fftypes.FFEnum("wrong")),
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
	})
	done()

	err := esm.reInit(ctx, es, nil)
	assert.Regexp(t, "FF00234", err)

}

func TestDeleteStreamNotKnown(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(&GenericEventStream{
			ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()}, // exists in DB, but does not exist in runtime state
		}, nil).Once()
	})
	defer done()

	err := esm.DeleteStream(ctx, fftypes.NewUUID().String())
	assert.Regexp(t, "FF00164", err)

}

func TestDeleteStreamNotFound(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return((*GenericEventStream)(nil), fmt.Errorf("not found")).Once()
	})
	defer done()

	err := esm.DeleteStream(ctx, fftypes.NewUUID().String())
	assert.Regexp(t, "not found", err)

}

func TestResetStreamNotKnown(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(&GenericEventStream{
			ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()}, // exists in DB, but does not exist in runtime state
		}, nil).Once()
	})
	defer done()

	err := esm.ResetStream(ctx, fftypes.NewUUID().String(), nil)
	assert.Regexp(t, "FF00164", err)

}

func TestStopStreamNotKnown(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(&GenericEventStream{
			ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()}, // exists in DB, but does not exist in runtime state
		}, nil).Once()
	})
	defer done()

	err := esm.StopStream(ctx, fftypes.NewUUID().String())
	assert.Regexp(t, "FF00164", err)

}

func TestStartStreamNotKnown(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(&GenericEventStream{
			ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()}, // exists in DB, but does not exist in runtime state
		}, nil).Once()
	})
	defer done()

	err := esm.StartStream(ctx, fftypes.NewUUID().String())
	assert.Regexp(t, "FF00164", err)

}

func TestEnrichStreamNotKnown(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
	})
	defer done()

	es := esm.enrichGetStream(ctx, &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
	})
	assert.NotNil(t, es)
	assert.Equal(t, EventStreamStatusUnknown, *es.Status)

}

func TestDeleteStreamFail(t *testing.T) {
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(es, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("pop")).Once()
	})
	defer done()

	err := esm.DeleteStream(ctx, es.GetID())
	assert.Regexp(t, "pop", err)

}

func TestDeleteStreamFailDelete(t *testing.T) {
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(es, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		mp.eventStreams.On("Delete", mock.Anything, mock.Anything).Return(fmt.Errorf("pop")).Once()
	})
	defer done()

	err := esm.DeleteStream(ctx, es.GetID())
	assert.Regexp(t, "pop", err)

}

func TestDeleteStreamByName(t *testing.T) {
	es := &GenericEventStream{
		ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
		EventStreamSpecFields: EventStreamSpecFields{
			Name:   ptrTo("stream1"),
			Status: ptrTo(EventStreamStatusStopped),
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(es, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{es}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		// Expect the ID to be passed to delete
		mp.eventStreams.On("Delete", mock.Anything, es.GetID()).Return(nil).Once()
	})
	defer done()

	err := esm.DeleteStream(ctx, *es.Name)
	assert.NoError(t, err)
}

func TestResetStreamStopFailTimeout(t *testing.T) {
	existing := &eventStream[*GenericEventStream, testData]{
		activeState: &activeStream[*GenericEventStream, testData]{},
		stopping:    make(chan struct{}),
		spec: &GenericEventStream{
			ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
			EventStreamSpecFields: EventStreamSpecFields{
				Name:   ptrTo("stream1"),
				Status: ptrTo(EventStreamStatusStopped),
			},
		},
	}

	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(existing.spec, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
	})
	done()
	existing.esm = esm

	esm.addStream(ctx, existing)
	err := esm.ResetStream(ctx, existing.spec.GetID(), nil)
	assert.Regexp(t, "FF00229", err)

}

func TestResetStreamStopFailDeleteCheckpoint(t *testing.T) {
	existing := &eventStream[*GenericEventStream, testData]{
		spec: &GenericEventStream{
			ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
			EventStreamSpecFields: EventStreamSpecFields{
				Name:   ptrTo("stream1"),
				Status: ptrTo(EventStreamStatusStopped),
			},
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(existing.spec, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.checkpoints.On("DeleteMany", mock.Anything, mock.Anything).Return(fmt.Errorf("pop")).Once()
	})
	done()
	existing.esm = esm

	esm.addStream(ctx, existing)
	err := esm.ResetStream(ctx, existing.spec.GetID(), nil)
	assert.Regexp(t, "pop", err)

}

func TestResetStreamStopFailUpdateSequence(t *testing.T) {
	existing := &eventStream[*GenericEventStream, testData]{
		spec: &GenericEventStream{
			ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
			EventStreamSpecFields: EventStreamSpecFields{
				Name:   ptrTo("stream1"),
				Status: ptrTo(EventStreamStatusStopped),
			},
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(existing.spec, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.checkpoints.On("DeleteMany", mock.Anything, mock.Anything).Return(nil).Once()
		mp.eventStreams.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("pop")).Once()
	})
	done()
	existing.esm = esm

	esm.addStream(ctx, existing)
	err := esm.ResetStream(ctx, existing.spec.GetID(), ptrTo("12345"))
	assert.Regexp(t, "pop", err)

}

func TestResetStreamNoOp(t *testing.T) {
	existing := &eventStream[*GenericEventStream, testData]{
		spec: &GenericEventStream{
			ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
			EventStreamSpecFields: EventStreamSpecFields{
				Name:   ptrTo("stream1"),
				Status: ptrTo(EventStreamStatusStopped),
			},
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(existing.spec, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.checkpoints.On("DeleteMany", mock.Anything, mock.Anything).Return(nil).Once()
		mp.eventStreams.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	})
	done()
	existing.esm = esm

	esm.addStream(ctx, existing)
	called := false
	err := esm.ResetStream(ctx, existing.spec.GetID(), ptrTo("12345"), func(_ context.Context, spec *GenericEventStream) error {
		called = true
		assert.Equal(t, existing.spec, spec)
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, called)

}

func TestResetStreamCallbackErr(t *testing.T) {
	existing := &eventStream[*GenericEventStream, testData]{
		spec: &GenericEventStream{
			ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
			EventStreamSpecFields: EventStreamSpecFields{
				Name:   ptrTo("stream1"),
				Status: ptrTo(EventStreamStatusStopped),
			},
		},
	}
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(existing.spec, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.checkpoints.On("DeleteMany", mock.Anything, mock.Anything).Return(nil).Once()
		mp.eventStreams.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	})
	done()
	existing.esm = esm

	esm.addStream(ctx, existing)
	err := esm.ResetStream(ctx, existing.spec.GetID(), ptrTo("12345"), func(_ context.Context, spec *GenericEventStream) error {
		return fmt.Errorf("pop")
	})
	assert.Regexp(t, "pop", err)

}

func TestListStreamsFail(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, fmt.Errorf("pop")).Once()
	})
	defer done()

	_, _, err := esm.ListStreams(ctx, GenericEventStreamFilters.NewFilter(ctx).And())
	assert.Regexp(t, "pop", err)

}

func TestGetStreamByIDFail(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetByID", mock.Anything, mock.Anything).Return((*GenericEventStream)(nil), fmt.Errorf("pop")).Once()
	})
	defer done()

	_, err := esm.GetStreamByID(ctx, fftypes.NewUUID().String())
	assert.Regexp(t, "pop", err)

}

func TestGetStreamByNameOrID(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
		mp.eventStreams.On("GetByUUIDOrName", mock.Anything, mock.Anything, dbsql.FailIfNotFound).Return(&GenericEventStream{}, nil).Once()
	})
	defer done()

	_, err := esm.GetStreamByNameOrID(ctx, "stream1", dbsql.FailIfNotFound)
	assert.NoError(t, err)

}

func TestCloseSuspendFail(t *testing.T) {
	ctx, esm, _, done := newMockESManager(t, func(mp *mockPersistence) {
		mp.eventStreams.On("GetMany", mock.Anything, mock.Anything).Return([]*GenericEventStream{}, &ffapi.FilterResult{}, nil).Once()
	})
	done()

	existing := &eventStream[*GenericEventStream, testData]{
		spec: &GenericEventStream{
			ResourceBase: dbsql.ResourceBase{ID: fftypes.NewUUID()},
			EventStreamSpecFields: EventStreamSpecFields{
				Name:   ptrTo("stream1"),
				Status: ptrTo(EventStreamStatusStopped),
			},
		},
		esm:         esm,
		activeState: &activeStream[*GenericEventStream, testData]{},
		stopping:    make(chan struct{}),
	}
	esm.addStream(ctx, existing)
	esm.Close(ctx)

}
