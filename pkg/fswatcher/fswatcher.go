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

package fswatcher

import (
	"context"
	"os"
	"path"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
)

// Listens for changes to the specified file.
// Note this:
// - Only ends if the context is closed
// - Listens at the directory level, for cross OS compatibility
// - Only fires if the config file has changed, and can be read
// - Only fires if the data in the file is different to the last notification
// - Does not reload the config - that's the caller's responsibility
func Watch(ctx context.Context, fullFilePath string, onChange, onClose func()) error {
	return sync(ctx, fullFilePath, onChange, onClose, nil, nil)
}

// Reconcile behaves the same as Watch, except it allows for running the onSync func on a provided
// interval. The default re-sync internal is 1m.
func Reconcile(ctx context.Context, fullFilePath string, onChange, onClose, onSync func(), resyncInterval *time.Duration) error {
	return sync(ctx, fullFilePath, onChange, onClose, onSync, resyncInterval)
}

func sync(ctx context.Context, fullFilePath string, onChange, onClose, onSync func(), resyncInterval *time.Duration) error {
	filePath := path.Dir(fullFilePath)
	fileName := path.Base(fullFilePath)
	log.L(ctx).Debugf("Starting file reconciler for '%s' in directory '%s'", fileName, filePath)

	watcher, err := fsnotify.NewWatcher()
	if err == nil {
		go fsListenerLoop(ctx, fullFilePath, onChange, func() {
			_ = watcher.Close()
			if onClose != nil {
				onClose()
			}
		}, onSync, resyncInterval, watcher.Events, watcher.Errors)
		err = watcher.Add(filePath)
	}
	if err != nil {
		log.L(ctx).Errorf("Failed to start filesystem listener: %s", err)
		return i18n.WrapError(ctx, err, i18n.MsgFailedToStartListener, err)
	}
	return nil
}

func fsListenerLoop(ctx context.Context, fullFilePath string, onChange, onClose, onSync func(), resyncInterval *time.Duration, events chan fsnotify.Event, errors chan error) {
	defer onClose()

	timeout := resyncInterval
	if timeout == nil {
		timeout = func() *time.Duration {
			defaultTimeout := time.Minute
			return &defaultTimeout
		}()
	}
	log.L(ctx).Debugf("re-sync interval set to '%s'", *timeout)

	var lastHash *fftypes.Bytes32
	for {
		select {
		case <-ctx.Done():
			// The only case where we end, is when the context is closed
			log.L(ctx).Infof("File listener exiting")
			return
		case event := <-events:
			isUpdate := (event.Op&(fsnotify.Create|fsnotify.Rename|fsnotify.Write) != 0)
			log.L(ctx).Debugf("FSEvent [%s] update=%t: %s", event.Op, isUpdate, event.Name)
			// Note that we are not guaranteed to get an event that has the target filename in `event.Name`,
			// particularly when listening in k8s to the softlink renames that occur for configmap/secret
			// dynamic updates. So we use a hash of the file in all cases when something in the directory changes.
			if isUpdate {
				data, err := os.ReadFile(fullFilePath)
				if err == nil {
					dataHash := fftypes.HashString(string(data))
					if lastHash == nil || !dataHash.Equals(lastHash) {
						log.L(ctx).Infof("Config file change detected. Event=%s Name=%s Size=%d Hash=%s", event.Op, fullFilePath, len(data), dataHash)
						onChange()
					}
					lastHash = dataHash
				}
			}
		case <-time.After(*timeout):
			if onSync != nil {
				data, err := os.ReadFile(fullFilePath)
				if err == nil {
					dataHash := fftypes.HashString(string(data))
					log.L(ctx).Infof("Config file re-sync. Event=Resync Name=%s Size=%d Hash=%s", fullFilePath, len(data), dataHash)
					onSync()
				}
			}
		case err, ok := <-errors:
			if ok {
				log.L(ctx).Errorf("FSEvent error: %s", err)
			}
		}
	}
}
