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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestFileReconcilerE2E(t *testing.T) {

	logrus.SetLevel(logrus.DebugLevel)
	tmpDir := t.TempDir()

	filePath := fmt.Sprintf("%s/test.yaml", tmpDir)

	// Create the file
	os.WriteFile(fmt.Sprintf("%s/test.yaml", tmpDir), []byte(`{"ut_conf": "one"}`), 0664)

	// Read initial config
	viper.SetConfigType("yaml")
	viper.SetConfigFile(filePath)
	viper.ReadInConfig()
	assert.Equal(t, "one", viper.Get("ut_conf"))

	// Start listener on config file
	fsListenerDone := make(chan struct{})
	fsListenerFired := make(chan bool)
	reSyncFired := make(chan bool)
	reSyncInterval := 1 * time.Second
	ctx, cancelCtx := context.WithCancel(context.Background())
	err := Reconcile(ctx, filePath, func() {
		err := viper.ReadInConfig()
		assert.NoError(t, err)
		fsListenerFired <- true
	}, func() {
		close(fsListenerDone)
	}, func() {
		err := viper.ReadInConfig()
		assert.NoError(t, err)
		reSyncFired <- true
	}, &reSyncInterval)
	assert.NoError(t, err)

	// Delete and rename in another file
	os.Remove(fmt.Sprintf("%s/test.yaml", tmpDir))
	os.WriteFile(fmt.Sprintf("%s/another.yaml", tmpDir), []byte(`{"ut_conf": "two"}`), 0664)
	os.Rename(fmt.Sprintf("%s/another.yaml", tmpDir), fmt.Sprintf("%s/test.yaml", tmpDir))
	<-fsListenerFired
	assert.Equal(t, "two", viper.Get("ut_conf"))
	<-reSyncFired

	defer func() {
		cancelCtx()
		if a := recover(); a != nil {
			panic(a)
		}
		<-fsListenerDone
	}()

}

func TestFileWatcherFail(t *testing.T) {

	logrus.SetLevel(logrus.DebugLevel)
	tmpDir := t.TempDir()
	os.RemoveAll(tmpDir)

	filepath := fmt.Sprintf("%s/test.yaml", tmpDir)

	viper.SetConfigType("yaml")
	viper.SetConfigFile(filepath)

	err := Watch(context.Background(), filepath, nil, nil)
	assert.Regexp(t, "FF00194", err)
}

func TestFileListenerLogError(t *testing.T) {

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()
	errors := make(chan error)
	fsListenerDone := make(chan struct{})
	go fsListenerLoop(ctx, "somefile", func() {}, func() { close(fsListenerDone) }, nil, nil, make(chan fsnotify.Event), errors)

	errors <- fmt.Errorf("pop")
	cancelCtx()
	<-fsListenerDone
}
