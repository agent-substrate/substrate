// Copyright 2026 Google LLC
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

import "sync"

// workerEpochs records the epoch each Worker on this node registered with
// through the relay, for the herder to report from Run and Restore as the
// worker process would. ate-api-server commits a resumed Actor as RUNNING
// only when that epoch is the Worker's active one.
//
// It is kept in memory only. A fake-atelet that restarts after its Workers
// registered reports epoch 0 for them, and fake-workersync never registers a
// Worker again, so every resume on those Workers fails with Unavailable until
// they are replaced.
//
// A nil workerEpochs records nothing and reports 0.
type workerEpochs struct {
	mu     sync.Mutex
	epochs map[string]int64
}

func newWorkerEpochs() *workerEpochs {
	return &workerEpochs{epochs: map[string]int64{}}
}

// set records epoch for the Worker named name.
func (e *workerEpochs) set(name string, epoch int64) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.epochs[name] = epoch
}

// get returns the epoch the Worker named name registered with, or 0 if it has
// not registered through this fake-atelet.
func (e *workerEpochs) get(name string) int64 {
	if e == nil {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.epochs[name]
}
