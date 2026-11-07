// Copyright 2026 The Agent Substrate Authors
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

package harness

import (
	"slices"
	"sync"
)

// RPC is one call a fake received, faulted or not. Request is the call's
// request message, or for a call that takes plain arguments, a harness struct
// holding them.
type RPC[Op ~string] struct {
	Op      Op
	Request any
}

// rpcLog records the calls a fake receives, in order.
type rpcLog[Op ~string] struct {
	mu   sync.Mutex
	rpcs []RPC[Op]
}

// add records a call to op. req must not be changed afterwards.
func (l *rpcLog[Op]) add(op Op, req any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rpcs = append(l.rpcs, RPC[Op]{Op: op, Request: req})
}

// record starts a recording of the calls l receives from now on.
func (l *rpcLog[Op]) record() *RPCRecording[Op] {
	l.mu.Lock()
	defer l.mu.Unlock()
	return &RPCRecording[Op]{log: l, start: len(l.rpcs)}
}

// RPCRecording holds the calls a fake receives from the moment it is started.
type RPCRecording[Op ~string] struct {
	log   *rpcLog[Op]
	start int
}

// RPCs returns the calls the fake received since r started, in order.
func (r *RPCRecording[Op]) RPCs() []RPC[Op] {
	r.log.mu.Lock()
	defer r.log.mu.Unlock()
	return slices.Clone(r.log.rpcs[r.start:])
}
