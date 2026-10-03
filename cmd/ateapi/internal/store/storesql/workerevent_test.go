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

package storesql

import (
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestUnmarshalWorkerEvent_BoundaryAssertions pins the write-side invariants
// asserted at the read boundary: known event-type byte and a keyable worker.
func TestUnmarshalWorkerEvent_BoundaryAssertions(t *testing.T) {
	valid, err := MarshalWorkerEvent(store.WorkerEventUpdated, &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: "w1"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	nameless, err := MarshalWorkerEvent(store.WorkerEventDeleted, &ateapipb.Worker{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for name, tc := range map[string]struct {
		payload []byte
		wantErr bool
	}{
		"valid":             {valid, false},
		"empty":             {nil, true},
		"unknown type byte": {[]byte{0xff, 0x00}, true},
		"type byte only":    {[]byte{byte(store.WorkerEventCreated)}, true}, // empty proto = nameless
		"garbage proto":     {append([]byte{byte(store.WorkerEventCreated)}, 0xde, 0xad, 0xbe), true},
		"nameless worker":   {nameless, true},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := UnmarshalWorkerEvent(tc.payload)
			if (err != nil) != tc.wantErr {
				t.Fatalf("UnmarshalWorkerEvent() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}
