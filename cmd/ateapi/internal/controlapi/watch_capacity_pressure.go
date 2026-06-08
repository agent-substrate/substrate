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

package controlapi

import (
	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// WatchCapacityPressure streams a CapacityPressureEvent every time a pool has
// no free worker for a resume, until the client disconnects.
func (s *Service) WatchCapacityPressure(_ *ateapipb.WatchCapacityPressureRequest, stream grpc.ServerStreamingServer[ateapipb.CapacityPressureEvent]) error {
	events, cancel := s.pressure.Subscribe()
	defer cancel()

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case key := <-events:
			if err := stream.Send(&ateapipb.CapacityPressureEvent{
				WorkerNamespace: key.namespace,
				WorkerPool:      key.name,
			}); err != nil {
				return err
			}
		}
	}
}
