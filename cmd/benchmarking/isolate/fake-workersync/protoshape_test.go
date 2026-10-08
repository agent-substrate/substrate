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

import (
	"testing"

	"github.com/agent-substrate/substrate/cmd/benchmarking/isolate/internal/protoshapetest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
)

// TestProtoShapes pins the field set of the WorkerService request
// fake-workersync sends through fake-atelet's relay, and of the capacity it
// builds for it. A capacity report is faithful only while it carries what a
// real atelet reports. When this fails, decide what fake-workersync should
// report for the change, then update the expected fields in the table below.
func TestProtoShapes(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		fields []string
	}{
		{&ateapipb.RegisterWorkerRequest{}, []string{
			"1 worker ateapi.ObjectRef",
			"2 capacity ateapi.WorkerResources",
			"3 hardware ateapi.HardwareIdentity",
		}},
		{&ateapipb.WorkerResources{}, []string{
			"1 resources ateapi.Resources",
			"2 actors int32",
		}},
	} {
		name := tc.msg.ProtoReflect().Descriptor().FullName()
		if diff := cmp.Diff(tc.fields, protoshapetest.Fields(tc.msg)); diff != "" {
			t.Errorf("%s fields changed (-expected +now):\n%s\nDecide what fake-workersync should report for the change, then update the expected fields in TestProtoShapes.", name, diff)
		}
	}
}
