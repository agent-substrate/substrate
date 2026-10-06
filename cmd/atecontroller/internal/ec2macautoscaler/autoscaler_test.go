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

package ec2macautoscaler

import (
	"context"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/labels"
)

type fakeScaler struct {
	desired int32
	maximum int32
	writes  []int32
}

func (f *fakeScaler) Desired(context.Context) (int32, int32, error) { return f.desired, f.maximum, nil }
func (f *fakeScaler) SetDesired(_ context.Context, desired int32) error {
	f.desired = desired
	f.writes = append(f.writes, desired)
	return nil
}

func testConfig() Config {
	return Config{
		LocalSelector:       labels.SelectorFromSet(labels.Set{"provider": "local"}),
		CloudSelector:       labels.SelectorFromSet(labels.Set{"provider": "aws"}),
		PollInterval:        time.Minute,
		LeadTime:            20 * time.Minute,
		ScaleUpCooldown:     10 * time.Minute,
		StressUtilization:   .75,
		StressSamples:       3,
		CloudHeadroomActors: 1,
		MaxCloudWorkers:     4,
	}
}

func macWorker(provider string, capacity, allocated int32) *ateapipb.Worker {
	return &ateapipb.Worker{
		SandboxClass: "macos-vz",
		Labels:       map[string]string{"provider": provider},
		Status: &ateapipb.WorkerStatus{
			State:     ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:  &ateapipb.WorkerResources{Actors: capacity},
			Allocated: &ateapipb.WorkerResources{Actors: allocated},
		},
	}
}

func TestSustainedStressWarmsOneWorker(t *testing.T) {
	scaler := &fakeScaler{maximum: 10}
	a, err := New(testConfig(), func(context.Context) ([]*ateapipb.Worker, error) { return nil, nil }, scaler)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0)
	workers := []*ateapipb.Worker{macWorker("local", 4, 3)}
	for i := range 3 {
		if err := a.reconcile(context.Background(), now.Add(time.Duration(i)*time.Minute), workers); err != nil {
			t.Fatal(err)
		}
		if i < 2 && len(scaler.writes) != 0 {
			t.Fatalf("scaled after %d stress samples, want 3 consecutive samples", i+1)
		}
	}
	if len(scaler.writes) != 1 || scaler.writes[0] != 1 {
		t.Fatalf("desired writes = %v, want [1]", scaler.writes)
	}
}

func TestProjectedExhaustionTriggersBeforeUtilizationThreshold(t *testing.T) {
	config := testConfig()
	config.StressSamples = 1
	scaler := &fakeScaler{maximum: 10}
	a, err := New(config, func(context.Context) ([]*ateapipb.Worker, error) { return nil, nil }, scaler)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0)
	if err := a.reconcile(context.Background(), now, []*ateapipb.Worker{macWorker("local", 10, 2)}); err != nil {
		t.Fatal(err)
	}
	// One additional allocation per minute projects exhaustion in seven minutes,
	// even though utilization is only 30%.
	if err := a.reconcile(context.Background(), now.Add(time.Minute), []*ateapipb.Worker{macWorker("local", 10, 3)}); err != nil {
		t.Fatal(err)
	}
	if len(scaler.writes) != 1 {
		t.Fatalf("desired writes = %v, want one predictive scale-up", scaler.writes)
	}
}

func TestWarmCloudHeadroomSuppressesScaleUp(t *testing.T) {
	config := testConfig()
	config.StressSamples = 1
	scaler := &fakeScaler{desired: 1, maximum: 10}
	a, err := New(config, func(context.Context) ([]*ateapipb.Worker, error) { return nil, nil }, scaler)
	if err != nil {
		t.Fatal(err)
	}
	workers := []*ateapipb.Worker{macWorker("local", 4, 4), macWorker("aws", 2, 1)}
	if err := a.reconcile(context.Background(), time.Unix(0, 0), workers); err != nil {
		t.Fatal(err)
	}
	if len(scaler.writes) != 0 {
		t.Fatalf("desired writes = %v, want none while EC2 has headroom", scaler.writes)
	}
}

func TestPendingCloudWorkerSuppressesAnotherScaleUp(t *testing.T) {
	config := testConfig()
	config.StressSamples = 1
	scaler := &fakeScaler{desired: 1, maximum: 10}
	a, err := New(config, func(context.Context) ([]*ateapipb.Worker, error) { return nil, nil }, scaler)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.reconcile(context.Background(), time.Unix(0, 0), []*ateapipb.Worker{macWorker("local", 4, 4)}); err != nil {
		t.Fatal(err)
	}
	if len(scaler.writes) != 0 {
		t.Fatalf("desired writes = %v, want none while desired EC2 Worker is still registering", scaler.writes)
	}
}
