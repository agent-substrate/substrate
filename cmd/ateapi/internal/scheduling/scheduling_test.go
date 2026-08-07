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

package scheduling

import (
	"context"
	"errors"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"k8s.io/apimachinery/pkg/labels"
)

func TestSchedule(t *testing.T) {
	tierOne := map[string]string{"tier": "1"}
	tierTwo := map[string]string{"tier": "2"}

	tests := []struct {
		name        string
		fleet       fleet
		constraints Constraints
		wantPod     string // "" means ErrNoCapacity expected
	}{
		{
			name: "picks the only eligible free worker",
			fleet: fleet{
				worker("w-busy", "gvisor", "node-a", tierTwo, assigned("demo", "other")),
				worker("w-free", "gvisor", "node-a", tierTwo),
			},
			constraints: Constraints{SandboxClass: "gvisor"},
			wantPod:     "w-free",
		},
		{
			name: "sandbox class is a hard constraint",
			fleet: fleet{
				worker("w-vm", "microvm", "node-a", tierTwo),
			},
			constraints: Constraints{SandboxClass: "gvisor"},
		},
		{
			name: "template selector filters",
			fleet: fleet{
				worker("w-1", "gvisor", "node-a", tierTwo),
				worker("w-2", "gvisor", "node-a", tierOne),
			},
			constraints: Constraints{SandboxClass: "gvisor",
				TemplateSelector: labels.SelectorFromSet(labels.Set{"tier": "1"})},
			wantPod: "w-2",
		},
		{
			name: "actor selector filters on top of template selector",
			fleet: fleet{
				worker("w-1", "gvisor", "node-a", map[string]string{"tier": "1", "worload": "a"}),
				worker("w-2", "gvisor", "node-b", map[string]string{"tier": "1", "workload": "b"}),
			},
			constraints: Constraints{SandboxClass: "gvisor",
				TemplateSelector: labels.SelectorFromSet(labels.Set{"tier": "1"}),
				ActorSelector:    labels.SelectorFromSet(labels.Set{"workload": "b"})},
			wantPod: "w-2",
		},
		{
			name: "node restriction is a hard constraint",
			fleet: fleet{
				worker("w-a", "gvisor", "node-a", tierTwo),
				worker("w-b", "gvisor", "node-b", tierTwo),
			},
			constraints: Constraints{SandboxClass: "gvisor", RequiredNodes: []string{"node-b"}},
			wantPod:     "w-b",
		},
		{
			name: "nil selectors match everything",
			fleet: fleet{
				worker("w-1", "gvisor", "node-a", nil),
			},
			constraints: Constraints{SandboxClass: "gvisor"},
			wantPod:     "w-1",
		},
		{
			name: "busy workers never scheduled even if eligible",
			fleet: fleet{
				worker("w-busy", "gvisor", "node-a", tierTwo, assigned("demo", "a")),
			},
			constraints: Constraints{SandboxClass: "gvisor"},
		},
		{
			name: "draining workers never scheduled",
			fleet: fleet{
				worker("w-draining", "gvisor", "node-a", tierTwo, withState(ateapipb.Worker_STATE_DRAINING)),
			},
			constraints: Constraints{SandboxClass: "gvisor"},
		},
		{
			name: "unspecified workers never scheduled",
			fleet: fleet{
				worker("w-unspecified", "gvisor", "node-a", tierTwo, withState(ateapipb.Worker_STATE_UNSPECIFIED)),
			},
			constraints: Constraints{SandboxClass: "gvisor"},
		},
		{
			name: "picks active worker over draining one",
			fleet: fleet{
				worker("w-draining", "gvisor", "node-a", tierTwo, withState(ateapipb.Worker_STATE_DRAINING)),
				worker("w-active", "gvisor", "node-a", tierTwo),
			},
			constraints: Constraints{SandboxClass: "gvisor"},
			wantPod:     "w-active",
		},
		{
			name:        "empty fleet",
			fleet:       fleet{},
			constraints: Constraints{SandboxClass: "gvisor"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(tc.fleet, WithIntn(firstIntn))
			got, err := s.Schedule(context.Background(), tc.constraints)

			if tc.wantPod != "" {
				if err != nil {
					t.Fatalf("Schedule() error = %v, want worker %q", err, tc.wantPod)
				}
				if got.GetWorkerPod() != tc.wantPod {
					t.Fatalf("Schedule() = %q, want %q", got.GetWorkerPod(), tc.wantPod)
				}
				return
			}

			if !errors.Is(err, ErrNoCapacity) {
				t.Fatalf("Schedule() error = %v, want ErrNoCapacity", err)
			}
		})
	}
}

func TestApplies(t *testing.T) {
	tierSel := labels.SelectorFromSet(labels.Set{"tier": "1"})
	workloadSel := labels.SelectorFromSet(labels.Set{"workload": "a"})

	tests := []struct {
		name        string
		worker      *ateapipb.Worker
		constraints Constraints
		want        bool
	}{
		{
			name:   "satisfies all constraints",
			worker: worker("w", "gvisor", "node-a", map[string]string{"tier": "1", "workload": "a"}),
			constraints: Constraints{SandboxClass: "gvisor", TemplateSelector: tierSel, RequiredNodes: []string{"node-a"},
				ActorSelector: workloadSel},
			want: true,
		},
		{
			name:        "assignment is ignored",
			worker:      worker("w", "gvisor", "node-a", nil, assigned("demo", "someone-else")),
			constraints: Constraints{SandboxClass: "gvisor"},
			want:        true,
		},
		{
			name:        "class mismatch",
			worker:      worker("w", "microvm", "node-a", nil),
			constraints: Constraints{SandboxClass: "gvisor"},
			want:        false,
		},
		{
			name:        "template selector mismatch",
			worker:      worker("w", "gvisor", "node-a", map[string]string{"tier": "2"}),
			constraints: Constraints{SandboxClass: "gvisor", TemplateSelector: tierSel},
			want:        false,
		},
		{
			name:        "actor selector mismatch",
			worker:      worker("w", "gvisor", "node-a", map[string]string{"workload": "b"}),
			constraints: Constraints{SandboxClass: "gvisor", ActorSelector: workloadSel},
			want:        false,
		},
		{
			name:        "node restriction excludes",
			worker:      worker("w", "gvisor", "node-b", nil),
			constraints: Constraints{SandboxClass: "gvisor", RequiredNodes: []string{"node-a"}},
			want:        false,
		},
		{
			name:   "AND of template and actor selectors match",
			worker: worker("w", "gvisor", "node-a", map[string]string{"tier": "1", "workload": "a"}),
			constraints: Constraints{SandboxClass: "gvisor",
				TemplateSelector: tierSel,
				ActorSelector:    workloadSel},
			want: true,
		},
		{
			name:   "AND of two selectors, one fails",
			worker: worker("w", "gvisor", "node-a", map[string]string{"tier": "1", "workload": "b"}),
			constraints: Constraints{SandboxClass: "gvisor",
				TemplateSelector: tierSel,
				ActorSelector:    workloadSel},
			want: false,
		},
		{
			name:        "skips draining worker",
			worker:      worker("w", "gvisor", "node-a", nil, withState(ateapipb.Worker_STATE_DRAINING)),
			constraints: Constraints{SandboxClass: "gvisor"},
			want:        false,
		},
		{
			name:        "skips unspecified worker",
			worker:      worker("w", "gvisor", "node-a", nil, withState(ateapipb.Worker_STATE_UNSPECIFIED)),
			constraints: Constraints{SandboxClass: "gvisor"},
			want:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New(fleet{})
			if got := s.Applies(tc.worker, tc.constraints); got != tc.want {
				t.Fatalf("Applies() = %v, want %v", got, tc.want)
			}
		})
	}
}

type fleet []*ateapipb.Worker

func (f fleet) Workers() ([]*ateapipb.Worker, error) { return f, nil }

func worker(pod, class, node string, lbls map[string]string, opts ...func(*ateapipb.Worker)) *ateapipb.Worker {
	w := &ateapipb.Worker{
		WorkerPod:    pod,
		SandboxClass: class,
		NodeName:     node,
		State:        ateapipb.Worker_STATE_ACTIVE,
		Labels:       lbls,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func withState(state ateapipb.Worker_State) func(*ateapipb.Worker) {
	return func(w *ateapipb.Worker) {
		w.State = state
	}
}

func assigned(atespace, name string) func(*ateapipb.Worker) {
	return func(w *ateapipb.Worker) {
		w.Assignment = &ateapipb.Assignment{
			Actor:    &ateapipb.ObjectRef{Atespace: atespace, Name: name},
			ActorUid: atespace + "/" + name,
		}
	}
}

// firstIntn always picks the first candidate, making Schedule deterministic.
func firstIntn(int) int { return 0 }

func workerWithPool(pod, ns, pool, class, node string, lbls map[string]string, opts ...func(*ateapipb.Worker)) *ateapipb.Worker {
	w := worker(pod, class, node, lbls, opts...)
	w.WorkerNamespace = ns
	w.WorkerPool = pool
	return w
}

func TestSchedule_EligibleWorkersMetric(t *testing.T) {
	t.Run("records histogram metric with namespaced attributes for candidates", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		meter := provider.Meter("test")

		flt := fleet{
			workerWithPool("w-1", "ns-a", "pool-1", "gvisor", "node-a", nil),
			workerWithPool("w-2", "ns-a", "pool-1", "gvisor", "node-a", nil, assigned("demo", "a")),
			workerWithPool("w-3", "ns-b", "pool-2", "gvisor", "node-b", nil),
		}

		s := New(flt, WithIntn(firstIntn), WithMeter(meter))
		_, err := s.Schedule(context.Background(), Constraints{SandboxClass: "gvisor"})
		if err != nil {
			t.Fatalf("Schedule() error = %v", err)
		}

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("reader.Collect() error = %v", err)
		}

		foundMetric := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "ate.scheduler.eligible_workers" {
					foundMetric = true
					histogram, ok := m.Data.(metricdata.Histogram[int64])
					if !ok {
						t.Fatalf("metric Data is %T, want metricdata.Histogram[int64]", m.Data)
					}
					if len(histogram.DataPoints) == 0 {
						t.Fatalf("got 0 DataPoints for ate.scheduler.eligible_workers")
					}
					for _, dp := range histogram.DataPoints {
						attrs := dp.Attributes
						ns, _ := attrs.Value(ateattr.WorkerPoolNamespaceKey)
						pool, _ := attrs.Value(ateattr.WorkerPoolNameKey)
						class, _ := attrs.Value(ateattr.SandboxClassKey)
						constraint, _ := attrs.Value(ateattr.SchedulingConstraintKey)
						if class.AsString() != "gvisor" {
							t.Errorf("got sandbox class %q, want %q", class.AsString(), "gvisor")
						}
						if constraint.AsString() != "none" {
							t.Errorf("got constraint %q, want %q", constraint.AsString(), "none")
						}
						if ns.AsString() == "ns-a" && pool.AsString() == "pool-1" {
							if dp.Count != 1 || dp.Sum != 1 {
								t.Errorf("pool-1 datapoint count=%d sum=%d, want count=1 sum=1", dp.Count, dp.Sum)
							}
						}
						if ns.AsString() == "ns-b" && pool.AsString() == "pool-2" {
							if dp.Count != 1 || dp.Sum != 1 {
								t.Errorf("pool-2 datapoint count=%d sum=%d, want count=1 sum=1", dp.Count, dp.Sum)
							}
						}
					}
				}
			}
		}
		if !foundMetric {
			t.Fatalf("ate.scheduler.eligible_workers metric not found")
		}
	})

	t.Run("records 0 eligible workers when fleet has no capacity", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		meter := provider.Meter("test")

		flt := fleet{
			workerWithPool("w-busy", "ns-a", "pool-1", "gvisor", "node-a", nil, assigned("demo", "a")),
		}

		s := New(flt, WithIntn(firstIntn), WithMeter(meter))
		_, err := s.Schedule(context.Background(), Constraints{SandboxClass: "gvisor"})
		if !errors.Is(err, ErrNoCapacity) {
			t.Fatalf("Schedule() error = %v, want ErrNoCapacity", err)
		}

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("reader.Collect() error = %v", err)
		}

		foundMetric := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "ate.scheduler.eligible_workers" {
					foundMetric = true
					histogram, ok := m.Data.(metricdata.Histogram[int64])
					if !ok {
						t.Fatalf("metric Data is %T, want metricdata.Histogram[int64]", m.Data)
					}
					if len(histogram.DataPoints) == 0 {
						t.Fatalf("got 0 DataPoints")
					}
					dp := histogram.DataPoints[0]
					if dp.Sum != 0 {
						t.Errorf("datapoint sum = %d, want 0", dp.Sum)
					}
					attrs := dp.Attributes
					ns, _ := attrs.Value(ateattr.WorkerPoolNamespaceKey)
					pool, _ := attrs.Value(ateattr.WorkerPoolNameKey)
					constraint, _ := attrs.Value(ateattr.SchedulingConstraintKey)
					if ns.AsString() != "ns-a" || pool.AsString() != "pool-1" {
						t.Errorf("got namespace=%q pool=%q, want ns-a / pool-1", ns.AsString(), pool.AsString())
					}
					if constraint.AsString() != "none" {
						t.Errorf("got constraint=%q, want none", constraint.AsString())
					}
				}
			}
		}
		if !foundMetric {
			t.Fatalf("ate.scheduler.eligible_workers metric not found")
		}
	})

	t.Run("records constraint classification attributes correctly", func(t *testing.T) {
		sel, _ := labels.Parse("env=prod")
		tests := []struct {
			name           string
			constraints    Constraints
			wantConstraint string
		}{
			{
				name:           "none",
				constraints:    Constraints{SandboxClass: "gvisor"},
				wantConstraint: ateattr.ConstraintNone,
			},
			{
				name:           "selector",
				constraints:    Constraints{SandboxClass: "gvisor", ActorSelector: sel},
				wantConstraint: ateattr.ConstraintSelector,
			},
			{
				name:           "required_nodes",
				constraints:    Constraints{SandboxClass: "gvisor", ActorSelector: sel, RequiredNodes: []string{"node-a"}},
				wantConstraint: ateattr.ConstraintRequiredNodes,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				reader := sdkmetric.NewManualReader()
				provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
				meter := provider.Meter("test")

				flt := fleet{
					workerWithPool("w-1", "ns-a", "pool-1", "gvisor", "node-a", map[string]string{"env": "prod"}),
				}

				s := New(flt, WithIntn(firstIntn), WithMeter(meter))
				_, err := s.Schedule(context.Background(), tc.constraints)
				if err != nil {
					t.Fatalf("Schedule() error = %v", err)
				}

				var rm metricdata.ResourceMetrics
				_ = reader.Collect(context.Background(), &rm)
				for _, sm := range rm.ScopeMetrics {
					for _, m := range sm.Metrics {
						if m.Name == "ate.scheduler.eligible_workers" {
							histogram := m.Data.(metricdata.Histogram[int64])
							dp := histogram.DataPoints[0]
							constraint, _ := dp.Attributes.Value(ateattr.SchedulingConstraintKey)
							if constraint.AsString() != tc.wantConstraint {
								t.Errorf("got constraint=%q, want %q", constraint.AsString(), tc.wantConstraint)
							}
						}
					}
				}
			})
		}
	})

	t.Run("records 0 eligible workers when fleet is completely empty", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		meter := provider.Meter("test")

		flt := fleet{}

		s := New(flt, WithIntn(firstIntn), WithMeter(meter))
		_, err := s.Schedule(context.Background(), Constraints{SandboxClass: "gvisor"})
		if !errors.Is(err, ErrNoCapacity) {
			t.Fatalf("Schedule() error = %v, want ErrNoCapacity", err)
		}

		var rm metricdata.ResourceMetrics
		_ = reader.Collect(context.Background(), &rm)
		foundMetric := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "ate.scheduler.eligible_workers" {
					foundMetric = true
					histogram := m.Data.(metricdata.Histogram[int64])
					dp := histogram.DataPoints[0]
					if dp.Sum != 0 {
						t.Errorf("datapoint sum = %d, want 0", dp.Sum)
					}
					class, _ := dp.Attributes.Value(ateattr.SandboxClassKey)
					if class.AsString() != "gvisor" {
						t.Errorf("got sandbox class %q, want gvisor", class.AsString())
					}
				}
			}
		}
		if !foundMetric {
			t.Fatalf("ate.scheduler.eligible_workers metric not found")
		}
	})

	t.Run("records 0 eligible workers on sandbox class mismatch", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		meter := provider.Meter("test")

		flt := fleet{
			workerWithPool("w-1", "ns-a", "pool-1", "gvisor", "node-a", nil),
		}

		s := New(flt, WithIntn(firstIntn), WithMeter(meter))
		_, err := s.Schedule(context.Background(), Constraints{SandboxClass: "kata"})
		if !errors.Is(err, ErrNoCapacity) {
			t.Fatalf("Schedule() error = %v, want ErrNoCapacity", err)
		}

		var rm metricdata.ResourceMetrics
		_ = reader.Collect(context.Background(), &rm)
		foundMetric := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "ate.scheduler.eligible_workers" {
					foundMetric = true
					histogram := m.Data.(metricdata.Histogram[int64])
					dp := histogram.DataPoints[0]
					if dp.Sum != 0 {
						t.Errorf("datapoint sum = %d, want 0", dp.Sum)
					}
					class, _ := dp.Attributes.Value(ateattr.SandboxClassKey)
					if class.AsString() != "kata" {
						t.Errorf("got sandbox class %q, want kata", class.AsString())
					}
				}
			}
		}
		if !foundMetric {
			t.Fatalf("ate.scheduler.eligible_workers metric not found")
		}
	})

	t.Run("excludes draining and inactive workers from eligible counts", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		meter := provider.Meter("test")

		drainingWorker := workerWithPool("w-draining", "ns-a", "pool-1", "gvisor", "node-a", nil)
		drainingWorker.State = ateapipb.Worker_STATE_DRAINING

		flt := fleet{drainingWorker}

		s := New(flt, WithIntn(firstIntn), WithMeter(meter))
		_, err := s.Schedule(context.Background(), Constraints{SandboxClass: "gvisor"})
		if !errors.Is(err, ErrNoCapacity) {
			t.Fatalf("Schedule() error = %v, want ErrNoCapacity", err)
		}

		var rm metricdata.ResourceMetrics
		_ = reader.Collect(context.Background(), &rm)
		foundMetric := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "ate.scheduler.eligible_workers" {
					foundMetric = true
					histogram := m.Data.(metricdata.Histogram[int64])
					dp := histogram.DataPoints[0]
					if dp.Sum != 0 {
						t.Errorf("datapoint sum = %d, want 0", dp.Sum)
					}
				}
			}
		}
		if !foundMetric {
			t.Fatalf("ate.scheduler.eligible_workers metric not found")
		}
	})
}
