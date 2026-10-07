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

package admission

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

func TestCreateActorValidation(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	adm := New(persistence, nil, nil)
	if _, err := adm.CreateActor(ctx, &ateapipb.Actor{}); !errors.Is(err, ErrInvalid) {
		t.Errorf("CreateActor(empty) = %v, want ErrInvalid", err)
	}
}

func TestValidateTemplateVolumesUnchanged(t *testing.T) {
	vol := func(name, scName, size string) *ateapipb.Volume {
		return &ateapipb.Volume{
			Name: name,
			ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
				StorageClassName: scName,
				Capacity:         size,
			},
		}
	}

	tests := []struct {
		name    string
		current []*ateapipb.Volume
		next    []*ateapipb.Volume
		wantErr bool
	}{
		{
			name:    "both nil",
			current: nil,
			next:    nil,
		},
		{
			name:    "identical external volumes",
			current: []*ateapipb.Volume{vol("data", "fast", "10Gi")},
			next:    []*ateapipb.Volume{vol("data", "fast", "10Gi")},
		},
		{
			name:    "external volumes in different order are rejected",
			current: []*ateapipb.Volume{vol("a", "fast", "10Gi"), vol("b", "slow", "20Gi")},
			next:    []*ateapipb.Volume{vol("b", "slow", "20Gi"), vol("a", "fast", "10Gi")},
			wantErr: true,
		},
		{
			name:    "storage class changed",
			current: []*ateapipb.Volume{vol("data", "fast", "10Gi")},
			next:    []*ateapipb.Volume{vol("data", "slow", "10Gi")},
			wantErr: true,
		},
		{
			name:    "capacity changed",
			current: []*ateapipb.Volume{vol("data", "fast", "10Gi")},
			next:    []*ateapipb.Volume{vol("data", "fast", "20Gi")},
			wantErr: true,
		},
		{
			name:    "volume added",
			current: []*ateapipb.Volume{vol("data", "fast", "10Gi")},
			next:    []*ateapipb.Volume{vol("data", "fast", "10Gi"), vol("extra", "fast", "5Gi")},
			wantErr: true,
		},
		{
			name:    "volume removed",
			current: []*ateapipb.Volume{vol("data", "fast", "10Gi")},
			next:    nil,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			curTmpl := &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "cur"},
				Volumes:  tc.current,
			}
			nextTmpl := &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "next"},
				Volumes:  tc.next,
			}
			err := validateTemplateVolumesUnchanged(curTmpl, nextTmpl)
			if tc.wantErr {
				if !errors.Is(err, ErrFailedPrecondition) {
					t.Fatalf("err = %v, want ErrFailedPrecondition", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestInitialActorVolumes(t *testing.T) {
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "fast-ssd"},
		Provisioner: "pd.csi.storage.gke.io",
	}
	client := fake.NewSimpleClientset(sc)
	factory := informers.NewSharedInformerFactory(client, 0)
	lister := factory.Storage().V1().StorageClasses().Lister()
	_ = factory.Storage().V1().StorageClasses().Informer().GetIndexer().Add(sc)

	tmpl := &ateapipb.ActorTemplate{
		Volumes: []*ateapipb.Volume{
			{
				Name: "data",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "fast-ssd",
					Capacity:         "10Gi",
				},
			},
		},
	}
	vols, err := initialActorVolumes(t.Context(), lister, tmpl)
	if err != nil {
		t.Fatalf("initialActorVolumes: %v", err)
	}
	if len(vols) != 1 {
		t.Fatalf("len(vols) = %d, want 1", len(vols))
	}
	if vols[0].GetVolumeName() != "data" || vols[0].GetVolumeType() != "pd.csi.storage.gke.io" || vols[0].GetStatus() != ateapipb.ExternalVolume_STATUS_PENDING {
		t.Errorf("got volume %+v, want data / pd.csi.storage.gke.io / PENDING", vols[0])
	}

	t.Run("missing storage class", func(t *testing.T) {
		badTmpl := &ateapipb.ActorTemplate{
			Volumes: []*ateapipb.Volume{
				{
					Name: "data",
					ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
						StorageClassName: "does-not-exist",
						Capacity:         "10Gi",
					},
				},
			},
		}
		_, err := initialActorVolumes(t.Context(), lister, badTmpl)
		if !errors.Is(err, ErrFailedPrecondition) {
			t.Fatalf("err = %v, want ErrFailedPrecondition", err)
		}
	})
}

func TestUpdateActorSpecAndStatusIsolation(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	configIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := configIdx.Add(&atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-default"},
		Spec:       atev1alpha1.SandboxConfigSpec{SandboxClass: atev1alpha1.SandboxClassGvisor},
	}); err != nil {
		t.Fatalf("adding SandboxConfig: %v", err)
	}
	adm := New(persistence, listersv1alpha1.NewSandboxConfigLister(configIdx), nil)
	if _, err := adm.CreateAtespace(ctx, &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{Name: "ns"},
	}); err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}
	if _, err := adm.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl-1"},
		Containers: []*ateapipb.Container{{
			Name:  "main",
			Image: "busybox@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		}},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
			ConfigName:   "gvisor-default",
		},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation: "gs://bucket/path",
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}

	created, err := adm.CreateActor(ctx, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "ns", Name: "actor-1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns", Name: "tmpl-1"},
	})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	// UpdateActorStatus mutates status and preserves spec.
	ref := resources.ActorRef{Atespace: "ns", Name: "actor-1"}
	withRunning, err := adm.UpdateActorStatus(ctx, ref, store.PreconditionFrom(created), func(s *ateapipb.ActorStatus) error {
		s.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateActorStatus: %v", err)
	}
	if withRunning.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("status.state = %v, want RUNNING", withRunning.GetStatus().GetState())
	}

	// UpdateActorSpec mutates spec and ignores status changes on input.
	specInput := proto.Clone(withRunning).(*ateapipb.Actor)
	specInput.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"env": "prod"}}
	specInput.Status = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED}
	afterSpec, err := adm.UpdateActorSpec(ctx, specInput)
	if err != nil {
		t.Fatalf("UpdateActorSpec: %v", err)
	}
	if afterSpec.GetWorkerSelector().GetMatchLabels()["env"] != "prod" {
		t.Errorf("worker_selector = %v, want env=prod", afterSpec.GetWorkerSelector())
	}
	if afterSpec.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("UpdateActorSpec overwrote status.state = %v, want RUNNING", afterSpec.GetStatus().GetState())
	}

	// Changing actor_template while RUNNING fails with ErrFailedPrecondition.
	badSpec := proto.Clone(afterSpec).(*ateapipb.Actor)
	badSpec.ActorTemplate = &ateapipb.ObjectRef{Atespace: "ns", Name: "tmpl-2"}
	if _, err := adm.UpdateActorSpec(ctx, badSpec); !errors.Is(err, ErrFailedPrecondition) || !strings.Contains(err.Error(), "SUSPENDED") {
		t.Errorf("UpdateActorSpec template change while RUNNING = %v, want ErrFailedPrecondition mentioning SUSPENDED", err)
	}
}
