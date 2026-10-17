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
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type templateReadRaceStore struct {
	store.Interface
	afterRead func(*ateapipb.ActorTemplate)
}

func (s *templateReadRaceStore) GetActorTemplate(ctx context.Context, ref resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	tmpl, err := s.Interface.GetActorTemplate(ctx, ref)
	if err == nil {
		s.afterRead(tmpl)
	}
	return tmpl, err
}

func TestCreateActor_TemplateChangedAfterRead(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		name, wantCode := "deleted", codes.FailedPrecondition
		if recreate {
			name, wantCode = "recreated", codes.Aborted
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			storetest.MustCreateAtespace(t, ctx, persistence, "team-a")
			tmpl := seedSubstrateTemplate(t, ctx, persistence, "tmpl")
			ref := resources.ActorTemplateRefFromActorTemplate(tmpl)
			racingStore := &templateReadRaceStore{Interface: persistence, afterRead: func(read *ateapipb.ActorTemplate) {
				if _, err := persistence.DeleteActorTemplate(ctx, ref, store.DeletePreconditions{UID: read.GetMetadata().GetUid()}); err != nil {
					t.Fatal(err)
				}
				if recreate {
					replacement := seedSubstrateTemplate(t, ctx, persistence, "tmpl")
					if replacement.GetMetadata().GetUid() == read.GetMetadata().GetUid() {
						t.Fatal("replacement template has the old UID")
					}
				}
			}}
			svc := &ServiceImpl{store: racingStore}
			actor := &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor"},
				ActorTemplate: ref.ToObjectRef(),
			}
			if _, err := svc.CreateActor(ctx, actor); status.Code(err) != wantCode {
				t.Fatalf("CreateActor = %v, want %v", err, wantCode)
			} else if !recreate && !strings.Contains(err.Error(), "actor template not found") {
				t.Fatalf("CreateActor = %v, want the existing missing-template error", err)
			}
			if _, err := persistence.GetActor(ctx, resources.ActorRefFromActor(actor)); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("GetActor = %v, want ErrNotFound after rejected create", err)
			}
		})
	}
}
