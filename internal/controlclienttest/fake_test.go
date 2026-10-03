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

package controlclienttest

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

func TestFakeDefaultsToZeroValueSuccess(t *testing.T) {
	f := &Fake{}
	ctx := context.Background()

	if _, err := f.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{}); err != nil {
		t.Errorf("CreateAtespace() error = %v, want nil", err)
	}
	if _, err := f.ResumeActor(ctx, &ateapipb.ResumeActorRequest{}); err != nil {
		t.Errorf("ResumeActor() error = %v, want nil", err)
	}
	if want := []string{"CreateAtespace", "ResumeActor"}; !reflect.DeepEqual(f.RecordedCalls(), want) {
		t.Errorf("RecordedCalls() = %v, want %v", f.RecordedCalls(), want)
	}
}

func TestFakeUsesFuncOverride(t *testing.T) {
	wantErr := errors.New("boom")
	f := &Fake{
		ResumeActorFunc: func(context.Context, *ateapipb.ResumeActorRequest, ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			return nil, wantErr
		},
	}

	_, err := f.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{})
	if !errors.Is(err, wantErr) {
		t.Errorf("ResumeActor() error = %v, want %v", err, wantErr)
	}
}

func TestFakeRecordsDeleteActorRequests(t *testing.T) {
	f := &Fake{}
	req := &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Name: "a"}}

	if _, err := f.DeleteActor(context.Background(), req); err != nil {
		t.Fatalf("DeleteActor() error = %v", err)
	}

	got := f.RecordedDeleteActorRequests()
	if len(got) != 1 || got[0] != req {
		t.Errorf("RecordedDeleteActorRequests() = %v, want [%v]", got, req)
	}
}

func TestSequenceDrainsThenSucceeds(t *testing.T) {
	err1 := errors.New("first")
	err2 := errors.New("second")
	next := Sequence(err1, err2)

	if got := next(); got != err1 {
		t.Errorf("next() = %v, want %v", got, err1)
	}
	if got := next(); got != err2 {
		t.Errorf("next() = %v, want %v", got, err2)
	}
	if got := next(); got != nil {
		t.Errorf("next() = %v, want nil once drained", got)
	}
}
