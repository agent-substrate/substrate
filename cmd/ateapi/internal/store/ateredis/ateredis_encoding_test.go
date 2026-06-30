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

// Tests for the binary-protobuf record encoding (issue #307). These cover the
// behaviour specific to the encoding switch: that records are stored as binary
// protobuf rather than protojson, and that the read paths reject empty/corrupt/
// identity-mismatched values. The latter matters because, unlike protojson,
// proto.Unmarshal accepts an empty byte slice and yields a zero-valued message
// without error, so the key-identity checks are what catch a blank record.
package ateredis

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestCreateActor_StoresBinaryProtobuf locks in the binary-protobuf encoding so
// an accidental revert to protojson (which would still round-trip through the
// store API and pass the other tests) is caught: protojson values start with
// '{', binary protobuf ones do not.
func TestCreateActor_StoresBinaryProtobuf(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	actor := &ateapipb.Actor{
		ActorId:                "session-1",
		Atespace:               "ns1",
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "test-template",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}
	if err := s.CreateActor(ctx, actor); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	raw, err := s.rdb.Get(ctx, actorDBKey(actor.GetAtespace(), actor.GetActorId())).Bytes()
	if err != nil {
		t.Fatalf("raw Get failed: %v", err)
	}
	if len(raw) > 0 && raw[0] == '{' {
		t.Errorf("stored actor value looks like JSON, expected binary protobuf: %q", raw)
	}
	if err := proto.Unmarshal(raw, &ateapipb.Actor{}); err != nil {
		t.Errorf("stored actor value is not valid binary protobuf: %v", err)
	}
}

// TestCreateWorker_StoresBinaryProtobuf is the Worker counterpart.
func TestCreateWorker_StoresBinaryProtobuf(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	worker := &ateapipb.Worker{WorkerNamespace: "default", WorkerPool: "pool-1", WorkerPod: "pod-1"}
	if err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	raw, err := s.rdb.Get(ctx, workerDBKey("default", "pool-1", "pod-1")).Bytes()
	if err != nil {
		t.Fatalf("raw Get failed: %v", err)
	}
	if len(raw) > 0 && raw[0] == '{' {
		t.Errorf("stored worker value looks like JSON, expected binary protobuf: %q", raw)
	}
	if err := proto.Unmarshal(raw, &ateapipb.Worker{}); err != nil {
		t.Errorf("stored worker value is not valid binary protobuf: %v", err)
	}
}

func TestGetActor_RejectsEmptyValue(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if err := s.rdb.Set(ctx, actorDBKey("ns1", "ghost"), "", 0).Err(); err != nil {
		t.Fatalf("seeding empty value failed: %v", err)
	}
	if _, err := s.GetActor(ctx, "ns1", "ghost"); err == nil {
		t.Errorf("expected GetActor to reject an empty value, got nil")
	}
}

func TestGetWorker_RejectsEmptyValue(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if err := s.rdb.Set(ctx, workerDBKey("ns1", "pool1", "ghost"), "", 0).Err(); err != nil {
		t.Fatalf("seeding empty value failed: %v", err)
	}
	if _, err := s.GetWorker(ctx, "ns1", "pool1", "ghost"); err == nil {
		t.Errorf("expected GetWorker to reject an empty value, got nil")
	}
}

func TestListActors_RejectsEmptyValue(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if err := s.rdb.Set(ctx, actorDBKey("ns1", "ghost"), "", 0).Err(); err != nil {
		t.Fatalf("seeding empty value failed: %v", err)
	}
	if _, _, err := s.ListActors(ctx, "ns1", 1000, ""); err == nil {
		t.Errorf("expected ListActors to reject an empty-value actor key, got nil")
	}
}

func TestListActors_RejectsIdentityMismatch(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	// A valid Actor proto, but stored under a key with a different actor id.
	bytes, err := proto.Marshal(&ateapipb.Actor{
		ActorId: "real", Atespace: "ns1", ActorTemplateNamespace: "ns1", ActorTemplateName: "tmpl1", Version: 1,
	})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if err := s.rdb.Set(ctx, actorDBKey("ns1", "wrong"), bytes, 0).Err(); err != nil {
		t.Fatalf("seeding mismatched actor failed: %v", err)
	}
	if _, _, err := s.ListActors(ctx, "ns1", 1000, ""); err == nil {
		t.Errorf("expected ListActors to reject an identity-mismatched actor, got nil")
	}
}

func TestListWorkers_RejectsEmptyValue(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	if err := s.rdb.Set(ctx, workerDBKey("ns1", "pool1", "ghost"), "", 0).Err(); err != nil {
		t.Fatalf("seeding empty value failed: %v", err)
	}
	if _, err := s.ListWorkers(ctx); err == nil {
		t.Errorf("expected ListWorkers to reject an empty-value worker key, got nil")
	}
}

func TestListWorkers_RejectsIdentityMismatch(t *testing.T) {
	mr, s, ctx := setupTest(t)
	defer mr.Close()

	// A valid Worker proto, but stored under a key with a different pod.
	bytes, err := proto.Marshal(&ateapipb.Worker{
		WorkerNamespace: "ns1", WorkerPool: "pool1", WorkerPod: "real", Version: 1,
	})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if err := s.rdb.Set(ctx, workerDBKey("ns1", "pool1", "wrong"), bytes, 0).Err(); err != nil {
		t.Fatalf("seeding mismatched worker failed: %v", err)
	}
	if _, err := s.ListWorkers(ctx); err == nil {
		t.Errorf("expected ListWorkers to reject an identity-mismatched worker, got nil")
	}
}
