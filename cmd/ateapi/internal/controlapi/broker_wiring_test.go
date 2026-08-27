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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/storagebroker"
)

// fakeBroker records the URIs it is asked to sign and returns a capability whose
// tokens embed the URI, so a test can tell entries apart.
type fakeBroker struct {
	err         error
	mintedRead  []string
	mintedWrite []string
}

func (f *fakeBroker) MintRead(_ context.Context, uri string, _ time.Duration) (storagebroker.Capability, error) {
	if f.err != nil {
		return storagebroker.Capability{}, f.err
	}
	f.mintedRead = append(f.mintedRead, uri)
	return storagebroker.Capability{PrefixURL: "https://read/" + uri, ReadToken: "rt-" + uri}, nil
}

func (f *fakeBroker) MintWrite(_ context.Context, uri string, _ time.Duration) (storagebroker.Capability, error) {
	if f.err != nil {
		return storagebroker.Capability{}, f.err
	}
	f.mintedWrite = append(f.mintedWrite, uri)
	return storagebroker.Capability{
		PrefixURL:   "https://write/" + uri,
		WriteMethod: storagebroker.WriteMethodPUT,
		WriteToken:  "wt-" + uri,
	}, nil
}

func TestReadAccessFor_MintsOnePerURI(t *testing.T) {
	fb := &fakeBroker{}
	w := &ActorWorkflow{broker: fb}

	// The empty URI (an unset golden) must be skipped, not signed.
	m, err := w.readAccessFor(context.Background(), "s3://bucket/actor", "s3://bucket/golden", "")
	if err != nil {
		t.Fatalf("readAccessFor: %v", err)
	}
	if len(m) != 2 {
		t.Fatalf("want 2 capabilities, got %d: %v", len(m), m)
	}
	if got := m["s3://bucket/actor"].GetReadToken(); got != "rt-s3://bucket/actor" {
		t.Errorf("actor read token = %q, want rt-s3://bucket/actor", got)
	}
	if got := m["s3://bucket/golden"].GetReadToken(); got != "rt-s3://bucket/golden" {
		t.Errorf("golden read token = %q, want rt-s3://bucket/golden", got)
	}
	if _, ok := m[""]; ok {
		t.Errorf("empty URI must not be signed")
	}
}

func TestReadAccessFor_DedupsRepeatedURI(t *testing.T) {
	fb := &fakeBroker{}
	w := &ActorWorkflow{broker: fb}

	m, err := w.readAccessFor(context.Background(), "s3://bucket/a", "s3://bucket/a")
	if err != nil {
		t.Fatalf("readAccessFor: %v", err)
	}
	if len(m) != 1 {
		t.Fatalf("want 1 capability, got %d", len(m))
	}
	if len(fb.mintedRead) != 1 {
		t.Errorf("broker minted %d times, want 1 (deduped)", len(fb.mintedRead))
	}
}

func TestReadWriteAccessFor_NilBrokerReturnsNil(t *testing.T) {
	w := &ActorWorkflow{} // no broker configured

	if m, err := w.readAccessFor(context.Background(), "s3://bucket/a"); err != nil || m != nil {
		t.Errorf("readAccessFor with no broker = (%v, %v), want (nil, nil)", m, err)
	}
	if m, err := w.writeAccessFor(context.Background(), "s3://bucket/a"); err != nil || m != nil {
		t.Errorf("writeAccessFor with no broker = (%v, %v), want (nil, nil)", m, err)
	}
}

func TestAccessFor_AllEmptyReturnsNil(t *testing.T) {
	w := &ActorWorkflow{broker: &fakeBroker{}}

	m, err := w.readAccessFor(context.Background(), "", "")
	if err != nil {
		t.Fatalf("readAccessFor: %v", err)
	}
	if m != nil {
		t.Errorf("want nil map when nothing to sign, got %v", m)
	}
}

func TestWriteAccessFor_MintsWriteCapability(t *testing.T) {
	fb := &fakeBroker{}
	w := &ActorWorkflow{broker: fb}

	m, err := w.writeAccessFor(context.Background(), "s3://bucket/dest")
	if err != nil {
		t.Fatalf("writeAccessFor: %v", err)
	}
	sa := m["s3://bucket/dest"]
	if sa.GetWriteMethod() != storagebroker.WriteMethodPUT || sa.GetWriteToken() != "wt-s3://bucket/dest" {
		t.Errorf("write capability = %+v, want PUT with wt-s3://bucket/dest", sa)
	}
}

func TestAccessFor_PropagatesMintError(t *testing.T) {
	w := &ActorWorkflow{broker: &fakeBroker{err: errors.New("boom")}}

	if _, err := w.readAccessFor(context.Background(), "s3://bucket/a"); err == nil {
		t.Errorf("readAccessFor: want error, got nil")
	}
	if _, err := w.writeAccessFor(context.Background(), "s3://bucket/a"); err == nil {
		t.Errorf("writeAccessFor: want error, got nil")
	}
}
