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
	"context"
	"errors"
	"testing"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/googleapis/gax-go/v2"
)

// fakeClient is a Secret Manager client that records whether it was closed.
type fakeClient struct {
	name   string
	closed bool
}

func (f *fakeClient) AccessSecretVersion(context.Context, *secretmanagerpb.AccessSecretVersionRequest, ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	return nil, errors.New("not used")
}

func (f *fakeClient) Close() error {
	f.closed = true
	return nil
}

// The global client serves ""; each region's client is made once, on first
// use; a failed creation is retried; Close closes every client.
func TestClientPool(t *testing.T) {
	global := &fakeClient{name: "global"}
	made := map[string]int{}
	var failNext bool
	var regional []*fakeClient
	p := &clientPool{
		global: global,
		newRegional: func(location string) (secretManagerClient, error) {
			made[location]++
			if failNext {
				failNext = false
				return nil, errors.New("cannot create client")
			}
			c := &fakeClient{name: location}
			regional = append(regional, c)
			return c, nil
		},
	}

	for _, tc := range []struct {
		location string
		want     string
	}{
		{"", "global"},
		{"us-central1", "us-central1"},
		{"us-central1", "us-central1"},
		{"europe-west4", "europe-west4"},
	} {
		c, err := p.For(tc.location)
		if err != nil {
			t.Fatalf("For(%q) error = %v", tc.location, err)
		}
		if got := c.(*fakeClient).name; got != tc.want {
			t.Errorf("For(%q) = client %q, want %q", tc.location, got, tc.want)
		}
	}
	if made["us-central1"] != 1 || made["europe-west4"] != 1 || made[""] != 0 {
		t.Errorf("clients made per location = %v, want one per region and none for global", made)
	}

	failNext = true
	if _, err := p.For("asia-east1"); err == nil {
		t.Fatal("For(asia-east1) succeeded while client creation fails, want an error")
	}
	if _, err := p.For("asia-east1"); err != nil {
		t.Fatalf("For(asia-east1) after a failed attempt: %v, want a fresh client", err)
	}
	if made["asia-east1"] != 2 {
		t.Errorf("clients made for asia-east1 = %d, want 2: a failure is not remembered", made["asia-east1"])
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !global.closed {
		t.Error("Close() left the global client open")
	}
	for _, c := range regional {
		if !c.closed {
			t.Errorf("Close() left the %s client open", c.name)
		}
	}
}

func TestRegionalEndpoint(t *testing.T) {
	if got, want := regionalEndpoint("us-central1"), "secretmanager.us-central1.rep.googleapis.com:443"; got != want {
		t.Errorf("regionalEndpoint(us-central1) = %q, want %q", got, want)
	}
}
