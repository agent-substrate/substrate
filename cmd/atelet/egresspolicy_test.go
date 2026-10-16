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

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestToAteletEgressPolicy(t *testing.T) {
	effects := &ateapipb.HttpRuleEffects{ReplaceHeaders: []*ateapipb.CredentialHeader{{
		Header:        "Authorization",
		CredentialUri: "secret://team-a/token",
	}}}
	for _, tc := range []struct {
		name string
		in   *ateapipb.EgressPolicy
		want *ateletpb.EgressPolicy
	}{{
		name: "no policy",
	}, {
		name: "no rules",
		in:   &ateapipb.EgressPolicy{Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default"}},
		want: &ateletpb.EgressPolicy{},
	}, {
		// Metadata and effects are the gateway's, and never reach the worker.
		name: "every protocol",
		in: &ateapipb.EgressPolicy{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default", Uid: "uid"},
			Rules: []*ateapipb.EgressRule{{
				Http: &ateapipb.HttpRule{
					Hostnames: []string{"api.example.com"},
					Ports:     &ateapipb.Ports{Numbers: []int32{80, 8080}},
					Effects:   effects,
				},
			}, {
				Https: &ateapipb.HttpsRule{
					Hostnames: []string{"*.example.com"},
					Ports:     &ateapipb.Ports{All: &ateapipb.AllPorts{}},
					Effects:   effects,
				},
			}, {
				TlsPassthrough: &ateapipb.TlsPassthroughRule{
					Hostnames: []string{"db.example.com", "cache.example.com"},
					Ports:     &ateapipb.Ports{Numbers: []int32{5432}},
				},
			}},
		},
		want: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
			Http: &ateletpb.EgressDestinations{
				Hostnames: []string{"api.example.com"},
				Ports:     &ateletpb.EgressPorts{Numbers: []int32{80, 8080}},
			},
		}, {
			Https: &ateletpb.EgressDestinations{
				Hostnames: []string{"*.example.com"},
				Ports:     &ateletpb.EgressPorts{All: &ateletpb.EgressAllPorts{}},
			},
		}, {
			TlsPassthrough: &ateletpb.EgressDestinations{
				Hostnames: []string{"db.example.com", "cache.example.com"},
				Ports:     &ateletpb.EgressPorts{Numbers: []int32{5432}},
			},
		}}},
	}, {
		name: "rule without ports",
		in: &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{
			TlsPassthrough: &ateapipb.TlsPassthroughRule{Hostnames: []string{"db.example.com"}},
		}}},
		want: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
			TlsPassthrough: &ateletpb.EgressDestinations{Hostnames: []string{"db.example.com"}},
		}}},
	}, {
		// A rule of a protocol the copy does not carry keeps its place, and
		// allows nothing.
		name: "rule without a protocol",
		in:   &ateapipb.EgressPolicy{Rules: []*ateapipb.EgressRule{{}}},
		want: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{}}},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			got := toAteletEgressPolicy(tc.in)
			if (got == nil) != (tc.want == nil) {
				t.Fatalf("toAteletEgressPolicy() = %v, want %v", got, tc.want)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("toAteletEgressPolicy() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
