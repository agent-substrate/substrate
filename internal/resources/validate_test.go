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

package resources

import (
	"regexp"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestIsValidResourceName(t *testing.T) {
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{"valid lowercase", "my-actor-1", true},
		{"valid single char", "a", true},
		{"missing name", "", false},
		{"invalid uppercase", "My-Actor", false},
		{"invalid start hyphen", "-actor", false},
		{"valid start number", "1actor", true},
		{"invalid end hyphen", "actor-", false},
		{"invalid special chars", "actor@1", false},
		{"invalid length", strings.Repeat("a", 64), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidResourceName(tt.value); got != tt.valid {
				t.Errorf("IsValidResourceName(%q) = %v, want %v", tt.value, got, tt.valid)
			}
		})
	}
}

func TestValidateObjectRef(t *testing.T) {
	tests := []struct {
		name    string
		input   *ateapipb.ObjectRef
		wantMsg string
	}{{
		"missing atespace",
		&ateapipb.ObjectRef{Name: "id1"},
		"atespace: Required value",
	}, {
		"invalid atespace",
		&ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"},
		"atespace: Invalid value",
	}, {
		"missing name",
		&ateapipb.ObjectRef{Atespace: "ns1"},
		"name: Required value",
	}, {
		"invalid name",
		&ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"},
		"name: Invalid value",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidateObjectRef(tt.input, field.NewPath("path"))
			if len(errs) == 0 {
				t.Fatalf("expected 1 error, got 0")
			}
			if len(errs) > 1 {
				t.Fatalf("expected 1 error, got %v", errs)
			}
			err := errs[0]
			got := err.Error()
			if matched, matchErr := regexp.MatchString(tt.wantMsg, got); matchErr != nil {
				t.Fatalf("failed to compile regex %q: %v", tt.wantMsg, matchErr)
			} else if !matched {
				t.Errorf("expected message %q, got %q", tt.wantMsg, got)
			}
		})
	}
}

func TestValidateGlobalObjectRef(t *testing.T) {
	tests := []struct {
		name    string
		input   *ateapipb.ObjectRef
		wantMsg string // empty means no error is expected
	}{{
		"valid global ref",
		&ateapipb.ObjectRef{Name: "team-a"},
		"",
	}, {
		"atespace must be empty",
		&ateapipb.ObjectRef{Atespace: "ns1", Name: "team-a"},
		"atespace: Invalid value",
	}, {
		"missing name",
		&ateapipb.ObjectRef{},
		"name: Required value",
	}, {
		"invalid name",
		&ateapipb.ObjectRef{Name: "TEAM-A"},
		"name: Invalid value",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidateGlobalObjectRef(tt.input, field.NewPath("path"))
			if tt.wantMsg == "" {
				if len(errs) != 0 {
					t.Fatalf("expected no errors, got %v", errs)
				}
				return
			}
			if len(errs) != 1 {
				t.Fatalf("expected 1 error, got %v", errs)
			}
			got := errs[0].Error()
			if matched, matchErr := regexp.MatchString(tt.wantMsg, got); matchErr != nil {
				t.Fatalf("failed to compile regex %q: %v", tt.wantMsg, matchErr)
			} else if !matched {
				t.Errorf("expected message %q, got %q", tt.wantMsg, got)
			}
		})
	}
}

func TestValidateAteomUID(t *testing.T) {
	tests := []struct {
		name    string
		uid     string
		wantErr bool
	}{
		{"uuid valid", "422938ba-8860-4983-a25d-d6bcb0a69d4e", false},
		{"separator", "a/b", true},
		{"traversal", "..", true},
		{"empty", "", true},
		{"uppercase", "Pod-UID", true},
		{"too long", strings.Repeat("a", 64), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateAteomUID(tt.uid); (err != nil) != tt.wantErr {
				t.Errorf("ValidateAteomUID(%q) err = %v, wantErr %v", tt.uid, err, tt.wantErr)
			}
		})
	}
}

func TestValidateContainerNames(t *testing.T) {
	tests := []struct {
		name    string
		names   []string
		wantErr bool
	}{
		{"no containers", nil, false},
		{"single valid", []string{"worker"}, false},
		{"multiple valid", []string{"worker", "sidecar"}, false},
		{"separator", []string{"a/b"}, true},
		{"traversal", []string{".."}, true},
		{"empty name", []string{""}, true},
		{"uppercase", []string{"Worker"}, true},
		{"reserved pause", []string{"pause"}, true},
		{"reserved pause among valid", []string{"worker", "pause"}, true},
		{"duplicate", []string{"worker", "worker"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateContainerNames(tt.names); (err != nil) != tt.wantErr {
				t.Errorf("ValidateContainerNames(%v) err = %v, wantErr %v", tt.names, err, tt.wantErr)
			}
		})
	}
}

func TestValidateRunscHash(t *testing.T) {
	const valid = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	tests := []struct {
		name    string
		hash    string
		wantErr bool
	}{
		{"valid lowercase", valid, false},
		{"valid uppercase", strings.ToUpper(valid), false},
		{"empty", "", true},
		{"too short", "abc123", true},
		{"too long", valid + "00", true},
		{"separator", strings.Repeat("a", 60) + "/../", true},
		{"non-hex", strings.Repeat("g", 64), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateRunscHash(tt.hash); (err != nil) != tt.wantErr {
				t.Errorf("ValidateRunscHash(%q) err = %v, wantErr %v", tt.hash, err, tt.wantErr)
			}
		})
	}
}

func TestValidateSnapshotURIPrefix(t *testing.T) {
	tests := []struct {
		name    string
		prefix  string
		wantErr bool
	}{
		{"valid with trailing slash", "gs://bucket/actors/1234/snapshots/5678/", false},
		{"valid without path", "gs://bucket", false},
		// Scheme is storage-backend policy, not validated here.
		{"valid alternate scheme", "s3://bucket/path", false},
		{"empty", "", true},
		{"missing bucket", "gs://", true},
		{"no scheme or bucket", "bucket/path", true},
		{"unparseable", "://bucket", true},
		// Appended object names must not be swallowed by URL components.
		{"query", "gs://bucket/path?x=1", true},
		{"fragment", "gs://bucket/path#frag", true},
		{"userinfo", "gs://user@bucket/path", true},
		// Opaque form (no //) parses with an empty host, so it is rejected
		// on either the bucket or the opaque check.
		{"opaque", "gs:bucket/path", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateSnapshotURIPrefix(tt.prefix); (err != nil) != tt.wantErr {
				t.Errorf("ValidateSnapshotURIPrefix(%q) err = %v, wantErr %v", tt.prefix, err, tt.wantErr)
			}
		})
	}
}

func TestValidateWorker(t *testing.T) {
	tests := []struct {
		name    string
		worker  *ateapipb.Worker
		wantMsg string // empty means valid
	}{{
		name: "valid unassigned worker",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Ip:              "10.0.0.1",
			WorkerPodUid:    "123e4567-e89b-12d3-a456-426614174000",
			NodeName:        "node-1.example.com",
		},
		wantMsg: "",
	}, {
		name: "valid assigned worker",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Assignment: &ateapipb.Assignment{
				ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
					Namespace: "actor-ns",
					Name:      "actor-template",
				},
				Actor: &ateapipb.ObjectRef{
					Name:     "actor-id",
					Atespace: "actor-atespace",
				},
			},
			Ip:           "10.0.0.1",
			WorkerPodUid: "123e4567-e89b-12d3-a456-426614174000",
			NodeName:     "node-1.example.com",
		},
		wantMsg: "",
	}, {
		name: "partially assigned worker, missing actor_template",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Assignment: &ateapipb.Assignment{
				Actor: &ateapipb.ObjectRef{
					Name:     "actor-id",
					Atespace: "actor-atespace",
				},
			},
			Ip:           "10.0.0.1",
			WorkerPodUid: "123e4567-e89b-12d3-a456-426614174000",
			NodeName:     "node-1.example.com",
		},
		wantMsg: "worker.assignment.actor_template: Required value",
	}, {
		name: "partially assigned worker, missing actor_template.namespace",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Assignment: &ateapipb.Assignment{
				ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
					Name: "actor-template",
				},
				Actor: &ateapipb.ObjectRef{
					Name:     "actor-id",
					Atespace: "actor-atespace",
				},
			},
			Ip:           "10.0.0.1",
			WorkerPodUid: "123e4567-e89b-12d3-a456-426614174000",
			NodeName:     "node-1.example.com",
		},
		wantMsg: "worker.assignment.actor_template.namespace: Required value",
	}, {
		name: "partially assigned worker, missing actor_template.name",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Assignment: &ateapipb.Assignment{
				ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
					Namespace: "actor-ns",
				},
				Actor: &ateapipb.ObjectRef{
					Name:     "actor-id",
					Atespace: "actor-atespace",
				},
			},
			Ip:           "10.0.0.1",
			WorkerPodUid: "123e4567-e89b-12d3-a456-426614174000",
			NodeName:     "node-1.example.com",
		},
		wantMsg: "worker.assignment.actor_template.name: Required value",
	}, {
		name: "partially assigned worker, missing actor",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Assignment: &ateapipb.Assignment{
				ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
					Name:      "actor-template",
					Namespace: "actor-ns",
				},
			},
			Ip:           "10.0.0.1",
			WorkerPodUid: "123e4567-e89b-12d3-a456-426614174000",
			NodeName:     "node-1.example.com",
		},
		wantMsg: "worker.assignment.actor: Required value",
	}, {
		name: "partially assigned worker, missing actor.name",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Assignment: &ateapipb.Assignment{
				ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
					Name:      "actor-template",
					Namespace: "actor-ns",
				},
				Actor: &ateapipb.ObjectRef{
					Atespace: "actor-atespace",
				},
			},
			Ip:           "10.0.0.1",
			WorkerPodUid: "123e4567-e89b-12d3-a456-426614174000",
			NodeName:     "node-1.example.com",
		},
		wantMsg: "worker.assignment.actor.name: Required value",
	}, {
		name: "partially assigned worker, missing actor.atespace",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Assignment: &ateapipb.Assignment{
				ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
					Name:      "actor-template",
					Namespace: "actor-ns",
				},
				Actor: &ateapipb.ObjectRef{
					Name: "actor-id",
				},
			},
			Ip:           "10.0.0.1",
			WorkerPodUid: "123e4567-e89b-12d3-a456-426614174000",
			NodeName:     "node-1.example.com",
		},
		wantMsg: "worker.assignment.actor.atespace: Required value",
	}, {
		name: "missing worker_namespace",
		worker: &ateapipb.Worker{
			WorkerPool:   "pool-1",
			WorkerPod:    "pod-1",
			Ip:           "10.0.0.1",
			WorkerPodUid: "123e4567-e89b-12d3-a456-426614174000",
			NodeName:     "node-1",
		},
		wantMsg: "worker_namespace: Required value",
	}, {
		name: "invalid worker_namespace",
		worker: &ateapipb.Worker{
			WorkerNamespace: "NS-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Ip:              "10.0.0.1",
			WorkerPodUid:    "123e4567-e89b-12d3-a456-426614174000",
			NodeName:        "node-1",
		},
		wantMsg: "worker_namespace: Invalid value",
	}, {
		name: "missing ip",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			WorkerPodUid:    "123e4567-e89b-12d3-a456-426614174000",
			NodeName:        "node-1",
		},
		wantMsg: "ip: Required value",
	}, {
		name: "invalid ip",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Ip:              "not-an-ip",
			WorkerPodUid:    "123e4567-e89b-12d3-a456-426614174000",
			NodeName:        "node-1",
		},
		wantMsg: "ip: Invalid value",
	}, {
		name: "missing worker_pod_uid",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Ip:              "10.0.0.1",
			NodeName:        "node-1",
		},
		wantMsg: "worker_pod_uid: Required value",
	}, {
		name: "invalid worker_pod_uid",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Ip:              "10.0.0.1",
			WorkerPodUid:    "INVALID-UUID",
			NodeName:        "node-1",
		},
		wantMsg: "worker_pod_uid: Invalid value",
	}, {
		name: "missing node_name",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Ip:              "10.0.0.1",
			WorkerPodUid:    "123e4567-e89b-12d3-a456-426614174000",
		},
		wantMsg: "node_name: Required value",
	}, {
		name: "invalid node_name",
		worker: &ateapipb.Worker{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			Ip:              "10.0.0.1",
			WorkerPodUid:    "123e4567-e89b-12d3-a456-426614174000",
			NodeName:        "NODE_NAME",
		},
		wantMsg: "node_name: Invalid value",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidateWorker(tt.worker, field.NewPath("worker"))
			if tt.wantMsg == "" {
				if len(errs) > 0 {
					t.Fatalf("expected 0 errors, got %v", errs)
				}
			} else {
				if len(errs) == 0 {
					t.Fatalf("expected error matching %q, got 0", tt.wantMsg)
				}
				err := errs[0]
				got := err.Error()
				if matched, matchErr := regexp.MatchString(tt.wantMsg, got); matchErr != nil {
					t.Fatalf("failed to compile regex %q: %v", tt.wantMsg, matchErr)
				} else if !matched {
					t.Errorf("expected message matching %q, got %q", tt.wantMsg, got)
				}
			}
		})
	}
}

func TestValidateIP(t *testing.T) {
	tests := []struct {
		name    string
		ip      string
		wantMsg string // empty means valid
	}{
		{"valid ipv4", "192.168.1.1", ""},
		{"valid ipv6", "2001:db8::1", ""},
		{"invalid format", "not-an-ip", "must be a valid IP address"},
		{"ipv4-mapped ipv6", "::ffff:192.168.1.1", "must not be an IPv4-mapped IPv6 address"},
		{"non-canonical ipv6", "2001:db8:0:0:0:0:0:1", "must be in canonical form"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidateIP(tt.ip, field.NewPath("ip"))
			if tt.wantMsg == "" {
				if len(errs) > 0 {
					t.Fatalf("expected 0 errors, got %v", errs)
				}
			} else {
				if len(errs) == 0 {
					t.Fatalf("expected error matching %q, got 0", tt.wantMsg)
				}
				err := errs[0]
				got := err.Error()
				if matched, matchErr := regexp.MatchString(tt.wantMsg, got); matchErr != nil {
					t.Fatalf("failed to compile regex %q: %v", tt.wantMsg, matchErr)
				} else if !matched {
					t.Errorf("expected message matching %q, got %q", tt.wantMsg, got)
				}
			}
		})
	}
}

func TestValidateUUID(t *testing.T) {
	tests := []struct {
		name    string
		uuid    string
		wantMsg string // empty means valid
	}{
		{"valid", "123e4567-e89b-12d3-a456-426614174000", ""},
		{"too short", "123e4567", "must be a lowercase UUID"},
		{"too long", "123e4567-e89b-12d3-a456-4266141740001", "must be a lowercase UUID"},
		{"missing dashes", "123e4567e89b12d3a456426614174000", "must be a lowercase UUID"},
		{"uppercase hex", "123E4567-E89B-12D3-A456-426614174000", "must be a lowercase UUID"},
		{"invalid characters", "123e4567-e89b-12d3-a456-42661417400g", "must be a lowercase UUID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidateUUID(tt.uuid, field.NewPath("uuid"))
			if tt.wantMsg == "" {
				if len(errs) > 0 {
					t.Fatalf("expected 0 errors, got %v", errs)
				}
			} else {
				if len(errs) == 0 {
					t.Fatalf("expected error matching %q, got 0", tt.wantMsg)
				}
				err := errs[0]
				got := err.Error()
				if matched, matchErr := regexp.MatchString(tt.wantMsg, got); matchErr != nil {
					t.Fatalf("failed to compile regex %q: %v", tt.wantMsg, matchErr)
				} else if !matched {
					t.Errorf("expected message matching %q, got %q", tt.wantMsg, got)
				}
			}
		})
	}
}
