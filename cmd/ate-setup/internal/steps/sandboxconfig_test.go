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

package steps

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
)

func TestSandboxConfigName(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    string
		wantErr bool
	}{
		{name: "release tag", version: "v0.4.2", want: "substrate-v0.4.2-gvisor"},
		{name: "git describe of a dirty checkout", version: "v0.4.2-12-gabc1234-dirty", want: "substrate-v0.4.2-12-gabc1234-dirty-gvisor"},
		{name: "upper case and build metadata", version: "V1.0.0+Build", want: "substrate-v1.0.0-build-gvisor"},
		{name: "no git", version: "dev", want: "substrate-dev-gvisor"},
		{name: "dot next to a dash", version: "v1.", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sandboxConfigName(tt.version, atev1.SandboxClassGvisor)
			if (err != nil) != tt.wantErr {
				t.Fatalf("sandboxConfigName(%q) error = %v, wantErr %v", tt.version, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("sandboxConfigName(%q) = %q, want %q", tt.version, got, tt.want)
			}
		})
	}
}

func TestCheckClassDefault(t *testing.T) {
	const name = "substrate-v2-gvisor"
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	config := func(name string, class atev1.SandboxClass, created time.Time, isDefault, deleting bool) atev1.SandboxConfig {
		sc := atev1.SandboxConfig{
			ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.NewTime(created)},
			Spec:       atev1.SandboxConfigSpec{SandboxClass: class},
		}
		if isDefault {
			sc.Annotations = map[string]string{atev1.SandboxConfigClassDefaultAnnotation: "true"}
		}
		if deleting {
			sc.DeletionTimestamp = &metav1.Time{Time: created.Add(time.Hour)}
		}
		return sc
	}
	gvisor, microvm := atev1.SandboxClassGvisor, atev1.SandboxClassMicroVM

	tests := []struct {
		name    string
		configs []atev1.SandboxConfig
		// wantWarning is a substring of the warning, empty for none.
		wantWarning string
		wantErr     bool
	}{
		{
			name:        "fresh cluster",
			wantWarning: "no SandboxConfig",
		},
		{
			name:    "the only config, annotated",
			configs: []atev1.SandboxConfig{config(name, gvisor, t0, true, false)},
		},
		{
			name:        "the only config, not annotated",
			configs:     []atev1.SandboxConfig{config(name, gvisor, t0, false, false)},
			wantWarning: "no SandboxConfig",
		},
		{
			name: "not annotated with an existing default",
			configs: []atev1.SandboxConfig{
				config("substrate-v1-gvisor", gvisor, t0, true, false),
				config(name, gvisor, t0.Add(time.Hour), false, false),
			},
		},
		{
			name: "not annotated when only another class has a default",
			configs: []atev1.SandboxConfig{
				config("substrate-v1-microvm", microvm, t0, true, false),
				config(name, gvisor, t0.Add(time.Hour), false, false),
			},
			wantWarning: "no SandboxConfig",
		},
		{
			name: "not annotated when the only default is being deleted",
			configs: []atev1.SandboxConfig{
				config("substrate-v1-gvisor", gvisor, t0, true, true),
				config(name, gvisor, t0.Add(time.Hour), false, false),
			},
			wantWarning: "no SandboxConfig",
		},
		{
			name: "annotated over an older default",
			configs: []atev1.SandboxConfig{
				config("substrate-v1-gvisor", gvisor, t0, true, false),
				config(name, gvisor, t0.Add(time.Hour), true, false),
			},
		},
		{
			name: "annotated, older than the newest of two newer defaults",
			configs: []atev1.SandboxConfig{
				config(name, gvisor, t0, true, false),
				config("substrate-v3-gvisor", gvisor, t0.Add(time.Hour), true, false),
				config("substrate-v4-gvisor", gvisor, t0.Add(2*time.Hour), true, false),
			},
			wantWarning: "because substrate-v4-gvisor is newer",
		},
		{
			name: "annotated, created in the same second as a default with a smaller name",
			configs: []atev1.SandboxConfig{
				config("substrate-v1-gvisor", gvisor, t0, true, false),
				config(name, gvisor, t0, true, false),
			},
			wantWarning: "because substrate-v1-gvisor is newer",
		},
		{
			name: "not annotated, older than a newer default",
			configs: []atev1.SandboxConfig{
				config(name, gvisor, t0, false, false),
				config("substrate-v3-gvisor", gvisor, t0.Add(time.Hour), true, false),
			},
		},
		{
			name: "annotated, older than a newer default being deleted",
			configs: []atev1.SandboxConfig{
				config(name, gvisor, t0, true, false),
				config("substrate-v3-gvisor", gvisor, t0.Add(time.Hour), true, true),
			},
		},
		{
			name: "annotated, older than a newer default of another class",
			configs: []atev1.SandboxConfig{
				config(name, gvisor, t0, true, false),
				config("substrate-v3-microvm", microvm, t0.Add(time.Hour), true, false),
			},
		},
		{
			name:    "being deleted, annotated",
			configs: []atev1.SandboxConfig{config(name, gvisor, t0, true, true)},
			wantErr: true,
		},
		{
			name: "being deleted, not annotated",
			configs: []atev1.SandboxConfig{
				config("substrate-v1-gvisor", gvisor, t0, true, false),
				config(name, gvisor, t0.Add(time.Hour), false, true),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warning, err := checkClassDefault(tt.configs, name, gvisor)
			if (err != nil) != tt.wantErr {
				t.Fatalf("checkClassDefault() error = %v, wantErr %v", err, tt.wantErr)
			}
			if (warning == "") != (tt.wantWarning == "") || !strings.Contains(warning, tt.wantWarning) {
				t.Errorf("checkClassDefault() warning = %q, want it to contain %q", warning, tt.wantWarning)
			}
		})
	}
}
