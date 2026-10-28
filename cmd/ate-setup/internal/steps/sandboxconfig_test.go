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
		name     string
		version  string
		manifest string
		want     string
		wantErr  bool
	}{
		{name: "release tag", version: "v0.4.2", manifest: "spec: a\n", want: "substrate-v0.4.2-gvisor"},
		{name: "release candidate in upper case", version: "v1.0.0-RC.1", manifest: "spec: a\n", want: "substrate-v1.0.0-rc.1-gvisor"},
		{name: "git describe of a dirty checkout", version: "v0.4.2-12-gabc1234-dirty", manifest: "spec: a\n", want: "substrate-dev-d5ee99cf6b-gvisor"},
		{name: "comment lines do not count", version: "v0.4.2-12-gabc1234-dirty", manifest: "# a\n  # b\nspec: a\n", want: "substrate-dev-d5ee99cf6b-gvisor"},
		{name: "another spec", version: "v0.4.2-12-gabc1234-dirty", manifest: "spec: b\n", want: "substrate-dev-22e34e826a-gvisor"},
		{name: "git describe of a release tag", version: "v0.4.2-3-gabc1234", manifest: "spec: a\n", want: "substrate-dev-d5ee99cf6b-gvisor"},
		{name: "dirty checkout of a release tag", version: "v0.4.2-dirty", manifest: "spec: a\n", want: "substrate-dev-d5ee99cf6b-gvisor"},
		{name: "moving image tag", version: "main", manifest: "spec: a\n", want: "substrate-dev-d5ee99cf6b-gvisor"},
		{name: "release candidate with an empty label", version: "v1.0.0-rc..1", manifest: "spec: a\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sandboxConfigName(tt.version, atev1.SandboxClassGvisor, []byte(tt.manifest))
			if (err != nil) != tt.wantErr {
				t.Fatalf("sandboxConfigName(%q) error = %v, wantErr %v", tt.version, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("sandboxConfigName(%q) = %q, want %q", tt.version, got, tt.want)
			}
		})
	}
}

func TestClassDefaultWarning(t *testing.T) {
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warning := classDefaultWarning(tt.configs, name, gvisor)
			if (warning == "") != (tt.wantWarning == "") || !strings.Contains(warning, tt.wantWarning) {
				t.Errorf("classDefaultWarning() = %q, want it to contain %q", warning, tt.wantWarning)
			}
		})
	}
}

func TestSandboxConfigExists(t *testing.T) {
	const name = "substrate-v2-gvisor"
	config := func(name string, deleting bool) atev1.SandboxConfig {
		sc := atev1.SandboxConfig{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if deleting {
			sc.DeletionTimestamp = &metav1.Time{Time: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
		}
		return sc
	}

	tests := []struct {
		name       string
		configs    []atev1.SandboxConfig
		wantExists bool
		wantErr    bool
	}{
		{name: "fresh cluster"},
		{name: "only other configs", configs: []atev1.SandboxConfig{config("substrate-v1-gvisor", false)}},
		{name: "exists", configs: []atev1.SandboxConfig{config(name, false)}, wantExists: true},
		{name: "being deleted", configs: []atev1.SandboxConfig{config(name, true)}, wantErr: true},
		{name: "another config being deleted", configs: []atev1.SandboxConfig{config("substrate-v1-gvisor", true)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exists, err := sandboxConfigExists(tt.configs, name)
			if (err != nil) != tt.wantErr {
				t.Fatalf("sandboxConfigExists() error = %v, wantErr %v", err, tt.wantErr)
			}
			if exists != tt.wantExists {
				t.Errorf("sandboxConfigExists() = %v, want %v", exists, tt.wantExists)
			}
		})
	}
}
