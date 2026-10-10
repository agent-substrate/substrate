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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// fakeSandboxConfigs is an in-memory dynamic.ResourceInterface implementing
// the calls releaseSandboxConfigs makes; any other call panics.
type fakeSandboxConfigs struct {
	dynamic.ResourceInterface
	objs map[string]*unstructured.Unstructured
	// listErr, when set, is returned by List instead of objs.
	listErr error
	// gone are names List returns that Get then reports NotFound, as if the
	// SandboxConfig was deleted between the two calls.
	gone []string
	// conflicts is the number of Update calls to reject with Conflict before
	// accepting the write.
	conflicts int
	updates   int
}

var sandboxConfigsGR = atev1alpha1.GroupVersion.WithResource("sandboxconfigs").GroupResource()

func (f *fakeSandboxConfigs) List(context.Context, metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	list := &unstructured.UnstructuredList{}
	for _, o := range f.objs {
		list.Items = append(list.Items, *o.DeepCopy())
	}
	for _, name := range f.gone {
		list.Items = append(list.Items, *newSandboxConfig(name, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer))
	}
	return list, nil
}

func (f *fakeSandboxConfigs) Get(_ context.Context, name string, _ metav1.GetOptions, _ ...string) (*unstructured.Unstructured, error) {
	o, ok := f.objs[name]
	if !ok {
		return nil, apierrors.NewNotFound(sandboxConfigsGR, name)
	}
	return o.DeepCopy(), nil
}

func (f *fakeSandboxConfigs) Update(_ context.Context, obj *unstructured.Unstructured, _ metav1.UpdateOptions, _ ...string) (*unstructured.Unstructured, error) {
	f.updates++
	if f.conflicts > 0 {
		f.conflicts--
		return nil, apierrors.NewConflict(sandboxConfigsGR, obj.GetName(), errors.New("stale resourceVersion"))
	}
	f.objs[obj.GetName()] = obj.DeepCopy()
	return obj, nil
}

func newSandboxConfig(name string, finalizers ...string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(atev1alpha1.GroupVersion.WithKind("SandboxConfig"))
	u.SetName(name)
	u.SetFinalizers(finalizers)
	return u
}

func TestReleaseSandboxConfigs(t *testing.T) {
	const inUse = atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer
	tests := []struct {
		name string
		res  *fakeSandboxConfigs
		// wantFinalizers is the expected finalizers of every object left in
		// res.objs after the call.
		wantFinalizers map[string][]string
		wantUpdates    int
		wantErr        bool
	}{
		{
			name: "strips only the protection finalizer and skips configs without it",
			res: &fakeSandboxConfigs{objs: map[string]*unstructured.Unstructured{
				"in-use": newSandboxConfig("in-use", inUse, "example.com/other"),
				"unused": newSandboxConfig("unused", "example.com/other"),
			}},
			wantFinalizers: map[string][]string{
				"in-use": {"example.com/other"},
				"unused": {"example.com/other"},
			},
			wantUpdates: 1,
		},
		{
			name: "List NotFound means the CRD is gone and is not an error",
			res: &fakeSandboxConfigs{
				listErr: apierrors.NewNotFound(sandboxConfigsGR, ""),
			},
		},
		{
			name: "List failure other than NotFound is returned",
			res: &fakeSandboxConfigs{
				listErr: apierrors.NewServiceUnavailable("apiserver down"),
			},
			wantErr: true,
		},
		{
			name: "config deleted between List and Get is skipped and the rest are released",
			res: &fakeSandboxConfigs{
				objs: map[string]*unstructured.Unstructured{
					"in-use": newSandboxConfig("in-use", inUse),
				},
				gone: []string{"vanished"},
			},
			wantFinalizers: map[string][]string{"in-use": {}},
			wantUpdates:    1,
		},
		{
			name: "Update conflict is retried until the write lands",
			res: &fakeSandboxConfigs{
				objs: map[string]*unstructured.Unstructured{
					"in-use": newSandboxConfig("in-use", inUse, "example.com/other"),
				},
				conflicts: 2,
			},
			wantFinalizers: map[string][]string{"in-use": {"example.com/other"}},
			wantUpdates:    3,
		},
		{
			name: "Update conflicts beyond the retry budget are returned",
			res: &fakeSandboxConfigs{
				objs: map[string]*unstructured.Unstructured{
					"in-use": newSandboxConfig("in-use", inUse),
				},
				conflicts: retry.DefaultRetry.Steps,
			},
			wantFinalizers: map[string][]string{"in-use": {inUse}},
			wantUpdates:    retry.DefaultRetry.Steps,
			wantErr:        true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := releaseSandboxConfigs(t.Context(), tt.res)
			if (err != nil) != tt.wantErr {
				t.Fatalf("releaseSandboxConfigs() error = %v, wantErr %v", err, tt.wantErr)
			}
			for name, want := range tt.wantFinalizers {
				obj, ok := tt.res.objs[name]
				if !ok {
					t.Fatalf("SandboxConfig %s missing from fake", name)
				}
				if got := obj.GetFinalizers(); !slices.Equal(got, want) {
					t.Errorf("SandboxConfig %s finalizers = %v, want %v", name, got, want)
				}
			}
			if tt.res.updates != tt.wantUpdates {
				t.Errorf("updates = %d, want %d", tt.res.updates, tt.wantUpdates)
			}
		})
	}
}

// recordingDeleter is a Deleter that appends its name to a log file shared
// with the fake microvm script, so a test can read back the order of calls.
type recordingDeleter struct {
	name string
	log  string
	err  error
}

func (d recordingDeleter) Delete(context.Context, *Env) error {
	appendLine(d.log, d.name)
	return d.err
}

func appendLine(path, line string) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fmt.Fprintln(f, line)
}

func TestDeleteDemos(t *testing.T) {
	demoErr := errors.New("demo failed")
	tests := []struct {
		name string
		// demos are the deleters in order; a nil entry's err is nil.
		demos []recordingDeleter
		// scriptExit is the exit code of the fake install-microvm-deps.sh.
		scriptExit int
		wantCalls  []string
		wantErr    error
	}{
		{
			name:      "microvm SandboxConfig is deleted after every demo",
			demos:     []recordingDeleter{{name: "demo-a"}, {name: "demo-b"}},
			wantCalls: []string{"demo-a", "demo-b", "script --delete"},
		},
		{
			name:       "script failure is logged, not returned",
			demos:      []recordingDeleter{{name: "demo-a"}},
			scriptExit: 1,
			wantCalls:  []string{"demo-a", "script --delete"},
		},
		{
			name:      "a failing demo stops before the script",
			demos:     []recordingDeleter{{name: "demo-a", err: demoErr}, {name: "demo-b"}},
			wantCalls: []string{"demo-a"},
			wantErr:   demoErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			calls := filepath.Join(root, "calls.log")
			script := filepath.Join(root, installMicrovmDepScript)
			if err := os.MkdirAll(filepath.Dir(script), 0o755); err != nil {
				t.Fatal(err)
			}
			body := fmt.Sprintf("#!/bin/sh\necho \"script $*\" >> %q\nexit %d\n", calls, tt.scriptExit)
			if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
				t.Fatal(err)
			}
			demos := make([]Deleter, 0, len(tt.demos))
			for _, d := range tt.demos {
				d.log = calls
				demos = append(demos, d)
			}
			e := &Env{Cfg: &config.Config{Root: root}}

			err := e.deleteDemos(t.Context(), demos)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("deleteDemos() error = %v, want %v", err, tt.wantErr)
			}
			got, _ := os.ReadFile(calls)
			gotCalls := strings.Fields(strings.ReplaceAll(strings.TrimSpace(string(got)), "script --delete", "script_--delete"))
			for i := range gotCalls {
				gotCalls[i] = strings.ReplaceAll(gotCalls[i], "script_--delete", "script --delete")
			}
			if !slices.Equal(gotCalls, tt.wantCalls) {
				t.Errorf("calls = %q, want %q", gotCalls, tt.wantCalls)
			}
		})
	}
}
