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

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func resolvedWith(t *testing.T, env map[string]string) *Resolved {
	t.Helper()
	useFixtures(t)
	r, err := Resolve(nil, ResolveOptions{Env: env})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	return r
}

// A record is written so the next run can reproduce this one by pointing
// --config at it. If it needed editing first, it would not serve that purpose.
func TestRecordRoundTripsThroughConfig(t *testing.T) {
	r := resolvedWith(t, map[string]string{
		"FIXTURE_MODE":            "beta",
		"FIXTURE_NAME":            "a-name",
		"FIXTURE_COUNT":           "6",
		"FIXTURE_NESTED_DEEP_KEY": "buried",
	})

	dir := t.TempDir()
	path, _, err := RecordSuccess(dir, "prod", r)
	if err != nil {
		t.Fatalf("RecordSuccess() error = %v", err)
	}

	f, err := ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile(%s) = %v; the record is not valid --config input", path, err)
	}
	for key, want := range map[string]string{
		"fixture.mode":            "beta",
		"fixture.name":            "a-name",
		"fixture.count":           "6",
		"fixture.nested.deep.key": "buried",
	} {
		if got, ok := f.Get(key); !ok || got != want {
			t.Errorf("Get(%s) = %q, %v; want %q", key, got, ok, want)
		}
	}
}

// A record has to reproduce the run that wrote it. An empty flag is the case
// that breaks it: it resolves as supplied, so the record names the setting
// with an empty value, and reading that back resolves to the default instead.
func TestRecordRoundTripsAnEmptyFlag(t *testing.T) {
	const key, flag = "scratch.str", "scratch-str"
	useSettings(t, Setting{
		Key: key, Env: "SCRATCH_STR", Flag: flag,
		Kind: KindString, Default: "alpha", Usage: "a string",
	})

	r, err := Resolve(flagsWith(t, map[string]string{flag: ""}), ResolveOptions{})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	path, _, err := RecordSuccess(t.TempDir(), "prod", r)
	if err != nil {
		t.Fatalf("RecordSuccess() error = %v", err)
	}
	f, err := ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}
	back, err := Resolve(nil, ResolveOptions{File: f})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	if got, want := back.String(key), r.String(key); got != want {
		t.Errorf("the record resolves to %q, the run it recorded resolved to %q", got, want)
	}
	if got, want := back.Supplied(key), r.Supplied(key); got != want {
		t.Errorf("Supplied() = %v after the round trip, was %v", got, want)
	}
}

// Removing a setting from an installation means writing its zero value and
// re-applying, so a record has to be able to carry an empty string and a
// later run has to read it back as empty. A string that declares a default
// cannot be cleared this way today: the empty value is dropped on the way in,
// so the record never names the setting and the default returns.
func TestRecordCanClearAStringThatHasADefault(t *testing.T) {
	const key, env = "scratch.str", "SCRATCH_STR"
	useSettings(t, Setting{
		Key: key, Env: env, Flag: "scratch-str", Kind: KindString,
		Default: "alpha", Usage: "a string",
	})

	r, err := Resolve(nil, ResolveOptions{Env: map[string]string{env: ""}})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	path, _, err := RecordSuccess(t.TempDir(), "prod", r)
	if err != nil {
		t.Fatalf("RecordSuccess() error = %v", err)
	}
	f, err := ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}
	if _, ok := f.Get(key); !ok {
		t.Fatalf("the record does not name %s, so the cleared value is lost", key)
	}

	back, err := Resolve(nil, ResolveOptions{File: f})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got := back.String(key); got != "" {
		t.Errorf("replaying the record gives %q, want the cleared value", got)
	}
}

// Writing the defaults out would freeze them: a setting nobody chose has to
// keep tracking its declared default when a later release changes it.
func TestRecordOmitsSettingsNobodySupplied(t *testing.T) {
	r := resolvedWith(t, map[string]string{"FIXTURE_NAME": "a-name"})

	dir := t.TempDir()
	path, _, err := RecordSuccess(dir, "prod", r)
	if err != nil {
		t.Fatalf("RecordSuccess() error = %v", err)
	}
	f, err := ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile: %v", err)
	}
	if len(f.Keys()) != 1 {
		t.Errorf("record holds %v, want only the one setting that was supplied", f.Keys())
	}
	if _, ok := f.Get("fixture.mode"); ok {
		t.Error("record names fixture.mode, which nothing supplied")
	}
}

// A record may be committed to an operations repository, so a credential in
// one is a credential in version control.
func TestRecordNeverWritesASecret(t *testing.T) {
	useFixtures(t)
	env := map[string]string{}
	var secrets []string
	for _, s := range All() {
		if s.Secret {
			env[s.Env] = "SENSITIVE-" + s.Key
			secrets = append(secrets, s.Key)
		}
	}
	if len(secrets) == 0 {
		t.Fatal("no Secret settings registered; this test would pass vacuously")
	}

	dir := t.TempDir()
	path, omitted, err := RecordSuccess(dir, "prod", resolvedWith(t, env))
	if err != nil {
		t.Fatalf("RecordSuccess() error = %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if strings.Contains(string(raw), "SENSITIVE") {
		t.Errorf("record discloses a secret:\n%s", raw)
	}
	// Reported rather than dropped in silence: a retry from this file will
	// fail on the missing value, and the operator should learn that now.
	if len(omitted) != len(secrets) {
		t.Errorf("omitted = %v, want every secret setting %v", omitted, secrets)
	}
}

// Success is the current configuration of one cluster, so it replaces. Failure
// is one attempt among several, so it accumulates and the operator picks.
func TestSuccessReplacesAndFailureAccumulates(t *testing.T) {
	r := resolvedWith(t, nil)
	dir := t.TempDir()

	for range 2 {
		if _, _, err := RecordSuccess(dir, "prod", r); err != nil {
			t.Fatalf("RecordSuccess() error = %v", err)
		}
	}
	if got := countFiles(t, filepath.Join(dir, installsDir)); got != 1 {
		t.Errorf("%d success records, want 1 -- the second must replace the first", got)
	}

	first, _, err := RecordFailure(dir, "prod", r, "ate-setup deploy atenet")
	if err != nil {
		t.Fatalf("RecordFailure() error = %v", err)
	}
	second, _, err := RecordFailure(dir, "prod", r, "ate-setup deploy atenet")
	if err != nil {
		t.Fatalf("RecordFailure() error = %v", err)
	}
	if first == second {
		t.Errorf("both failures wrote %s; each attempt needs its own file", first)
	}
}

// Nothing reads these automatically, but the operator has to be able to find
// the right one, and a context name is not a safe path component.
func TestRecordNameIsPathSafe(t *testing.T) {
	for _, tc := range []struct {
		context string
		want    string
	}{
		{context: "kind-kind", want: "kind-kind"},
		{context: "gke_prod_us-central1-a_substrate", want: "gke_prod_us-central1-a_substrate"},
		{
			context: "arn:aws:eks:us-east-1:111122223333:cluster/prod",
			want:    "arn-aws-eks-us-east-1-111122223333-cluster-prod",
		},
		{context: "", want: unkeyedContext},
	} {
		t.Run(tc.context, func(t *testing.T) {
			if got := recordName(tc.context); got != tc.want {
				t.Errorf("recordName() = %q, want %q", got, tc.want)
			}
			if strings.ContainsAny(tc.want, `/\`) {
				t.Errorf("recordName() = %q, which escapes its directory", tc.want)
			}
		})
	}
}

func TestRecordFileIsNotWorldReadable(t *testing.T) {
	dir := t.TempDir()
	path, _, err := RecordSuccess(dir, "prod", resolvedWith(t, map[string]string{"FIXTURE_NAME": "x"}))
	if err != nil {
		t.Fatalf("RecordSuccess() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("record mode = %o, want 600", perm)
	}
}

// The renaming write must not leave its temporary behind, or the directory
// fills with files the operator cannot tell apart from records.
func TestRecordLeavesNoTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	r := resolvedWith(t, map[string]string{"FIXTURE_NAME": "x"})
	if _, _, err := RecordSuccess(dir, "prod", r); err != nil {
		t.Fatalf("RecordSuccess() error = %v", err)
	}
	if got := countFiles(t, filepath.Join(dir, installsDir)); got != 1 {
		t.Errorf("%d files in the records directory, want exactly the record", got)
	}
}

func TestRecordDir(t *testing.T) {
	t.Run("from the configured directory", func(t *testing.T) {
		got, err := RecordDir("/tmp/somewhere")
		if err != nil {
			t.Fatalf("RecordDir() error = %v", err)
		}
		if got != "/tmp/somewhere" {
			t.Errorf("RecordDir() = %q, want the configured directory", got)
		}
	})
	t.Run("falls back to the cache directory", func(t *testing.T) {
		got, err := RecordDir("")
		if err != nil {
			t.Fatalf("RecordDir() error = %v", err)
		}
		if !strings.HasSuffix(got, filepath.Join("ate-setup")) {
			t.Errorf("RecordDir() = %q, want a directory of our own under the cache root", got)
		}
		if got == "ate-setup" {
			t.Error("RecordDir() is relative; a record would land wherever the shell installer chdir'd to")
		}
	})
}

func countFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	return len(entries)
}

// kubeconfigFixture names one context, so a test can assert which cluster was
// found without depending on the developer's own kubeconfig.
const kubeconfigFixture = `apiVersion: v1
kind: Config
current-context: prod-cluster
clusters:
- {name: c, cluster: {server: "https://127.0.0.1:1"}}
contexts:
- {name: prod-cluster, context: {cluster: c, user: u}}
users:
- {name: u, user: {}}
`

func writeKubeconfig(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(kubeconfigFixture), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

// An unset kubeconfig setting is not the empty path. It leaves client-go's
// default rules in place, which find the file the way kubectl does -- and the
// cluster still has to be named, or every install that does not set
// --kubeconfig shares one record.
func TestClusterKeyUsesTheDefaultKubeconfigRules(t *testing.T) {
	t.Setenv("KUBECONFIG", writeKubeconfig(t, "config"))

	// Env is empty, so nothing supplies the kubeconfig setting and ClusterKey
	// takes the branch that configures no path of its own.
	r := resolvedWith(t, nil)
	if got := r.String("kubeconfig"); got != "" {
		t.Fatalf("kubeconfig = %q, want it unset for this case", got)
	}
	if got := r.ClusterKey(); got != "prod-cluster" {
		t.Errorf("ClusterKey() = %q, want %q", got, "prod-cluster")
	}
}

// $KUBECONFIG holding several files is a precedence chain, not a path.
// Handing the whole string to client-go as one file would find nothing and
// leave the record unnamed.
func TestClusterKeyReadsAKubeconfigList(t *testing.T) {
	list := strings.Join([]string{
		filepath.Join(t.TempDir(), "missing"),
		writeKubeconfig(t, "second"),
	}, string(os.PathListSeparator))

	r := resolvedWith(t, map[string]string{"KUBECONFIG": list})
	if got := r.ClusterKey(); got != "prod-cluster" {
		t.Errorf("ClusterKey() = %q, want %q", got, "prod-cluster")
	}
}

// A kubeconfig that is named but absent leaves the record unnamed rather than
// failing a run that otherwise worked.
func TestClusterKeyIsEmptyWithoutAReadableKubeconfig(t *testing.T) {
	r := resolvedWith(t, map[string]string{"KUBECONFIG": "/nonexistent/kubeconfig"})
	if got := r.ClusterKey(); got != "" {
		t.Errorf("ClusterKey() = %q, want \"\"", got)
	}
}
