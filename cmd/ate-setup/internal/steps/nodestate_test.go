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
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/versionlabel"
)

// runWipeScript runs wipe_node_state against root with the given mount table,
// keeping the top-level entries named in keep, and reports whether it exited
// zero.
func runWipeScript(t *testing.T, root, mountinfo string, keep ...string) bool {
	t.Helper()
	for _, tool := range []string{"sh", "awk"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available: %v", tool, err)
		}
	}
	cmd := exec.Command("sh", "-c", nodeStateWipeScript+`wipe_node_state "$1" "$2" "$3"`, "sh", root, mountinfo, strings.Join(keep, " "))
	out, err := cmd.CombinedOutput()
	t.Logf("wipe_node_state output:\n%s", out)
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		t.Fatalf("running wipe_node_state: %v", err)
	}
	return err == nil
}

// writeTree creates each file, with its parents, under root.
func writeTree(t *testing.T, root string, files ...string) {
	t.Helper()
	for _, f := range files {
		path := filepath.Join(root, f)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// listTree returns every path under root, relative to it.
func listTree(t *testing.T, root string) []string {
	t.Helper()
	var got []string
	err := filepath.WalkDir(root, func(path string, _ os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path != root {
			rel, _ := filepath.Rel(root, path)
			got = append(got, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	return got
}

// mountTable writes a mountinfo file with one line per mount point.
func mountTable(t *testing.T, mountPoints ...string) string {
	t.Helper()
	var b strings.Builder
	for i, mp := range mountPoints {
		b.WriteString(strings.Join([]string{
			"10" + string(rune('0'+i)), "1", "0:50", "/", mp, "rw,relatime", "-", "tmpfs", "tmpfs", "rw",
		}, " ") + "\n")
	}
	path := filepath.Join(t.TempDir(), "mountinfo")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestNodeStateWipeScriptEmptiesRoot(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root,
		"actors/a/bundle/rootfs/file",
		"actors/a/local-checkpoint/snap/checkpoint.img",
		".hidden",
		"..dotdot",
		"ateom-support.sock",
	)
	if err := os.Symlink("/nonexistent", filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}
	// The root's own mount and unrelated mounts do not hold anything back.
	mounts := mountTable(t, "/", "/proc", root, root+"-sibling/x")

	if !runWipeScript(t, root, mounts) {
		t.Error("wipe_node_state failed, want success")
	}
	if got := listTree(t, root); len(got) != 0 {
		t.Errorf("left %q under the root, want it empty", got)
	}
	if _, err := os.Stat(root); err != nil {
		t.Errorf("root itself is gone (%v), want it kept: it is the pod's mount point", err)
	}
}

// The content-addressed caches hold nothing tied to an install, so the wipe
// can leave them. Entries are kept by their whole name and only at the top: a
// directory of the same name deeper down is node state like any other.
func TestNodeStateWipeScriptKeepsNamedEntries(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root,
		"actors/a/bundle/file",
		"actors/a/image-cache/file",
		"ateoms/pod-1/file",
		"image-cache/layers/sha256-1/file",
		"static-files/runsc-1",
		"static-files-old/file",
	)
	mounts := mountTable(t, root)

	if !runWipeScript(t, root, mounts, "image-cache", "static-files") {
		t.Error("wipe_node_state failed, want success")
	}
	want := []string{
		"image-cache",
		"image-cache/layers",
		"image-cache/layers/sha256-1",
		"image-cache/layers/sha256-1/file",
		"static-files",
		"static-files/runsc-1",
	}
	if got := listTree(t, root); !slices.Equal(got, want) {
		t.Errorf("left %q under the root, want only the kept entries: %q", got, want)
	}
}

// An external volume still mounted under an actor directory must keep its
// contents: deleting through the mount would delete the volume's data, not
// node state.
func TestNodeStateWipeScriptLeavesMountsInPlace(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root,
		"actors/a/bundle/file",
		"actors/b/volumes/vol/data",
		"actors/b/volumes/other/data",
		"actors/b/bundle/file",
		"static-files/f",
	)
	mounts := mountTable(t, root, filepath.Join(root, "actors/b/volumes/vol"))

	if runWipeScript(t, root, mounts) {
		t.Error("wipe_node_state succeeded, want a failure reporting the mount left in place")
	}
	want := []string{
		"actors",
		"actors/b",
		"actors/b/volumes",
		"actors/b/volumes/vol",
		"actors/b/volumes/vol/data",
	}
	if got := listTree(t, root); !slices.Equal(got, want) {
		t.Errorf("left %q under the root, want only the mount and the directories leading to it: %q", got, want)
	}
}

func TestNodeStateWipeScriptRefusesWithoutMountTable(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, "actors/a/bundle/file")

	if runWipeScript(t, root, filepath.Join(t.TempDir(), "missing")) {
		t.Error("wipe_node_state succeeded without a mount table, want a failure")
	}
	if got := listTree(t, root); len(got) == 0 {
		t.Error("deleted node state without knowing what is mounted, want it left in place")
	}
}

func TestNodeStateWipeDaemonSet(t *testing.T) {
	ds := nodeStateWipeDaemonSet(nodeStateWipeKeep(DeleteOptions{}))
	pod := ds.Spec.Template.Spec

	if ds.Namespace != metav1.NamespaceSystem {
		t.Errorf("namespace = %q, want %q: the install namespace is deleted by then", ds.Namespace, metav1.NamespaceSystem)
	}
	if len(pod.Volumes) != 1 || pod.Volumes[0].HostPath == nil || pod.Volumes[0].HostPath.Path != nodepath.BasePath {
		t.Errorf("volumes = %+v, want one hostPath of %s", pod.Volumes, nodepath.BasePath)
	}
	terms := pod.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 1 ||
		terms[0].MatchExpressions[0].Key != versionlabel.Key ||
		terms[0].MatchExpressions[0].Operator != corev1.NodeSelectorOpExists {
		t.Errorf("node affinity = %+v, want the nodes carrying %s", terms, versionlabel.Key)
	}
	if len(pod.Tolerations) != 1 || pod.Tolerations[0].Operator != corev1.TolerationOpExists || pod.Tolerations[0].Key != "" {
		t.Errorf("tolerations = %+v, want one that tolerates every taint", pod.Tolerations)
	}
	c := pod.Containers[0]
	if !strings.Contains(c.Image, "@sha256:") {
		t.Errorf("image = %q, want it pinned by digest", c.Image)
	}
	if len(c.VolumeMounts) != 1 || c.VolumeMounts[0].MountPath != nodeStateMountPath {
		t.Errorf("volume mounts = %+v, want %s", c.VolumeMounts, nodeStateMountPath)
	}
	script := strings.Join(c.Command, " ")
	if !strings.Contains(script, "wipe_node_state "+nodeStateMountPath) || !strings.Contains(script, "touch "+nodeStateWipeDoneFile) {
		t.Errorf("command = %q, want it to wipe %s and then mark %s", script, nodeStateMountPath, nodeStateWipeDoneFile)
	}
	for _, cache := range []string{"image-cache", "static-files"} {
		if !strings.Contains(script, cache) {
			t.Errorf("command = %q, want it to keep %s", script, cache)
		}
	}
	if probe := c.ReadinessProbe; probe == nil || probe.Exec == nil || !slices.Contains(probe.Exec.Command, nodeStateWipeDoneFile) {
		t.Errorf("readiness probe = %+v, want it to check %s", probe, nodeStateWipeDoneFile)
	}
}

// The wipe keeps each cache by its name directly under nodepath.BasePath, so
// that is where each has to sit; --wipe-node-caches keeps nothing.
func TestNodeStateWipeKeep(t *testing.T) {
	if got, want := nodeStateWipeKeep(DeleteOptions{}), []string{"image-cache", "static-files"}; !slices.Equal(got, want) {
		t.Errorf("nodeStateWipeKeep() = %q, want %q", got, want)
	}
	if filepath.Dir(nodepath.StaticFilesDir) != nodepath.BasePath {
		t.Errorf("%s is not directly under %s, so keeping it by name keeps nothing", nodepath.StaticFilesDir, nodepath.BasePath)
	}
	if got := nodeStateWipeKeep(DeleteOptions{WipeNodeCaches: true}); len(got) != 0 {
		t.Errorf("nodeStateWipeKeep(WipeNodeCaches) = %q, want nothing kept", got)
	}
	script := strings.Join(nodeStateWipeDaemonSet(nil).Spec.Template.Spec.Containers[0].Command, " ")
	for _, cache := range nodeStateCaches {
		if strings.Contains(script, cache) {
			t.Errorf("command with nothing to keep still names %s", cache)
		}
	}
}

func TestShouldWipeNodeState(t *testing.T) {
	for _, tc := range []struct {
		name     string
		opts     DeleteOptions
		cfg      config.Config
		recorded map[string]string
		want     bool
	}{
		{
			name: "bundled database",
			want: true,
		},
		{
			name: "keep-node-state",
			opts: DeleteOptions{KeepNodeState: true},
			want: false,
		},
		{
			name: "external connection string",
			cfg:  config.Config{PostgresReadWriteConnectionString: "postgres://db.example/ate"},
			want: false,
		},
		{
			name: "Cloud SQL from the environment",
			cfg:  config.Config{CloudSQL: config.CloudSQLConfig{Instance: "p:r:i", InstanceSet: true}},
			want: false,
		},
		{
			name:     "Cloud SQL recorded by the install",
			recorded: map[string]string{envCloudSQLInstance: "p:r:i"},
			want:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{Cfg: &tc.cfg, Kube: fakeKube(t, apiServerEnvVarsConfigMap(tc.recorded))}
			if got := e.shouldWipeNodeState(context.Background(), tc.opts); got != tc.want {
				t.Errorf("shouldWipeNodeState() = %v, want %v", got, tc.want)
			}
		})
	}
}

// The wipe DaemonSet is removed once it has run, including one left over from
// an interrupted run.
func TestWipeNodeStateRemovesItsDaemonSet(t *testing.T) {
	for _, tc := range []struct {
		name     string
		leftover bool
	}{
		{name: "fresh"},
		{name: "leftover from an interrupted run", leftover: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{Cfg: &config.Config{}, Kube: fakeKube(t)}
			ctx := context.Background()
			daemonSets := e.Kube.Typed.AppsV1().DaemonSets(nodeStateWipeNamespace)
			if tc.leftover {
				if _, err := daemonSets.Create(ctx, nodeStateWipeDaemonSet(nil), metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}

			if err := e.wipeNodeState(ctx, nil); err != nil {
				t.Fatalf("wipeNodeState() = %v", err)
			}
			if _, err := daemonSets.Get(ctx, nodeStateWipeName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Errorf("get daemonset after the wipe = %v, want NotFound", err)
			}
		})
	}
}
