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

package e2e

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RequireShortSnapshotDelays skips t unless atelet runs with
// --snapshot-upload-delay and --snapshot-detach-delay of at most maxDelay, so t
// can wait for atelet to upload or detach a suspended actor's snapshot in the
// background. In CI it fails instead.
func RequireShortSnapshotDelays(t testing.TB, ctx context.Context, maxDelay time.Duration) {
	t.Helper()
	ds, err := GetClients().K8s.AppsV1().DaemonSets(SystemNamespace()).Get(ctx, ResourceName("atelet"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting the atelet DaemonSet: %v", err)
	}
	containers := ds.Spec.Template.Spec.Containers
	i := slices.IndexFunc(containers, func(c corev1.Container) bool { return c.Name == "atelet" })
	if i < 0 {
		t.Fatalf("DaemonSet %s/%s has no atelet container", ds.Namespace, ds.Name)
	}
	skip, fail := shortSnapshotDelaysDecision(containers[i].Args, maxDelay, shortSnapshotDelaysRequired())
	if fail != "" {
		t.Fatal(fail)
	}
	if skip != "" {
		t.Skip(skip)
	}
}

// shortSnapshotDelaysRequired reports whether RequireShortSnapshotDelays fails
// rather than skips. CI runs with --e2e-tuning, which sets both delays short,
// so a skip there would pass having checked nothing.
func shortSnapshotDelaysRequired() bool {
	return os.Getenv("CI") == "true"
}

// shortSnapshotDelaysDecision is RequireShortSnapshotDelays's rule for args,
// atelet's arguments, apart from testing.TB and the cluster so it is tested
// without them.
func shortSnapshotDelaysDecision(args []string, maxDelay time.Duration, required bool) (skip, fail string) {
	fs := pflag.NewFlagSet("atelet", pflag.ContinueOnError)
	fs.ParseErrorsAllowlist.UnknownFlags = true
	upload := fs.Duration("snapshot-upload-delay", 0, "")
	detach := fs.Duration("snapshot-detach-delay", 0, "")
	if err := fs.Parse(args); err != nil {
		return "", fmt.Sprintf("parsing atelet's arguments: %v", err)
	}
	// An unset flag leaves atelet's default delay, which is minutes long.
	describe := func(name string, d time.Duration) string {
		if !fs.Changed(name) {
			return "unset"
		}
		return d.String()
	}
	if fs.Changed("snapshot-upload-delay") && *upload <= maxDelay && fs.Changed("snapshot-detach-delay") && *detach <= maxDelay {
		return "", ""
	}
	msg := fmt.Sprintf("this check waits for atelet to upload or detach a snapshot in the background, so it needs atelet's --snapshot-upload-delay and --snapshot-detach-delay at most %v, as manifests/ate-install/components/e2e sets them; they are %s and %s",
		maxDelay, describe("snapshot-upload-delay", *upload), describe("snapshot-detach-delay", *detach))
	if required {
		return "", msg + " (required because CI=true)"
	}
	return msg, ""
}
