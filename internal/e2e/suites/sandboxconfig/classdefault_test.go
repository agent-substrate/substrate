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

// Package sandboxconfig holds the end-to-end tests for how a WorkerPool
// resolves its SandboxConfig: which config atecontroller picks, how the pick
// shows up in the pool's status and events, and how the protection finalizer
// holds a config until no pool names it.
//
// The pools here run no workers (replicas is 0). Worker pods take nothing
// from the SandboxConfig, so the resolution is all there is to test, and a
// pool without pods costs the cluster nothing.
//
// The class defaults the tests create are cluster-scoped, so every other pool
// of the class without a configRef follows them too, and back. Each is a copy
// of the installed default's spec under another name, so nothing those pools
// run changes.
package sandboxconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/e2e"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// eventReasonSwitched is the reason of the Normal event atecontroller
	// emits each time a pool's SandboxConfig changes.
	eventReasonSwitched = "SandboxConfigSwitched"

	// stepTimeout bounds each wait on the controller. A switch takes a few
	// reconciles. The deletion of a config in use also waits for the
	// protection reconciler to release it once every pool has moved off it.
	stepTimeout = 60 * time.Second
)

// TestWorkerPoolFollowsClassDefault covers a pool with no configRef, which
// follows its class default: the newest SandboxConfig of its class annotated
// sandboxconfig.ate.dev/is-class-default=true. Starting from the default the
// cluster installs, a newer default is created, un-annotated, re-annotated,
// and finally deleted while the pool uses it. At each step the pool's status
// must follow, and at the end its events must record every move exactly once.
func TestWorkerPoolFollowsClassDefault(t *testing.T) {
	ctx := t.Context()
	clients := e2e.GetClients()

	class := atev1.SandboxClassGvisor
	if e2e.IsMicroVM() {
		class = atev1.SandboxClassMicroVM
	}
	base := newestClassDefault(t, ctx, clients, class)
	t.Logf("Class %q default as installed: %s", class, base.Name)

	switched := func(from, to string) string {
		return fmt.Sprintf("sandbox class %q switched from SandboxConfig %q to %q", class, from, to)
	}

	// 1. A pool with no configRef resolves to the installed default.
	ns := e2e.CreateNamespace(t).Name
	pool, err := clients.SubstrateK8s.AteV1().WorkerPools(ns).Create(ctx, &atev1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "follows-default", Namespace: ns},
		Spec: atev1.WorkerPoolSpec{
			Replicas: 0,
			// Never pulled: the pool runs no workers.
			WorkerImage:    "worker:unused",
			SandboxClasses: []atev1.WorkerPoolSandboxClass{{Name: class}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create WorkerPool: %v", err)
	}
	waitForSandboxConfig(t, ctx, clients, pool, base.Name)
	waitForEvent(t, ctx, clients, pool, fmt.Sprintf("sandbox class %q uses SandboxConfig %q", class, base.Name))

	// 2. A newer class default appears: the pool moves to it. The name sorts
	// before the installed default's, which is the tie-break should both
	// carry the same creation second.
	v2Name := fmt.Sprintf("e2e-%s-default-%s", class, ns)
	if _, err := clients.SubstrateK8s.AteV1().SandboxConfigs().Create(ctx, &atev1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:        v2Name,
			Annotations: map[string]string{atev1.SandboxConfigClassDefaultAnnotation: "true"},
		},
		Spec: *base.Spec.DeepCopy(),
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create SandboxConfig %s: %v", v2Name, err)
	}
	// Always, pass or fail: a class default left behind moves every pool of
	// the class in the cluster. Not t.Context: it is canceled before cleanups.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := clients.SubstrateK8s.AteV1().SandboxConfigs().Delete(cleanupCtx, v2Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("failed to delete SandboxConfig %s: %v", v2Name, err)
		}
	})
	waitForSandboxConfig(t, ctx, clients, pool, v2Name)
	waitForEvent(t, ctx, clients, pool, switched(base.Name, v2Name))

	// 3. The newer config stops being a default: the pool moves back.
	setClassDefault(t, ctx, clients, v2Name, false)
	waitForSandboxConfig(t, ctx, clients, pool, base.Name)
	waitForEvent(t, ctx, clients, pool, switched(v2Name, base.Name))

	// 4. It becomes a default again, and is deleted while the pool uses it.
	// A default being deleted is never picked, so the pool moves back, and
	// only then does the protection finalizer let the config go.
	setClassDefault(t, ctx, clients, v2Name, true)
	waitForSandboxConfig(t, ctx, clients, pool, v2Name)
	waitForEvent(t, ctx, clients, pool, switched(base.Name, v2Name))

	if err := clients.SubstrateK8s.AteV1().SandboxConfigs().Delete(ctx, v2Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("failed to delete SandboxConfig %s: %v", v2Name, err)
	}
	waitForSandboxConfig(t, ctx, clients, pool, base.Name)
	waitForSandboxConfigGone(t, ctx, clients, v2Name)
	waitForEvent(t, ctx, clients, pool, switched(v2Name, base.Name))

	// 5. The pool's events are the history of its configs, each move once.
	// Two moves to the same config give two events, in order. A move
	// reported twice within one reconcile would show up as a series instead.
	want := []string{
		fmt.Sprintf("sandbox class %q uses SandboxConfig %q", class, base.Name),
		switched(base.Name, v2Name),
		switched(v2Name, base.Name),
		switched(base.Name, v2Name),
		switched(v2Name, base.Name),
	}
	events, err := poolEvents(ctx, clients, pool)
	if err != nil {
		t.Fatalf("failed to list events: %v", err)
	}
	var got []string
	for _, ev := range events {
		got = append(got, ev.Note)
		if ev.Type != corev1.EventTypeNormal || ev.Reason != eventReasonSwitched {
			t.Errorf("unexpected %s %s event: %s", ev.Type, ev.Reason, ev.Note)
		}
		if ev.Series != nil {
			t.Errorf("event recorded %d times, want once: %s", ev.Series.Count, ev.Note)
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("events regarding the pool, oldest first:\n  got:\n    %s\n  want:\n    %s",
			strings.Join(got, "\n    "), strings.Join(want, "\n    "))
	}
}

// newestClassDefault returns the SandboxConfig a pool of class with no
// configRef resolves to when the test starts: the newest of the class
// annotated as its default, by creation time and then by name, the same rule
// atecontroller applies. The test cannot start without one.
func newestClassDefault(t *testing.T, ctx context.Context, clients *e2e.Clients, class atev1.SandboxClass) *atev1.SandboxConfig {
	t.Helper()
	list, err := clients.SubstrateK8s.AteV1().SandboxConfigs().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list SandboxConfigs: %v", err)
	}
	var best *atev1.SandboxConfig
	for i := range list.Items {
		sc := &list.Items[i]
		if sc.Spec.SandboxClass != class || sc.Annotations[atev1.SandboxConfigClassDefaultAnnotation] != "true" || !sc.DeletionTimestamp.IsZero() {
			continue
		}
		if best == nil || best.CreationTimestamp.Before(&sc.CreationTimestamp) ||
			(best.CreationTimestamp.Equal(&sc.CreationTimestamp) && sc.Name < best.Name) {
			best = sc
		}
	}
	if best == nil {
		t.Fatalf("no SandboxConfig of class %q is annotated %s=true; the install ships one, see manifests/ate-install", class, atev1.SandboxConfigClassDefaultAnnotation)
	}
	return best
}

// setClassDefault sets or removes the class-default annotation on the
// SandboxConfig named name.
func setClassDefault(t *testing.T, ctx context.Context, clients *e2e.Clients, name string, isDefault bool) {
	t.Helper()
	var value any
	if isDefault {
		value = "true"
	}
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]any{
		atev1.SandboxConfigClassDefaultAnnotation: value,
	}}})
	if err != nil {
		t.Fatalf("failed to marshal patch: %v", err)
	}
	if _, err := clients.SubstrateK8s.AteV1().SandboxConfigs().Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		t.Fatalf("failed to patch SandboxConfig %s: %v", name, err)
	}
	t.Logf("SandboxConfig %s: %s=%v", name, atev1.SandboxConfigClassDefaultAnnotation, value)
}

// waitForSandboxConfig blocks until pool's status reports its sandbox class
// Resolved, using the SandboxConfig named want, with no switch pending.
func waitForSandboxConfig(t *testing.T, ctx context.Context, clients *e2e.Clients, pool *atev1.WorkerPool, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, stepTimeout)
	defer cancel()

	class := pool.Spec.DefaultSandboxClass()
	var last *atev1.WorkerPoolSandboxClassStatus
	for {
		wp, err := clients.SubstrateK8s.AteV1().WorkerPools(pool.Namespace).Get(ctx, pool.Name, metav1.GetOptions{})
		if err == nil {
			last = nil
			for i := range wp.Status.SandboxClasses {
				if wp.Status.SandboxClasses[i].Name == class {
					last = &wp.Status.SandboxClasses[i]
				}
			}
			if last != nil && last.ConfigRef != nil && last.ConfigRef.Name == want && last.PendingConfigRef == nil &&
				meta.IsStatusConditionTrue(last.Conditions, atev1.WorkerPoolSandboxClassConditionResolved) {
				t.Logf("WorkerPool %s/%s uses SandboxConfig %s", pool.Namespace, pool.Name, want)
				return
			}
		}
		select {
		case <-ctx.Done():
			status, _ := json.Marshal(last)
			t.Fatalf("timed out after %v waiting for WorkerPool %s/%s to use SandboxConfig %q (last status %s, err %v)", stepTimeout, pool.Namespace, pool.Name, want, status, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitForSandboxConfigGone blocks until the SandboxConfig named name no
// longer exists, which for a config a pool used means its protection
// finalizer was released.
func waitForSandboxConfigGone(t *testing.T, ctx context.Context, clients *e2e.Clients, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, stepTimeout)
	defer cancel()

	for {
		sc, err := clients.SubstrateK8s.AteV1().SandboxConfigs().Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			t.Logf("SandboxConfig %s is gone", name)
			return
		}
		select {
		case <-ctx.Done():
			var state string
			if err == nil {
				state = fmt.Sprintf("deletionTimestamp %v, finalizers %v", sc.DeletionTimestamp, sc.Finalizers)
			} else {
				state = err.Error()
			}
			t.Fatalf("timed out after %v waiting for SandboxConfig %s to be deleted (%s)", stepTimeout, name, state)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitForEvent blocks until an event regarding pool carries note. Events are
// recorded asynchronously, so one can trail the status write it reports.
func waitForEvent(t *testing.T, ctx context.Context, clients *e2e.Clients, pool *atev1.WorkerPool, note string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, stepTimeout)
	defer cancel()

	var seen []string
	for {
		events, err := poolEvents(ctx, clients, pool)
		if err == nil {
			seen = seen[:0]
			for _, ev := range events {
				if ev.Note == note {
					t.Logf("Event: %s %s %s", ev.Type, ev.Reason, ev.Note)
					return
				}
				seen = append(seen, fmt.Sprintf("%s %s %s", ev.Type, ev.Reason, ev.Note))
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out after %v waiting for event %q on WorkerPool %s/%s (seen: %q, err %v)", stepTimeout, note, pool.Namespace, pool.Name, seen, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// poolEvents returns the events regarding pool, oldest first.
func poolEvents(ctx context.Context, clients *e2e.Clients, pool *atev1.WorkerPool) ([]eventsv1.Event, error) {
	list, err := clients.K8s.EventsV1().Events(pool.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var events []eventsv1.Event
	for _, ev := range list.Items {
		if ev.Regarding.UID == pool.UID {
			events = append(events, ev)
		}
	}
	slices.SortFunc(events, func(a, b eventsv1.Event) int {
		return eventTime(a).Compare(eventTime(b))
	})
	return events, nil
}

// eventTime is when an event was emitted: eventTime for one recorded through
// events.k8s.io, else its creation time.
func eventTime(ev eventsv1.Event) time.Time {
	if !ev.EventTime.IsZero() {
		return ev.EventTime.Time
	}
	return ev.CreationTimestamp.Time
}
