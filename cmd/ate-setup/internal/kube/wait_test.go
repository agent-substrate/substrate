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

package kube

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// shortPollInterval makes the retry path observable without a real sleep.
func shortPollInterval(t *testing.T) {
	t.Helper()
	original := pollInterval
	pollInterval = time.Millisecond
	t.Cleanup(func() { pollInterval = original })
}

var deploymentsResource = schema.GroupResource{Group: "apps", Resource: "deployments"}

func TestRetryableWaitError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		// The install runs a burst of applies and waits against a control
		// plane that is often still starting, which is exactly when these
		// show up.
		{"throttled", apierrors.NewTooManyRequests("slow down", 1), true},
		{"apiserver unavailable", apierrors.NewServiceUnavailable("apiserver is starting"), true},
		{"webhook not serving", apierrors.NewInternalError(errors.New("failed calling webhook")), true},
		{"server timeout", apierrors.NewServerTimeout(deploymentsResource, "get", 1), true},
		{"connection reset", fmt.Errorf("Get \"https://k8s/api\": %w", syscall.ECONNRESET), true},
		{"connection refused", fmt.Errorf("Get \"https://k8s/api\": %w", syscall.ECONNREFUSED), true},

		// These describe the cluster, not the connection to it. Retrying only
		// delays the report.
		{"not found", apierrors.NewNotFound(deploymentsResource, "api"), false},
		{"forbidden", apierrors.NewForbidden(deploymentsResource, "api", errors.New("nope")), false},
		{"invalid", apierrors.NewBadRequest("malformed"), false},
		{"plain error", errors.New("deployment exceeded its progress deadline"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryableWaitError(tc.err); got != tc.want {
				t.Errorf("retryableWaitError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// One 429 during a two-minute rollout wait used to fail the install.
func TestPollRecoversFromRetryableErrors(t *testing.T) {
	shortPollInterval(t)

	calls := 0
	err := poll(t.Context(), time.Minute, func(context.Context) (bool, error) {
		calls++
		switch calls {
		case 1:
			return false, apierrors.NewTooManyRequests("slow down", 1)
		case 2:
			return false, apierrors.NewServiceUnavailable("apiserver is starting")
		default:
			return true, nil
		}
	})
	if err != nil {
		t.Fatalf("poll() error = %v, want it to keep polling through transient errors", err)
	}
	if calls != 3 {
		t.Errorf("check called %d times, want 3", calls)
	}
}

// A wait that only ever sees transient errors has to name one. Otherwise the
// operator gets a bare "context deadline exceeded" for a cluster that was
// refusing every request.
func TestPollReportsTheLastRetryableError(t *testing.T) {
	shortPollInterval(t)

	err := poll(t.Context(), 10*time.Millisecond, func(context.Context) (bool, error) {
		return false, apierrors.NewTooManyRequests("slow down", 1)
	})
	if err == nil {
		t.Fatal("poll() succeeded, want a timeout")
	}
	if !strings.Contains(err.Error(), "slow down") {
		t.Errorf("poll() error = %v, want it to carry the last retryable error", err)
	}
}

// Retrying a permanent error just spends the timeout before reporting it.
func TestPollAbortsOnPermanentErrors(t *testing.T) {
	shortPollInterval(t)

	calls := 0
	want := apierrors.NewForbidden(deploymentsResource, "api", errors.New("nope"))
	err := poll(t.Context(), time.Minute, func(context.Context) (bool, error) {
		calls++
		return false, want
	})
	if !apierrors.IsForbidden(err) {
		t.Fatalf("poll() error = %v, want it to return the Forbidden unchanged", err)
	}
	if calls != 1 {
		t.Errorf("check called %d times, want 1", calls)
	}
}

// RolloutStatus classifies before poll sees the error: a workload that is
// missing right after its manifest was applied is worth a few more probes, but
// one that never appears is a real failure rather than a bare timeout.
func TestRolloutStatusGivesUpOnAPersistentlyMissingWorkload(t *testing.T) {
	shortPollInterval(t)

	c := &Client{Typed: kubefake.NewSimpleClientset()}
	err := c.RolloutStatus(t.Context(), KindDeployment, "ate-system", "api", time.Minute)
	if err == nil {
		t.Fatal("RolloutStatus() succeeded, want an error")
	}

	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("RolloutStatus() error = %v, want it to say the deployment was not found", err)
	}
}

func TestWaitClusterTrustBundles(t *testing.T) {
	for _, tc := range []struct {
		name, version string
	}{
		{name: "stable", version: "v1"},
		{name: "beta", version: "v1beta1"},
		{name: "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kc := kubefake.NewSimpleClientset()
			if tc.version != "" {
				kc.Resources = []*metav1.APIResourceList{{GroupVersion: "certificates.k8s.io/" + tc.version,
					APIResources: []metav1.APIResource{{Name: "clustertrustbundles"}}}}
				if tc.version == "v1" {
					if _, err := kc.CertificatesV1().ClusterTrustBundles().Create(t.Context(),
						&certsv1.ClusterTrustBundle{ObjectMeta: metav1.ObjectMeta{Name: "identity"}}, metav1.CreateOptions{}); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := kc.CertificatesV1beta1().ClusterTrustBundles().Create(t.Context(),
						&certsv1beta1.ClusterTrustBundle{ObjectMeta: metav1.ObjectMeta{Name: "identity"}}, metav1.CreateOptions{}); err != nil {
						t.Fatal(err)
					}
				}
			}
			c := &Client{Typed: kc}
			checkErr := c.CheckClusterTrustBundleAPI()
			err := c.WaitClusterTrustBundles(t.Context(), []string{"identity"}, time.Second)
			if tc.version == "" {
				if checkErr == nil || !strings.Contains(checkErr.Error(), "neither v1 nor v1beta1") {
					t.Fatalf("prerequisite: %v", checkErr)
				}
				if err == nil || !strings.Contains(err.Error(), "neither v1 nor v1beta1") {
					t.Fatalf("missing API: %v", err)
				}
			} else if checkErr != nil {
				t.Fatal(checkErr)
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
