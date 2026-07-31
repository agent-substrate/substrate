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
	"context"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const snapshotGCLeaseName = "ateapi-snapshot-gc"

func runSnapshotGCLeaderElection(ctx context.Context, client kubernetes.Interface, namespace, identity string, run func(context.Context)) error {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: snapshotGCLeaseName, Namespace: namespace},
		Client:    client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   30 * time.Second,
		RenewDeadline:   20 * time.Second,
		RetryPeriod:     5 * time.Second,
		ReleaseOnCancel: true,
		Name:            snapshotGCLeaseName,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: run,
			OnStoppedLeading: func() {
				slog.InfoContext(ctx, "Stopped leading ActorSnapshot garbage collection")
			},
			OnNewLeader: func(current string) {
				slog.InfoContext(ctx, "ActorSnapshot garbage collection leader elected", "identity", current)
			},
		},
		Coordinated: false,
	})
	if err != nil {
		return err
	}
	elector.Run(ctx)
	return nil
}
