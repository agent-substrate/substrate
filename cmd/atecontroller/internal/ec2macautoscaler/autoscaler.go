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

// Package ec2macautoscaler keeps EC2 Mac overflow capacity warm before local
// Mac workers run out of Actor slots.
package ec2macautoscaler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/labels"
)

// WorkerLister returns the current schedulable fleet.
type WorkerLister func(context.Context) ([]*ateapipb.Worker, error)

// ListAllWorkers adapts the paginated control API to WorkerLister.
func ListAllWorkers(client ateapipb.ControlClient) WorkerLister {
	return func(ctx context.Context) ([]*ateapipb.Worker, error) {
		var (
			workers   []*ateapipb.Worker
			pageToken string
		)
		for {
			page, err := client.ListWorkers(ctx, &ateapipb.ListWorkersRequest{
				PageSize:  1000,
				PageToken: pageToken,
			}, grpc.WaitForReady(true))
			if err != nil {
				return nil, err
			}
			workers = append(workers, page.GetWorkers()...)
			pageToken = page.GetNextPageToken()
			if pageToken == "" {
				return workers, nil
			}
		}
	}
}

// GroupScaler changes the desired size of the EC2 Mac Auto Scaling Group.
type GroupScaler interface {
	Desired(context.Context) (desired, maximum int32, err error)
	SetDesired(context.Context, int32) error
}

// Config controls predictive scale-up. The autoscaler deliberately never
// scales down: EC2 Mac hosts have a 24-hour minimum allocation and a Worker
// must be drained before its backing instance can safely terminate.
type Config struct {
	LocalSelector       labels.Selector
	CloudSelector       labels.Selector
	PollInterval        time.Duration
	LeadTime            time.Duration
	ScaleUpCooldown     time.Duration
	StressUtilization   float64
	StressSamples       int
	CloudHeadroomActors int64
	MaxCloudWorkers     int32
}

// Autoscaler samples Worker occupancy and warms EC2 Mac capacity when local
// utilization is sustained or its recent growth projects exhaustion soon.
type Autoscaler struct {
	config Config
	list   WorkerLister
	scaler GroupScaler

	lastSample    time.Time
	lastAllocated int64
	stressSamples int
	lastScaleUp   time.Time
}

func New(config Config, list WorkerLister, scaler GroupScaler) (*Autoscaler, error) {
	if config.LocalSelector == nil || config.CloudSelector == nil {
		return nil, fmt.Errorf("local and cloud selectors are required")
	}
	if config.PollInterval <= 0 || config.LeadTime <= 0 || config.ScaleUpCooldown <= 0 {
		return nil, fmt.Errorf("poll interval, lead time, and scale-up cooldown must be positive")
	}
	if config.StressUtilization <= 0 || config.StressUtilization > 1 {
		return nil, fmt.Errorf("stress utilization must be in (0, 1]")
	}
	if config.StressSamples < 1 || config.CloudHeadroomActors < 1 || config.MaxCloudWorkers < 1 {
		return nil, fmt.Errorf("stress samples, cloud headroom, and max cloud workers must be positive")
	}
	if list == nil || scaler == nil {
		return nil, fmt.Errorf("worker lister and group scaler are required")
	}
	return &Autoscaler{config: config, list: list, scaler: scaler}, nil
}

// Run samples until ctx is canceled. A failed sample is logged and retried on
// the next interval; it never causes a blind scaling write.
func (a *Autoscaler) Run(ctx context.Context) error {
	ticker := time.NewTicker(a.config.PollInterval)
	defer ticker.Stop()
	for {
		if err := a.sample(ctx, time.Now()); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "EC2 Mac warm-capacity sample failed", slog.Any("err", err))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Start implements controller-runtime's Runnable interface.
func (a *Autoscaler) Start(ctx context.Context) error { return a.Run(ctx) }

func (a *Autoscaler) sample(ctx context.Context, now time.Time) error {
	workers, err := a.list(ctx)
	if err != nil {
		return fmt.Errorf("listing workers: %w", err)
	}
	return a.reconcile(ctx, now, workers)
}

func (a *Autoscaler) reconcile(ctx context.Context, now time.Time, workers []*ateapipb.Worker) error {
	localCapacity, localAllocated, _ := actorSlots(workers, a.config.LocalSelector)
	cloudCapacity, cloudAllocated, cloudWorkers := actorSlots(workers, a.config.CloudSelector)
	cloudFree := cloudCapacity - cloudAllocated

	highUtilization := localCapacity > 0 && float64(localAllocated)/float64(localCapacity) >= a.config.StressUtilization
	projectedExhaustion := false
	if !a.lastSample.IsZero() && now.After(a.lastSample) && localAllocated > a.lastAllocated {
		growthPerSecond := float64(localAllocated-a.lastAllocated) / now.Sub(a.lastSample).Seconds()
		free := max(localCapacity-localAllocated, 0)
		projectedExhaustion = growthPerSecond > 0 && time.Duration(float64(time.Second)*float64(free)/growthPerSecond) <= a.config.LeadTime
	}
	a.lastSample = now
	a.lastAllocated = localAllocated

	if highUtilization || projectedExhaustion {
		a.stressSamples++
	} else {
		a.stressSamples = 0
	}
	if a.stressSamples < a.config.StressSamples || cloudFree >= a.config.CloudHeadroomActors {
		return nil
	}
	if !a.lastScaleUp.IsZero() && now.Sub(a.lastScaleUp) < a.config.ScaleUpCooldown {
		return nil
	}

	desired, groupMaximum, err := a.scaler.Desired(ctx)
	if err != nil {
		return fmt.Errorf("reading EC2 Mac Auto Scaling Group: %w", err)
	}
	// A desired instance that has not registered is already warm capacity in
	// flight. Wait for it rather than stacking costly 24-hour allocations.
	if int64(desired) > cloudWorkers {
		return nil
	}
	maximum := min(groupMaximum, a.config.MaxCloudWorkers)
	if desired >= maximum {
		return nil
	}
	if err := a.scaler.SetDesired(ctx, desired+1); err != nil {
		return fmt.Errorf("warming EC2 Mac capacity: %w", err)
	}
	a.lastScaleUp = now
	a.stressSamples = 0
	slog.InfoContext(ctx, "Requested warm EC2 Mac capacity",
		slog.Int64("local_capacity", localCapacity),
		slog.Int64("local_allocated", localAllocated),
		slog.Int64("cloud_free", cloudFree),
		slog.Int("desired_workers", int(desired+1)),
		slog.Bool("projected_exhaustion", projectedExhaustion))
	return nil
}

func actorSlots(workers []*ateapipb.Worker, selector labels.Selector) (capacity, allocated, count int64) {
	for _, worker := range workers {
		if worker.GetSandboxClass() != "macos-vz" || worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE || !selector.Matches(labels.Set(worker.GetLabels())) {
			continue
		}
		capacity += int64(worker.GetStatus().GetCapacity().GetActors())
		allocated += int64(worker.GetStatus().GetAllocated().GetActors())
		count++
	}
	return capacity, allocated, count
}
