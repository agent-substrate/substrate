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

package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const defaultWaitPollInterval = 500 * time.Millisecond

var (
	waitActorAtespace string
	waitActorFor      string
	waitActorTimeout  time.Duration
)

var waitActorCmd = &cobra.Command{
	Use:     "actor <actor-name>",
	Aliases: []string{"actors"},
	Short:   "Wait for an actor to reach a status",
	Example: `  # Wait up to 60 seconds for an actor to reach the running status
  kubectl ate wait actor my-actor --atespace team-a --for=status=running --timeout=60s

  # Check whether an actor is currently suspended without polling
  kubectl ate wait actor my-actor -a team-a --for=status=suspended --timeout=0s`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		targetStatus, err := parseWaitActorStatus(waitActorFor)
		if err != nil {
			return err
		}
		if waitActorTimeout < 0 {
			return fmt.Errorf("--timeout must be greater than or equal to zero")
		}

		apiClient, err := ateclient.NewClient(cmd.Context(), kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		runner := &WaitActorRunner{
			apiClient:    apiClient,
			out:          cmd.OutOrStdout(),
			pollInterval: defaultWaitPollInterval,
		}
		return runner.Run(cmd.Context(), waitActorAtespace, args[0], targetStatus, waitActorTimeout)
	},
}

// WaitActorRunner polls an actor until it reaches the requested status.
type WaitActorRunner struct {
	apiClient    ActorGetter
	out          io.Writer
	pollInterval time.Duration
}

type ActorGetter interface {
	GetActor(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error)
}

func (r *WaitActorRunner) Run(ctx context.Context, atespace, actorName string, targetStatus ateapipb.Actor_Status, timeout time.Duration) error {
	waitCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	check := func(checkCtx context.Context) (bool, error) {
		actor, err := r.apiClient.GetActor(checkCtx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actorName},
		})
		if err != nil {
			return false, fmt.Errorf("failed to get actor %q: %w", actorName, err)
		}
		return actor.GetStatus() == targetStatus, nil
	}

	met, err := check(waitCtx)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if waitCtx.Err() == context.DeadlineExceeded {
			return waitActorTimeoutError(actorName, targetStatus, timeout)
		}
		return err
	}
	if met {
		fmt.Fprintf(r.out, "actor/%s condition met\n", actorName)
		return nil
	}
	if timeout == 0 {
		return waitActorTimeoutError(actorName, targetStatus, timeout)
	}

	interval := r.pollInterval
	if interval <= 0 {
		interval = defaultWaitPollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return waitActorTimeoutError(actorName, targetStatus, timeout)
		case <-ticker.C:
			met, err := check(waitCtx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if waitCtx.Err() == context.DeadlineExceeded {
					return waitActorTimeoutError(actorName, targetStatus, timeout)
				}
				return err
			}
			if met {
				fmt.Fprintf(r.out, "actor/%s condition met\n", actorName)
				return nil
			}
		}
	}
}

func parseWaitActorStatus(condition string) (ateapipb.Actor_Status, error) {
	const prefix = "status="
	if !strings.HasPrefix(strings.ToLower(condition), prefix) {
		return ateapipb.Actor_STATUS_UNSPECIFIED, fmt.Errorf("unsupported --for value %q: expected status=<status>", condition)
	}

	statusName := strings.ToUpper(strings.TrimSpace(condition[len(prefix):]))
	if !strings.HasPrefix(statusName, "STATUS_") {
		statusName = "STATUS_" + statusName
	}
	statusValue, ok := ateapipb.Actor_Status_value[statusName]
	if !ok || statusName == "STATUS_UNSPECIFIED" {
		return ateapipb.Actor_STATUS_UNSPECIFIED, fmt.Errorf("unknown actor status %q", condition[len(prefix):])
	}
	return ateapipb.Actor_Status(statusValue), nil
}

func waitActorTimeoutError(actorName string, targetStatus ateapipb.Actor_Status, timeout time.Duration) error {
	status := strings.TrimPrefix(targetStatus.String(), "STATUS_")
	return fmt.Errorf("timed out after %s waiting for actor %q to reach status %s", timeout, actorName, status)
}

func init() {
	waitActorCmd.Flags().StringVarP(&waitActorAtespace, "atespace", "a", "", "Atespace the actor lives in")
	_ = waitActorCmd.MarkFlagRequired("atespace")
	waitActorCmd.Flags().StringVar(&waitActorFor, "for", "", "Condition to wait for, in status=<status> format")
	_ = waitActorCmd.MarkFlagRequired("for")
	waitActorCmd.Flags().DurationVar(&waitActorTimeout, "timeout", 30*time.Second, "Maximum time to wait; zero checks once without waiting")
	waitCmd.AddCommand(waitActorCmd)
}
