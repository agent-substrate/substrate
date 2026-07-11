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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var followLogs bool
var logsAtespaceFlag string

var logsActorsCmd = &cobra.Command{
	Use:     "actors <actor-name>",
	Aliases: []string{"actor"},
	Short:   "Stream logs for a specific actor",
	Args:    cobra.ExactArgs(1),
	RunE:    runLogsActor,
}

func init() {
	logsActorsCmd.Flags().BoolVarP(&followLogs, "follow", "f", false, "Specify if the logs should be streamed.")
	logsActorsCmd.Flags().StringVarP(&logsAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in")
	_ = logsActorsCmd.MarkFlagRequired("atespace")
	logsCmd.AddCommand(logsActorsCmd)
}

// AteAPIClient abstracts the gRPC client calls.
type AteAPIClient interface {
	GetActor(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error)
	Close()
}

// PodLogsStreamer abstracts log streaming from pods.
type PodLogsStreamer interface {
	StreamLogs(ctx context.Context, namespace, podName string, opts *corev1.PodLogOptions) (io.ReadCloser, error)
}

// k8sPodLogsStreamer implements PodLogsStreamer using Kubernetes Clientset.
type k8sPodLogsStreamer struct {
	clientset kubernetes.Interface
}

func (s *k8sPodLogsStreamer) StreamLogs(ctx context.Context, namespace, podName string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	return s.clientset.CoreV1().Pods(namespace).GetLogs(podName, opts).Stream(ctx)
}

// LogsActorRunner executes the log printing or streaming.
type LogsActorRunner struct {
	apiClient         AteAPIClient
	streamer          PodLogsStreamer
	atespace          string
	stdout            io.Writer
	stderr            io.Writer
	follow            bool
	pollInterval      time.Duration
	reconnectInterval time.Duration
	tickerInterval    time.Duration
}

// Run executes the logs command.
func (r *LogsActorRunner) Run(ctx context.Context, actorName string) error {
	if r.pollInterval <= 0 {
		r.pollInterval = 2 * time.Second
	}
	if r.reconnectInterval <= 0 {
		r.reconnectInterval = 1 * time.Second
	}
	if r.tickerInterval <= 0 {
		r.tickerInterval = 2 * time.Second
	}

	defer r.apiClient.Close()
	if r.follow {
		return r.runFollow(ctx, actorName)
	}
	return r.runOneShot(ctx, actorName)
}

func (r *LogsActorRunner) runOneShot(ctx context.Context, actorName string) error {
	actor, err := r.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: r.atespace, Name: actorName}})
	if err != nil {
		return fmt.Errorf("failed to get actor: %w", err)
	}

	podName := actor.GetAteomPodName()
	namespace := actor.GetAteomPodNamespace()

	if podName == "" || namespace == "" || actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		return fmt.Errorf("actor %s is not currently running on any worker pod", actorName)
	}

	opts := &corev1.PodLogOptions{
		Follow: false,
	}

	stream, err := r.streamer.StreamLogs(ctx, namespace, podName, opts)
	if err != nil {
		return fmt.Errorf("failed to stream logs from pod %s: %w", podName, err)
	}
	defer stream.Close()

	scanner := bufio.NewScanner(stream)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024) // Support up to 1MB lines
	for scanner.Scan() {
		line := scanner.Text()
		filterAndDisplayLogLine(line, actorName, r.stdout)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading log stream: %w", err)
	}
	return nil
}

func (r *LogsActorRunner) runFollow(ctx context.Context, actorName string) error {
	var lastWorkerPod string
	var lastSeenTime time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		actor, err := r.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: r.atespace, Name: actorName}})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return fmt.Errorf("actor %s not found: %w", actorName, err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.pollInterval):
				continue
			}
		}

		podName := actor.GetAteomPodName()
		namespace := actor.GetAteomPodNamespace()

		if podName == "" || namespace == "" || actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.pollInterval):
				continue
			}
		}

		// actor is resumed on anther worker
		if podName != lastWorkerPod {
			fmt.Fprintf(r.stderr, "Actor is currently running on pod %s/%s\n", namespace, podName)
			lastWorkerPod = podName
		}

		opts := &corev1.PodLogOptions{
			Follow: true,
		}
		if !lastSeenTime.IsZero() {
			opts.SinceTime = &metav1.Time{Time: lastSeenTime}
		}

		streamCtx, streamCancel := context.WithCancel(ctx)
		stream, err := r.streamer.StreamLogs(streamCtx, namespace, podName, opts)
		if err != nil {
			streamCancel()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.pollInterval):
				continue
			}
		}

		var wg sync.WaitGroup
		r.startMigrationMonitor(streamCtx, streamCancel, &wg, actorName, podName)

		scanner := bufio.NewScanner(stream)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024) // Support up to 1MB lines
		for scanner.Scan() {
			line := scanner.Text()
			logTime, _ := filterAndDisplayLogLine(line, actorName, r.stdout)
			if !logTime.IsZero() {
				lastSeenTime = logTime
			}
		}
		scanErr := scanner.Err()
		stream.Close()
		streamCancel()
		wg.Wait()

		if scanErr != nil {
			if errors.Is(scanErr, bufio.ErrTooLong) {
				return fmt.Errorf("log line exceeded buffer limit: %w", scanErr)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !errors.Is(scanErr, context.Canceled) {
				fmt.Fprintf(r.stderr, "Error reading log stream: %v. Reconnecting...\n", scanErr)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.reconnectInterval):
		}
	}
}

// startMigrationMonitor launches a background goroutine to query the control plane
// and aborts the stream context if the actor is suspended and then resumed to a different pod.
func (r *LogsActorRunner) startMigrationMonitor(
	ctx context.Context,
	cancel context.CancelFunc,
	wg *sync.WaitGroup,
	actorName string,
	currentPod string,
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(r.tickerInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				resp, err := r.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: r.atespace, Name: actorName}})
				if err == nil {
					act := resp
					if act.GetStatus() != ateapipb.Actor_STATUS_RUNNING || act.GetAteomPodName() != currentPod {
						// Actor suspended or migrated! Cancel stream context to reconnect.
						cancel()
						return
					}
				}
			}
		}
	}()
}

func runLogsActor(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	actorName := args[0]

	apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}

	k8sClient, err := ateclient.NewK8sClientset(kubeconfig, k8sContext)
	if err != nil {
		apiClient.Close()
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	runner := &LogsActorRunner{
		apiClient:         apiClient,
		streamer:          &k8sPodLogsStreamer{clientset: k8sClient},
		atespace:          logsAtespaceFlag,
		stdout:            os.Stdout,
		stderr:            os.Stderr,
		follow:            followLogs,
		pollInterval:      2 * time.Second,
		reconnectInterval: 1 * time.Second,
		tickerInterval:    2 * time.Second,
	}

	return runner.Run(ctx, actorName)
}

func filterAndDisplayLogLine(line, targetActorName string, w io.Writer) (time.Time, bool) {
	var m map[string]any
	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return time.Time{}, false
	}

	var logTime time.Time
	if tVal, ok := m["time"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, tVal); err == nil {
			logTime = t
		} else if t, err := time.Parse(time.RFC3339, tVal); err == nil {
			logTime = t
		}
	}

	var actorName string
	for _, labelKey := range []string{"logging.googleapis.com/labels", "labels"} {
		if labelsAny, ok := m[labelKey]; ok {
			if labels, ok := labelsAny.(map[string]any); ok {
				if name, ok := labels["ate.dev/actor_name"].(string); ok && name != "" {
					actorName = name
					break
				}
			}
		}
	}

	matched := (actorName != "" && actorName == targetActorName)

	if !matched {
		return logTime, false
	}

	// remove actor labels from CLI output
	for _, labelKey := range []string{"logging.googleapis.com/labels", "labels"} {
		if labelsAny, ok := m[labelKey]; ok {
			if labels, ok := labelsAny.(map[string]any); ok {
				for k := range labels {
					if strings.HasPrefix(k, "ate.dev/") {
						delete(labels, k)
					}
				}
				if len(labels) == 0 {
					delete(m, labelKey)
				}
			}
		}
	}

	timeVal, hasTime := m["time"]
	if hasTime {
		delete(m, "time")
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		return logTime, false
	}

	encodedStr := strings.TrimSpace(buf.String())
	if hasTime {
		timeJSON, _ := json.Marshal(timeVal)
		if encodedStr == "{}" {
			fmt.Fprintf(w, `{"time":%s}`+"\n", string(timeJSON))
		} else if strings.HasPrefix(encodedStr, "{") {
			fmt.Fprintf(w, `{"time":%s,%s`+"\n", string(timeJSON), encodedStr[1:])
		} else {
			fmt.Fprintln(w, encodedStr)
		}
	} else {
		fmt.Fprintln(w, encodedStr)
	}

	return logTime, true
}
