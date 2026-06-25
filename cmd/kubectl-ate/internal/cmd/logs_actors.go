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

var (
	followLogs       bool
	containerFlag    string
	supervisorFlag   bool
	logsAtespaceFlag string
)

var logsActorsCmd = &cobra.Command{
	Use:     "actors <actor-id>",
	Aliases: []string{"actor"},
	Short:   "Stream logs for a specific actor",
	Args:    cobra.ExactArgs(1),
	RunE:    runLogsActor,
}

func init() {
	logsActorsCmd.Flags().BoolVarP(&followLogs, "follow", "f", false, "Specify if the logs should be streamed.")
	logsActorsCmd.Flags().StringVarP(&logsAtespaceFlag, "atespace", "a", "", "Atespace the actor lives in")
	_ = logsActorsCmd.MarkFlagRequired("atespace")
	logsActorsCmd.Flags().StringVarP(&containerFlag, "container", "c", "", "Show logs only from the named container within the actor. Mutually exclusive with --supervisor.")
	logsActorsCmd.Flags().BoolVar(&supervisorFlag, "supervisor", false, "Show only the ateom supervisor (lifecycle) logs. Mutually exclusive with --container.")
	logsCmd.AddCommand(logsActorsCmd)
}

// AteAPIClient abstracts the gRPC client calls.
type AteAPIClient interface {
	GetActor(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error)
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
	container         string
	supervisor        bool
	pollInterval      time.Duration
	reconnectInterval time.Duration
	tickerInterval    time.Duration
}

// Run executes the logs command.
func (r *LogsActorRunner) Run(ctx context.Context, actorID string) error {
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
	if err := validateLogFilterFlags(r.container, r.supervisor); err != nil {
		return err
	}
	if r.follow {
		return r.runFollow(ctx, actorID)
	}
	return r.runOneShot(ctx, actorID)
}

func (r *LogsActorRunner) runOneShot(ctx context.Context, actorID string) error {
	actorResp, err := r.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: r.atespace, Name: actorID}})
	if err != nil {
		return fmt.Errorf("failed to get actor: %w", err)
	}

	actor := actorResp.GetActor()
	podName := actor.GetAteomPodName()
	namespace := actor.GetAteomPodNamespace()

	if podName == "" || namespace == "" || actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		return fmt.Errorf("actor %s is not currently running on any worker pod", actorID)
	}

	opts := &corev1.PodLogOptions{
		Follow: false,
	}

	stream, err := r.streamer.StreamLogs(ctx, namespace, podName, opts)
	if err != nil {
		return fmt.Errorf("failed to stream logs from pod %s: %w", podName, err)
	}
	defer stream.Close()

	filter := logLineFilter{actorID: actorID, container: r.container, supervisor: r.supervisor}
	scanner := bufio.NewScanner(stream)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024) // Support up to 1MB lines
	for scanner.Scan() {
		line := scanner.Text()
		filterAndDisplayLogLine(line, filter, r.stdout)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading log stream: %w", err)
	}
	return nil
}

func (r *LogsActorRunner) runFollow(ctx context.Context, actorID string) error {
	var lastWorkerPod string
	var lastSeenTime time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		actorResp, err := r.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: r.atespace, Name: actorID}})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return fmt.Errorf("actor %s not found: %w", actorID, err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.pollInterval):
				continue
			}
		}

		actor := actorResp.GetActor()
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
		r.startMigrationMonitor(streamCtx, streamCancel, &wg, actorID, podName)

		filter := logLineFilter{actorID: actorID, container: r.container, supervisor: r.supervisor}
		scanner := bufio.NewScanner(stream)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024) // Support up to 1MB lines
		for scanner.Scan() {
			line := scanner.Text()
			logTime, _ := filterAndDisplayLogLine(line, filter, r.stdout)
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
	actorID string,
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
				resp, err := r.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: r.atespace, Name: actorID}})
				if err == nil {
					act := resp.GetActor()
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

// validateLogFilterFlags rejects the one unsupported filter combination: a
// container filter and the supervisor filter cannot be requested together.
func validateLogFilterFlags(container string, supervisor bool) error {
	if container != "" && supervisor {
		return fmt.Errorf("--container and --supervisor are mutually exclusive; specify only one")
	}
	return nil
}

func runLogsActor(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	actorID := args[0]

	if err := validateLogFilterFlags(containerFlag, supervisorFlag); err != nil {
		return err
	}

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
		container:         containerFlag,
		supervisor:        supervisorFlag,
		pollInterval:      2 * time.Second,
		reconnectInterval: 1 * time.Second,
		tickerInterval:    2 * time.Second,
	}

	return runner.Run(ctx, actorID)
}

// logLineFilter selects which of an actor's log lines are displayed. container
// and supervisor are mutually exclusive; zero values show all of the lines.
type logLineFilter struct {
	actorID    string
	container  string // when non-empty, show only lines from this container
	supervisor bool   // when true, show only supervisor (non-container) lines
}

// matchesSource reports whether a line from the given container (empty for
// supervisor lines) passes the filter's source selection.
func (f logLineFilter) matchesSource(containerName string) bool {
	switch {
	case f.supervisor:
		return containerName == ""
	case f.container != "":
		return containerName == f.container
	default:
		return true
	}
}

// Reserved Substrate log labels, written under one of logActorLabelKeys.
const (
	ateLabelPrefix        = "ate.dev/"
	ateActorIDLabel       = ateLabelPrefix + "actor_id"
	ateContainerNameLabel = ateLabelPrefix + "container_name"
)

// logActorLabelKeys are the keys a line may carry labels under, GCE key first.
var logActorLabelKeys = []string{"logging.googleapis.com/labels", "labels"}

// filterAndDisplayLogLine writes the cleaned line (ate.dev labels stripped) to w
// when it belongs to the target actor and passes the filter. It returns the
// line's timestamp only for displayed lines (zero otherwise), so follow mode
// never advances its resume cursor past a filtered-out or foreign line and skips
// logs on reconnect. The bool reports whether the line was written.
func filterAndDisplayLogLine(line string, filter logLineFilter, w io.Writer) (time.Time, bool) {
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

	// Read actor_id and container_name from the same (first actor-identifying)
	// label map so a line's source is never split across the two keys.
	var actorID, containerName string
	for _, labelKey := range logActorLabelKeys {
		labelsAny, ok := m[labelKey]
		if !ok {
			continue
		}
		labels, ok := labelsAny.(map[string]any)
		if !ok {
			continue
		}
		id, _ := labels[ateActorIDLabel].(string)
		if id == "" {
			continue
		}
		actorID = id
		containerName, _ = labels[ateContainerNameLabel].(string)
		break
	}

	if actorID == "" || actorID != filter.actorID {
		return time.Time{}, false
	}

	if !filter.matchesSource(containerName) {
		return time.Time{}, false
	}

	// remove actor labels from CLI output
	for _, labelKey := range logActorLabelKeys {
		if labelsAny, ok := m[labelKey]; ok {
			if labels, ok := labelsAny.(map[string]any); ok {
				for k := range labels {
					if strings.HasPrefix(k, ateLabelPrefix) {
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
		return time.Time{}, false
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
