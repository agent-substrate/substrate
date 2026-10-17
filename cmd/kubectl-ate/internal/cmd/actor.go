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

	"github.com/spf13/cobra"
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var (
	getActorAtespaceFlag     string
	getActorAllAtespacesFlag bool
	createActorAtespaceFlag  string
	createActorTemplateFlag  string
	createActorTagFlag       string
	deleteActorAtespaceFlag  string
	deleteActorAnyStateFlag  bool
	pauseActorAtespaceFlag   string
	resumeActorAtespaceFlag  string
	suspendActorAtespaceFlag string
	logsActorAtespaceFlag    string
	logsActorFollowFlag      bool
	logsActorContainerFlag   string
	logsActorSourceFlag      string
)

var getActorsCmd = &cobra.Command{
	Use:     "actor [actor-name ...]",
	Aliases: []string{"actors"},
	Short:   "List all actors or get one or more actors",
	Args:    cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := validateAtespaceFlags("actors", args, getActorAtespaceFlag, getActorAllAtespacesFlag); err != nil {
			return err
		}

		ctx := cmd.Context()
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		if len(args) > 0 {
			actors := make([]*ateapipb.Actor, 0, len(args))
			for _, actorName := range args {
				actorRef := resources.ActorRef{Atespace: getActorAtespaceFlag, Name: actorName}
				resp, err := apiClient.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef.ToObjectRef()})
				if err != nil {
					return fmt.Errorf("failed to get actor %q: %w", actorName, err)
				}
				actors = append(actors, resp)
			}
			if len(actors) == 1 {
				return printer.PrintActorTo(cmd.OutOrStdout(), actors[0], outputFmt)
			}
			return printer.PrintActorsTo(cmd.OutOrStdout(), actors, outputFmt)
		}

		var allActors []*ateapipb.Actor
		pageToken := ""
		for {
			resp, err := apiClient.ListActors(ctx, &ateapipb.ListActorsRequest{
				Options:  &ateapipb.ListOptions{PageSize: 1000, PageToken: pageToken},
				Atespace: getActorAtespaceFlag,
			})
			if err != nil {
				return fmt.Errorf("failed to list actors: %w", err)
			}
			allActors = append(allActors, resp.GetActors()...)

			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}

		return printer.PrintActorsTo(cmd.OutOrStdout(), allActors, outputFmt)
	},
}

var createActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Create an actor",
	Args:  exactlyOneArg("<actor-name>", "atespace", "template"),
	RunE: func(cmd *cobra.Command, args []string) error {
		request, err := buildCreateActorRequest(args[0], createActorAtespaceFlag, createActorTemplateFlag, createActorTagFlag)
		if err != nil {
			return wrapUsageError(err)
		}

		ctx := cmd.Context()
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		resp, err := apiClient.CreateActor(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to create actor: %w", err)
		}

		return printer.PrintActorTo(cmd.OutOrStdout(), resp, outputFmt)
	},
}

var deleteActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Delete an actor",
	Args:  exactlyOneArg("<actor-name>", "atespace"),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		c, err := newAPIClient(cmd)
		if err != nil {
			return err
		}
		defer c.Close()

		actorRef := resources.ActorRef{Atespace: deleteActorAtespaceFlag, Name: args[0]}
		_, err = c.ControlClient.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor:    actorRef.ToObjectRef(),
			AnyState: deleteActorAnyStateFlag,
		})
		if err != nil {
			return err
		}

		fmt.Printf("actor %q deleted\n", actorRef.Name)
		return nil
	},
}

var pauseActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Pause an actor",
	Args:  exactlyOneArg("<actor-name>", "atespace"),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		actorRef := resources.ActorRef{Atespace: pauseActorAtespaceFlag, Name: args[0]}
		resp, err := apiClient.SuspendActor(ctx, buildSuspendActorRequest(actorRef, true))
		if err != nil {
			return fmt.Errorf("failed to pause actor: %w", err)
		}

		return printer.PrintActorTo(cmd.OutOrStdout(), resp.GetActor(), outputFmt)
	},
}

var resumeActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Resume an actor",
	Args:  exactlyOneArg("<actor-name>", "atespace"),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		actorRef := resources.ActorRef{Atespace: resumeActorAtespaceFlag, Name: args[0]}
		resp, err := apiClient.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
			Actor: actorRef.ToObjectRef(),
		})
		if err != nil {
			return fmt.Errorf("failed to resume actor: %w", err)
		}

		return printer.PrintActorTo(cmd.OutOrStdout(), resp.GetActor(), outputFmt)
	},
}

var suspendActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Suspend an actor",
	Args:  exactlyOneArg("<actor-name>", "atespace"),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		actorRef := resources.ActorRef{Atespace: suspendActorAtespaceFlag, Name: args[0]}
		resp, err := apiClient.SuspendActor(ctx, buildSuspendActorRequest(actorRef, false))
		if err != nil {
			return fmt.Errorf("failed to suspend actor: %w", err)
		}

		return printer.PrintActorTo(cmd.OutOrStdout(), resp.GetActor(), outputFmt)
	},
}

var logsActorsCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Stream logs for a specific actor",
	Args:  exactlyOneArg("<actor-name>", "atespace"),
	RunE:  runLogsActor,
}

// buildSuspendActorRequest builds the request behind both pause and suspend.
// A pause leaves the snapshot on the node, a suspend waits for it to reach
// object storage.
func buildSuspendActorRequest(actorRef resources.ActorRef, pause bool) *ateapipb.SuspendActorRequest {
	mode := ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS
	if pause {
		mode = ateapipb.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS
	}
	return &ateapipb.SuspendActorRequest{
		Actor:   actorRef.ToObjectRef(),
		Options: &ateapipb.SuspendOptions{Mode: mode},
	}
}

func buildCreateActorRequest(actorName, atespace, template, tag string) (*ateapipb.CreateActorRequest, error) {
	templateRef, err := parseAtespacedName(template, atespace)
	if err != nil {
		return nil, err
	}
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: atespace,
			Name:     actorName,
		},
		ActorTemplate: templateRef,
	}

	if tag != "" {
		ref, err := parseAtespacedName(tag, atespace)
		if err != nil {
			return nil, err
		}
		actor.SourceTag = ref
	}
	return &ateapipb.CreateActorRequest{Actor: actor}, nil
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
	apiClient AteAPIClient
	streamer  PodLogsStreamer
	actorRef  resources.ActorRef
	stdout    io.Writer
	stderr    io.Writer
	follow    bool
	// filter selects the lines to display; see newLogLineFilter.
	filter            logLineFilter
	pollInterval      time.Duration
	reconnectInterval time.Duration
	tickerInterval    time.Duration
}

// Run executes the logs command.
func (r *LogsActorRunner) Run(ctx context.Context) error {
	defer r.apiClient.Close()

	if r.pollInterval <= 0 {
		r.pollInterval = 2 * time.Second
	}
	if r.reconnectInterval <= 0 {
		r.reconnectInterval = 1 * time.Second
	}
	if r.tickerInterval <= 0 {
		r.tickerInterval = 2 * time.Second
	}

	if r.follow {
		return r.runFollow(ctx)
	}
	return r.runOneShot(ctx)
}

func (r *LogsActorRunner) runOneShot(ctx context.Context) error {
	actor, err := r.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{Actor: r.actorRef.ToObjectRef()})
	if err != nil {
		return fmt.Errorf("failed to get actor: %w", err)
	}

	podName := actor.GetStatus().GetWorkerAssignment().GetWorkerPod()
	namespace := actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace()

	if podName == "" || namespace == "" || actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return fmt.Errorf("actor %s is not currently running on any worker pod", r.actorRef)
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
		filterAndDisplayLogLine(line, r.filter, r.stdout, r.stderr)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading log stream: %w", err)
	}
	return nil
}

func (r *LogsActorRunner) runFollow(ctx context.Context) error {
	var lastWorkerPod string
	// lastSeenTime is the kubelet's timestamp on the last line read from
	// lastWorkerPod, the resume cursor for a reconnect to it.
	var lastSeenTime time.Time
	var waitingLogged bool
	var lastWaitingState ateapipb.ActorState

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		actor, err := r.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{Actor: r.actorRef.ToObjectRef()})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return fmt.Errorf("actor %s not found: %w", r.actorRef, err)
			}
			vlogf(r.stderr, vBasic, "Failed to get actor %s: %v; retrying in %s", r.actorRef, err, r.pollInterval)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.pollInterval):
				continue
			}
		}

		podName := actor.GetStatus().GetWorkerAssignment().GetWorkerPod()
		namespace := actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace()
		state := actor.GetStatus().GetState()

		if podName == "" || namespace == "" || state != ateapipb.ActorState_ACTOR_STATE_RUNNING {
			if !waitingLogged || state != lastWaitingState {
				vlogf(r.stderr, vBasic, "Actor %s is not running (state=%s); waiting for worker assignment...", r.actorRef, state)
				waitingLogged = true
				lastWaitingState = state
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.pollInterval):
				continue
			}
		}
		waitingLogged = false

		// The actor resumed on another worker.
		switchedPod := podName != lastWorkerPod
		if switchedPod {
			fmt.Fprintf(r.stderr, "Actor is currently running on pod %s/%s\n", namespace, podName)
			lastWorkerPod = podName
		}

		// The kubelet's own timestamp on every line is the resume cursor. It
		// is what SinceTime is compared against, and it does not depend on the
		// line's content: an actor's record keeps a time the actor wrote, which
		// can run ahead of the node's clock and would make a reconnect skip
		// lines.
		opts := &corev1.PodLogOptions{
			Follow:     true,
			Timestamps: true,
		}
		if !lastSeenTime.IsZero() {
			opts.SinceTime = &metav1.Time{Time: lastSeenTime}
		}

		streamCtx, streamCancel := context.WithCancel(ctx)
		stream, err := r.streamer.StreamLogs(streamCtx, namespace, podName, opts)
		if err != nil {
			streamCancel()
			vlogf(r.stderr, vBasic, "Failed to stream logs from pod %s/%s: %v; retrying in %s", namespace, podName, err, r.pollInterval)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.pollInterval):
				continue
			}
		}

		var wg sync.WaitGroup
		r.startMigrationMonitor(streamCtx, streamCancel, &wg, podName)

		scanner := bufio.NewScanner(stream)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024) // Support up to 1MB lines
		for scanner.Scan() {
			stamp, line, ok := splitKubeletTimestamp(scanner.Text())
			if ok {
				// SinceTime has second precision, so a reconnect to the same pod
				// re-reads the cursor's second; lines up to the cursor were
				// already shown. Another pod's lines are all new, and its node's
				// clock is not this one's, so they are not compared.
				if !switchedPod && !stamp.After(lastSeenTime) {
					continue
				}
				lastSeenTime = stamp
			} else {
				vlogf(r.stderr, vTrace, "Log line had no kubelet timestamp prefix")
			}
			filterAndDisplayLogLine(line, r.filter, r.stdout, r.stderr)
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
				resp, err := r.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{Actor: r.actorRef.ToObjectRef()})
				if err != nil {
					if ctx.Err() == nil {
						vlogf(r.stderr, vBasic, "Migration monitor failed to get actor %s: %v", r.actorRef, err)
					}
					continue
				}
				act := resp
				if act.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING || act.GetStatus().GetWorkerAssignment().GetWorkerPod() != currentPod {
					// Actor suspended or migrated! Cancel stream context to reconnect.
					vlogf(r.stderr, vBasic, "Actor %s left pod %s (state=%s, pod=%s); closing current log stream",
						r.actorRef, currentPod, act.GetStatus().GetState(), act.GetStatus().GetWorkerAssignment().GetWorkerPod())
					cancel()
					return
				}
			}
		}
	}()
}

// splitKubeletTimestamp splits the timestamp the kubelet prefixes to a log
// line when PodLogOptions.Timestamps is set (RFC 3339 with nanoseconds, then
// a space) from the line itself. ok is false for a line without one, which
// is then returned whole.
func splitKubeletTimestamp(line string) (stamp time.Time, rest string, ok bool) {
	stampText, rest, found := strings.Cut(line, " ")
	if !found {
		return time.Time{}, line, false
	}
	stamp, err := time.Parse(time.RFC3339Nano, stampText)
	if err != nil {
		return time.Time{}, line, false
	}
	return stamp, rest, true
}

func runLogsActor(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Before any connection: a bad flag combination is answered as such, not
	// as an unreachable server.
	actorRef := resources.ActorRef{Atespace: logsActorAtespaceFlag, Name: args[0]}
	filter, err := newLogLineFilter(actorRef, logsActorSourceFlag, logsActorContainerFlag)
	if err != nil {
		return wrapUsageError(err)
	}

	apiClient, err := newAPIClient(cmd)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}

	config, err := loadKubeConfig(cmd.ErrOrStderr(), kubeconfig, k8sContext)
	if err != nil {
		apiClient.Close()
		return fmt.Errorf("while loading kubeconfig: %w", err)
	}
	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		apiClient.Close()
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	runner := &LogsActorRunner{
		apiClient:         apiClient,
		streamer:          &k8sPodLogsStreamer{clientset: k8sClient},
		actorRef:          actorRef,
		stdout:            os.Stdout,
		stderr:            os.Stderr,
		follow:            logsActorFollowFlag,
		filter:            filter,
		pollInterval:      2 * time.Second,
		reconnectInterval: 1 * time.Second,
		tickerInterval:    2 * time.Second,
	}

	return runner.Run(ctx)
}

// logSource names a class of lines in an actor's log stream, selected with
// --source. Container output carries the container's name; the lifecycle
// events worker emits around a start, checkpoint, restore, or termination
// carry none, which is what tells the two apart.
type logSource string

const (
	logSourceAll        logSource = "all"
	logSourceContainers logSource = "containers"
	logSourceLifecycle  logSource = "lifecycle"
)

// logLineFilter selects which of an actor's log lines are displayed: all of
// them by default, one class of them when source is set, or only the named
// container's when container is set.
type logLineFilter struct {
	target    resources.ActorRef
	source    logSource
	container string
}

// newLogLineFilter builds the filter for target from the --source and
// --container flag values, rejecting combinations that could never match.
func newLogLineFilter(target resources.ActorRef, source, container string) (logLineFilter, error) {
	switch logSource(source) {
	case logSourceAll, logSourceContainers:
	case logSourceLifecycle:
		if container != "" {
			return logLineFilter{}, fmt.Errorf("--container cannot be combined with --source=lifecycle: lifecycle events are not emitted by a container")
		}
	default:
		return logLineFilter{}, fmt.Errorf("invalid --source %q: must be one of %s, %s, %s", source, logSourceAll, logSourceContainers, logSourceLifecycle)
	}
	return logLineFilter{target: target, source: logSource(source), container: container}, nil
}

// matches reports whether a line emitted by emitter from containerName (empty
// for lifecycle events) should be displayed.
func (f logLineFilter) matches(emitter resources.ActorRef, containerName string) bool {
	// Actor names are only unique within an atespace, and a worker pod can host
	// actors from different atespaces over time, so match on both.
	if emitter != f.target || f.target.Atespace == "" || f.target.Name == "" {
		return false
	}
	switch f.source {
	case logSourceLifecycle:
		// f.container is empty here: newLogLineFilter rejects it with lifecycle.
		return containerName == ""
	case logSourceContainers:
		if containerName == "" {
			return false
		}
	}
	if f.container == "" {
		return containerName != ocispec.PauseContainer
	}
	return containerName == f.container
}

// containerNameKeyQuoted is the container-name label key as it appears in a
// JSON line. Every container line carries it (ateattr.ActorLogLabels) and
// no lifecycle line does; an actor cannot forge it, since actorlog strips
// the reserved keys from an actor's own labels, and the text inside a value
// only ever makes a container line skip early.
var containerNameKeyQuoted = `"` + string(ateattr.ActorContainerNameKey) + `"`

// filterAndDisplayLogLine writes line to w if filter selects it, and
// reports whether it did.
func filterAndDisplayLogLine(line string, filter logLineFilter, w, stderr io.Writer) bool {
	// Under lifecycle, nearly every line is a container's; a busy actor
	// writes many, so skip them before decoding.
	if filter.source == logSourceLifecycle && strings.Contains(line, containerNameKeyQuoted) {
		return false
	}

	var m map[string]any
	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		vlogf(stderr, vTrace, "Skipping non-JSON log line: %v", err)
		return false
	}

	var emitter resources.ActorRef
	var emitterContainer string
	for _, labelKey := range []string{"logging.googleapis.com/labels", "labels"} {
		if labelsAny, ok := m[labelKey]; ok {
			if labels, ok := labelsAny.(map[string]any); ok {
				if name, ok := labels[string(ateattr.ActorNameKey)].(string); ok && name != "" {
					emitter.Name = name
					emitter.Atespace, _ = labels[string(ateattr.AtespaceKey)].(string)
					emitterContainer, _ = labels[string(ateattr.ActorContainerNameKey)].(string)
					break
				}
			}
		}
	}

	if !filter.matches(emitter, emitterContainer) {
		return false
	}

	// Remove substrate's labels from CLI output. Stripping the whole reserved
	// prefix rather than the known keys means an actor cannot get a plausible
	// ate.*-namespaced label of its own printed as platform attribution.
	for _, labelKey := range []string{"logging.googleapis.com/labels", "labels"} {
		if labelsAny, ok := m[labelKey]; ok {
			if labels, ok := labelsAny.(map[string]any); ok {
				for k := range labels {
					if strings.HasPrefix(k, ateattr.ReservedNamespace) {
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
		return false
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

	return true
}

func init() {
	getActorsCmd.Flags().StringVarP(&getActorAtespaceFlag, "atespace", "a", "", "get actors in a specific atespace (mutually exclusive with --all-atespaces)")
	getActorsCmd.Flags().BoolVarP(&getActorAllAtespacesFlag, "all-atespaces", "A", false, "get actors across all atespaces (mutually exclusive with --atespace)")
	getCmd.AddCommand(getActorsCmd)

	createActorCmd.Flags().StringVar(&createActorTemplateFlag, "template", "", "actor-template to derive the actor from, as <atespace>/<template-name>, or just <template-name> to use the actor's own atespace (see --atespace)")
	createActorCmd.Flags().StringVarP(&createActorAtespaceFlag, "atespace", "a", "", "atespace to create the actor in")
	createActorCmd.Flags().StringVar(&createActorTagFlag, "tag", "", "tag to initialize the actor from, as <atespace>/<tag-name>, or just <tag-name> to use the actor's own atespace (see --atespace)")
	createCmd.AddCommand(createActorCmd)

	deleteActorCmd.Flags().StringVarP(&deleteActorAtespaceFlag, "atespace", "a", "", "the actor's atespace")
	deleteActorCmd.Flags().BoolVar(&deleteActorAnyStateFlag, "any-state", false, "delete the actor regardless of current state")
	deleteCmd.AddCommand(deleteActorCmd)

	pauseActorCmd.Flags().StringVarP(&pauseActorAtespaceFlag, "atespace", "a", "", "the actor's atespace")
	pauseCmd.AddCommand(pauseActorCmd)

	resumeActorCmd.Flags().StringVarP(&resumeActorAtespaceFlag, "atespace", "a", "", "the actor's atespace")
	resumeCmd.AddCommand(resumeActorCmd)

	suspendActorCmd.Flags().StringVarP(&suspendActorAtespaceFlag, "atespace", "a", "", "the actor's atespace")
	suspendCmd.AddCommand(suspendActorCmd)

	logsActorsCmd.Flags().BoolVarP(&logsActorFollowFlag, "follow", "f", false, "stream logs continuously")
	logsActorsCmd.Flags().StringVarP(&logsActorAtespaceFlag, "atespace", "a", "", "the actor's atespace")
	logsActorsCmd.Flags().StringVarP(&logsActorContainerFlag, "container", "c", "", "show only logs from a named container; use _pause for runsc diagnostics")
	logsActorsCmd.Flags().StringVar(&logsActorSourceFlag, "source", string(logSourceAll), "which lines to show: all, containers (container output, no lifecycle events), or lifecycle (only the actor's lifecycle events)")
	logsCmd.AddCommand(logsActorsCmd)
}
