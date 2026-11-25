// Copyright 2026 The Agent Substrate Authors
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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/ateclient"
	ateenvpb "go.ate.dev/substrate/internal/proto/ateenv/v1alpha"
	"go.ate.dev/substrate/internal/resources"
)

const (
	// guestPort is where ate-env-guest serves its process API in the actor.
	guestPort = 80
	// stepTimeout caps one step. ate-env-guest kills a process after 1 h by
	// default, which a kernel build can exceed.
	stepTimeout = 24 * time.Hour
)

// apiPhases are the phases that record API calls in a script run, so steps
// can't use them as names.
var apiPhases = map[string]bool{
	"create": true, "egress-policy": true, "first-resume": true,
	"suspend": true, "resume": true, "delete": true,
}

// scriptStep is one line of a -script file: a shell command to run in the
// actor, or a suspend and resume.
type scriptStep struct {
	name    string // empty for a cycle
	command string
}

// parseScript reads a script. Each line is "run NAME COMMAND" or "cycle".
// Blank lines and lines that start with # are skipped.
func parseScript(in io.Reader) ([]scriptStep, error) {
	var steps []scriptStep
	sc := bufio.NewScanner(in)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		verb, rest, _ := strings.Cut(line, " ")
		switch verb {
		case "cycle":
			if strings.TrimSpace(rest) != "" {
				return nil, fmt.Errorf("line %d: cycle takes no arguments", n)
			}
			steps = append(steps, scriptStep{})
		case "run":
			name, cmd, _ := strings.Cut(strings.TrimSpace(rest), " ")
			if name == "" || strings.TrimSpace(cmd) == "" {
				return nil, fmt.Errorf("line %d: want \"run NAME COMMAND\"", n)
			}
			if apiPhases[name] {
				return nil, fmt.Errorf("line %d: step name %q is used for API calls", n, name)
			}
			steps = append(steps, scriptStep{name: name, command: strings.TrimSpace(cmd)})
		default:
			return nil, fmt.Errorf("line %d: unknown step %q", n, verb)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("script has no steps")
	}
	return steps, nil
}

// parseEnv turns KEY=VALUE flags into a map.
func parseEnv(kvs []string) (map[string]string, error) {
	env := map[string]string{}
	for _, kv := range kvs {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("-env %q: want KEY=VALUE", kv)
		}
		env[k] = v
	}
	return env, nil
}

// egressRules turns -allow-http HOST:PORT flags into egress rules.
func egressRules(hostPorts []string) ([]*ateapipb.EgressRule, error) {
	var rules []*ateapipb.EgressRule
	for _, hp := range hostPorts {
		host, port, err := splitHostPort(hp)
		if err != nil {
			return nil, err
		}
		rules = append(rules, &ateapipb.EgressRule{Http: &ateapipb.HttpRule{
			Hostnames: []string{host},
			Ports:     &ateapipb.Ports{Numbers: []int32{port}},
		}})
	}
	return rules, nil
}

func splitHostPort(hp string) (string, int32, error) {
	host, p, err := net.SplitHostPort(hp)
	if err != nil {
		return "", 0, fmt.Errorf("-allow-http %q: %w", hp, err)
	}
	var port int32
	if _, err := fmt.Sscan(p, &port); err != nil || port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("-allow-http %q: bad port", hp)
	}
	return host, port, nil
}

// script runs the -script steps on every actor at the same time. Each actor
// goes through create, an egress policy with rules, first resume, the steps
// and delete on its own, so a slow actor doesn't hold up the others.
func (r *runner) script(ctx context.Context, steps []scriptStep, env map[string]string, rules []*ateapipb.EgressRule) {
	dialer, err := ateclient.NewRouterDialer(ctx, *flagKubeconfig, *flagContext)
	if err != nil {
		fatal(fmt.Errorf("connect to atenet-router: %w", err))
	}
	logf("script: %d steps on %d actors", len(steps), len(r.actors))
	t0 := time.Now()
	var wg sync.WaitGroup
	for _, name := range r.actors {
		wg.Go(func() { r.runScript(ctx, t0, dialer, name, steps, env, rules) })
	}
	wg.Wait()
	r.summarizeScript(t0)
}

// runScript drives one actor. It stops at the first failed step and still
// deletes the actor, even after an interrupt.
func (r *runner) runScript(ctx context.Context, t0 time.Time, dialer *ateclient.RouterDialer, name string, steps []scriptStep, env map[string]string, rules []*ateapipb.EgressRule) {
	call := func(ctx context.Context, phase, op string, fn func(context.Context) (int, error)) bool {
		cctx, cancel := context.WithTimeout(ctx, *flagCallTimeout)
		defer cancel()
		start := time.Now()
		retries, err := fn(cctx)
		r.record(t0, start, time.Now(), phase, op, name, retries, err)
		return err == nil
	}
	created := call(ctx, "create", "CreateActor", func(cctx context.Context) (int, error) {
		return 0, r.createActor(cctx, name)
	})
	// An interrupt during CreateActor may still leave the actor behind.
	if created || ctx.Err() != nil {
		defer call(context.WithoutCancel(ctx), "delete", "DeleteActor", func(cctx context.Context) (int, error) {
			return r.retry.do(cctx, false, func() error {
				_, err := r.cli.DeleteActor(cctx, &ateapipb.DeleteActorRequest{Actor: r.ref(name)})
				if status.Code(err) == codes.NotFound {
					return nil
				}
				return err
			})
		})
	}
	if !created {
		return
	}
	if len(rules) > 0 && !call(ctx, "egress-policy", "CreateActorEgressPolicy", func(cctx context.Context) (int, error) {
		_, err := r.cli.CreateActorEgressPolicy(cctx, &ateapipb.CreateActorEgressPolicyRequest{
			Actor: r.ref(name),
			EgressPolicy: &ateapipb.EgressPolicy{
				Metadata: &ateapipb.ResourceMetadata{Atespace: *flagAtespace, Name: resources.SingletonName},
				Rules:    rules,
			},
		})
		return 0, err
	}) {
		return
	}
	if !call(ctx, "first-resume", "ResumeActorFirstResume", func(cctx context.Context) (int, error) {
		return r.resumeCall(cctx, name)
	}) {
		return
	}

	ref := resources.ActorRefFromObjectRef(r.ref(name))
	// conn is closed before each suspend and opened again for the next
	// command, so nothing reaches the actor while it is suspended.
	var conn *grpc.ClientConn
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	for _, s := range steps {
		if ctx.Err() != nil {
			return
		}
		if s.name == "" {
			if conn != nil {
				_ = conn.Close()
				conn = nil
			}
			if !call(ctx, "suspend", "SuspendActor", func(cctx context.Context) (int, error) {
				return r.suspendCall(cctx, name, ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS)
			}) {
				return
			}
			select {
			case <-time.After(*flagIdle):
			case <-ctx.Done():
				return
			}
			if !call(ctx, "resume", "ResumeActor", func(cctx context.Context) (int, error) {
				return r.resumeCall(cctx, name)
			}) {
				return
			}
			continue
		}
		if conn == nil {
			var err error
			conn, err = grpc.NewClient("passthrough:///"+name,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(dctx context.Context, _ string) (net.Conn, error) {
					return dialer.DialActor(dctx, ref, guestPort)
				}))
			if err != nil {
				now := time.Now()
				r.record(t0, now, now, s.name, "Run", name, 0, err)
				return
			}
		}
		if !r.runCommand(ctx, t0, ateenvpb.NewProcessServiceClient(conn), name, s, env) {
			return
		}
	}
}

// runCommand runs one step with sh -c in the actor and records its run time,
// which the guest measures from process start to exit. It polls GetProcess,
// so no request stays open longer than one poll.
func (r *runner) runCommand(ctx context.Context, t0 time.Time, guest ateenvpb.ProcessServiceClient, name string, s scriptStep, env map[string]string) bool {
	start := time.Now()
	fail := func(err error) bool {
		r.record(t0, start, time.Now(), s.name, "Run", name, 0, err)
		return false
	}
	// Right after a resume, the router can fail requests while the actor
	// settles. Wait until the guest answers a lookup of a process that
	// doesn't exist, then start the command once, so it never runs twice.
	if err := retryGuest(ctx, func(cctx context.Context) error {
		_, err := guest.GetProcess(cctx, &ateenvpb.GetProcessRequest{ProcessId: "wakestorm-ready-check"})
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return err
	}); err != nil {
		return fail(fmt.Errorf("guest not reachable: %w", err))
	}
	start = time.Now()
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	proc, err := guest.StartProcess(cctx, &ateenvpb.StartProcessRequest{
		Command: []string{"sh", "-c", s.command},
		Env:     maps.Clone(env),
		Timeout: durationpb.New(stepTimeout),
	})
	cancel()
	if err != nil {
		return fail(fmt.Errorf("StartProcess: %w", err))
	}
	for proc.GetState() != ateenvpb.ProcessState_PROCESS_STATE_EXITED {
		select {
		case <-time.After(r.poll):
		case <-ctx.Done():
			return fail(ctx.Err())
		}
		id := proc.GetProcessId()
		if err := retryGuest(ctx, func(cctx context.Context) error {
			var err error
			proc, err = guest.GetProcess(cctx, &ateenvpb.GetProcessRequest{ProcessId: id})
			return err
		}); err != nil {
			return fail(fmt.Errorf("GetProcess: %w", err))
		}
	}
	if proc.GetStartedAt() == nil || proc.GetFinishedAt() == nil {
		return fail(fmt.Errorf("process %s exited without start and finish times", proc.GetProcessId()))
	}
	end := start.Add(proc.GetFinishedAt().AsTime().Sub(proc.GetStartedAt().AsTime()))
	var runErr error
	if code := proc.GetExitCode(); code != 0 {
		runErr = status.Errorf(codes.Unknown, "exit %d, stderr: %q", code, stderrTail(ctx, guest, proc.GetProcessId()))
	}
	r.record(t0, start, end, s.name, "Run", name, 0, runErr)
	return runErr == nil
}

// stderrTail returns the last 200 bytes a finished process wrote to stderr.
func stderrTail(ctx context.Context, guest ateenvpb.ProcessServiceClient, id string) string {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	stream, err := guest.StreamProcessOutput(ctx, &ateenvpb.StreamProcessOutputRequest{
		ProcessId:    id,
		StdoutOffset: math.MaxInt64, // skip stdout
	})
	if err != nil {
		return fmt.Sprintf("unavailable: %v", err)
	}
	var out []byte
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Sprintf("unavailable: %v", err)
		}
		out = append(out, msg.GetStderr()...)
		if len(out) > 200 {
			out = out[len(out)-200:]
		}
	}
	return strings.TrimSpace(string(out))
}

// retryGuest calls fn until it succeeds, it fails with something other than
// Unavailable, or 30 attempts 2 s apart have failed.
func retryGuest(ctx context.Context, fn func(context.Context) error) error {
	var err error
	for range 30 {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err = fn(cctx)
		cancel()
		if status.Code(err) != codes.Unavailable {
			return err
		}
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

// record saves one timed call or step.
func (r *runner) record(t0, start, end time.Time, phase, op, actor string, retries int, err error) {
	rec := newCallRecord(t0, start, end, phase, 0, op, actor, retries, err)
	if err != nil {
		logf("%s %s on %s failed: %v", op, phase, actor, err)
	}
	r.mu.Lock()
	r.calls = append(r.calls, rec)
	r.mu.Unlock()
}

// summarizeScript groups the script's records by phase, in the order each
// phase first ran, and adds one summary per phase.
func (r *runner) summarizeScript(t0 time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	byPhase := map[string][]callRecord{}
	var order []string
	ops := map[string]string{}
	for _, c := range r.calls {
		if _, ok := byPhase[c.Phase]; !ok {
			order = append(order, c.Phase)
			ops[c.Phase] = c.Op
		}
		byPhase[c.Phase] = append(byPhase[c.Phase], c)
	}
	for _, p := range order {
		r.stats = append(r.stats, summarize(p, 0, ops[p], len(r.actors), t0, byPhase[p]))
	}
}

// readScript opens and parses the -script file.
func readScript(path string) ([]scriptStep, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	steps, err := parseScript(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return steps, nil
}

// stringList is a repeatable string flag.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }
