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

package glutton

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

// Script builders, so the tests read like the trees they send.

func writeDisk(key string, size int32) *gluttonpb.Request {
	return &gluttonpb.Request{Kind: &gluttonpb.Request_WriteDisk{WriteDisk: &gluttonpb.WriteDiskRequest{Key: key, Size: size}}}
}

func readDisk(key string) *gluttonpb.Request {
	return &gluttonpb.Request{Kind: &gluttonpb.Request_ReadDisk{ReadDisk: &gluttonpb.ReadDiskRequest{Key: key, ReadMode: gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY}}}
}

func writeRAM(key, size string) *gluttonpb.Request {
	return &gluttonpb.Request{Kind: &gluttonpb.Request_WriteRam{WriteRam: &gluttonpb.WriteRAMRequest{Key: key, Size: size}}}
}

func readRAM(key string) *gluttonpb.Request {
	return &gluttonpb.Request{Kind: &gluttonpb.Request_ReadRam{ReadRam: &gluttonpb.ReadRAMRequest{Key: key}}}
}

func burn(ms int64) *gluttonpb.Request {
	return &gluttonpb.Request{Kind: &gluttonpb.Request_BurnCpu{BurnCpu: &gluttonpb.BurnCPURequest{DurationMs: ms}}}
}

func sleepFor(ms int64) *gluttonpb.Request {
	return &gluttonpb.Request{Kind: &gluttonpb.Request_Sleep{Sleep: &gluttonpb.SleepRequest{DurationMs: ms}}}
}

// op is a step holding one operation of the given requests, run together.
func op(requests ...*gluttonpb.Request) *gluttonpb.Step {
	return &gluttonpb.Step{Kind: &gluttonpb.Step_Operation{Operation: &gluttonpb.Operation{Requests: requests}}}
}

// seq is a step holding a block of the given steps, run in order once.
func seq(steps ...*gluttonpb.Step) *gluttonpb.Step {
	return &gluttonpb.Step{Kind: &gluttonpb.Step_Block{Block: &gluttonpb.Block{Steps: steps}}}
}

// loop is a step holding a block that cycles its steps for budgetMs.
func loop(budgetMs int64, steps ...*gluttonpb.Step) *gluttonpb.Step {
	return &gluttonpb.Step{Kind: &gluttonpb.Step_Block{Block: &gluttonpb.Block{Steps: steps, LoopDurationMs: budgetMs}}}
}

func script(steps ...*gluttonpb.Step) *gluttonpb.RunScriptRequest {
	return &gluttonpb.RunScriptRequest{Script: &gluttonpb.Block{Steps: steps}}
}

func run(t *testing.T, svc *Service, req *gluttonpb.RunScriptRequest) *gluttonpb.StepResult {
	t.Helper()
	resp, err := svc.RunScript(context.Background(), req)
	if err != nil {
		t.Fatalf("RunScript: %v", err)
	}
	return resp.GetResult()
}

// Steps run in order, each request has its usual effect, and the result
// mirrors the tree with the totals summed to the root.
func TestRunScriptRunsStepsInOrderAndSumsStats(t *testing.T) {
	svc := newTestService(t)
	result := run(t, svc, script(
		op(writeDisk("scripted", 1024)),
		op(readDisk("scripted")),
		seq(op(writeRAM("arr", "8Ki")), op(readRAM("arr"))),
		op(burn(1), sleepFor(1)),
	))
	root := result.GetStats()
	if root.GetPasses() != 1 || root.GetRequestsRun() != 6 {
		t.Errorf("root passes=%d requests_run=%d, want 1/6", root.GetPasses(), root.GetRequestsRun())
	}
	if root.GetDiskBytesWritten() != 1024 || root.GetDiskBytesRead() != 1024 {
		t.Errorf("disk bytes written=%d read=%d, want 1024/1024", root.GetDiskBytesWritten(), root.GetDiskBytesRead())
	}
	if root.GetRamBytesWritten() != 8192 || root.GetRamBytesRead() != 8192 {
		t.Errorf("ram bytes written=%d read=%d, want 8192/8192", root.GetRamBytesWritten(), root.GetRamBytesRead())
	}
	if root.GetBurnIterations() < 1 || root.GetSleptMs() < 1 {
		t.Errorf("burn_iterations=%d slept_ms=%d, want both positive", root.GetBurnIterations(), root.GetSleptMs())
	}
	if len(result.GetChildren()) != 4 {
		t.Fatalf("root has %d children, want 4 steps", len(result.GetChildren()))
	}
	nested := result.GetChildren()[2]
	if len(nested.GetChildren()) != 2 || nested.GetStats().GetRequestsRun() != 2 || nested.GetStats().GetRamBytesWritten() != 8192 {
		t.Errorf("nested block = %v, want two children and the RAM totals", nested)
	}
	// An operation's children are its requests, each with its own stats.
	parallel := result.GetChildren()[3]
	if len(parallel.GetChildren()) != 2 || parallel.GetChildren()[0].GetStats().GetBurnIterations() < 1 || parallel.GetChildren()[1].GetStats().GetSleptMs() < 1 {
		t.Errorf("operation = %v, want a burn child and a sleep child", parallel)
	}
}

// Requests in one operation overlap: two sleeps of 100ms take about 100ms,
// and the operation's elapsed is its longest request, not their sum.
func TestRunScriptOperationRunsRequestsTogether(t *testing.T) {
	svc := newTestService(t)
	start := time.Now()
	result := run(t, svc, script(op(sleepFor(100), sleepFor(100), sleepFor(100))))
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Errorf("three parallel 100ms sleeps took %v", elapsed)
	}
	stats := result.GetChildren()[0].GetStats()
	if stats.GetSleptMs() < 300 || stats.GetElapsedMs() > 250 {
		t.Errorf("operation slept_ms=%d elapsed_ms=%d, want ~300 summed and ~100 wall", stats.GetSleptMs(), stats.GetElapsedMs())
	}
}

// A looped block cycles until its budget and finishes the pass it is in:
// every pass is whole, the loop overruns by at most one pass, and the
// stats count every pass.
func TestRunScriptLoopsUntilTheBudget(t *testing.T) {
	svc := newTestService(t)
	start := time.Now()
	result := run(t, svc, script(loop(100, op(sleepFor(30)), op(writeDisk("churn", 16)))))
	elapsed := time.Since(start)
	if elapsed < 100*time.Millisecond || elapsed > 300*time.Millisecond {
		t.Errorf("a 100ms loop returned after %v", elapsed)
	}
	looped := result.GetChildren()[0].GetStats()
	if looped.GetPasses() < 4 || looped.GetPasses() > 5 {
		t.Errorf("passes = %d, want 4 whole 30ms passes to cover 100ms", looped.GetPasses())
	}
	// Whole passes: as many writes as sleeps, and the sleeps complete.
	if looped.GetDiskBytesWritten() != 16*looped.GetPasses() || looped.GetSleptMs() < 30*(looped.GetPasses()-1) {
		t.Errorf("loop stats = %v, want %d whole passes", looped, looped.GetPasses())
	}
	if root := result.GetStats(); root.GetPasses() != 1 || root.GetDiskBytesWritten() != looped.GetDiskBytesWritten() {
		t.Errorf("root = %v, want one pass and the loop's totals", root)
	}
}

// An inner loop stops cycling once the outer one's budget has elapsed,
// finishing its pass; that is not an error.
func TestRunScriptNestedLoopsShareTheBudget(t *testing.T) {
	svc := newTestService(t)
	start := time.Now()
	result := run(t, svc, script(loop(100, loop(5000, op(sleepFor(20))))))
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("a 5s loop inside a 100ms loop ran for %v", elapsed)
	}
	inner := result.GetChildren()[0].GetChildren()[0].GetStats()
	if inner.GetPasses() < 5 || inner.GetSleptMs() < 20*inner.GetPasses() {
		t.Errorf("inner loop = %v, want ~5 whole 20ms passes", inner)
	}
}

// Shape problems are rejected before any step runs, with the node named.
func TestRunScriptRejectsShape(t *testing.T) {
	svc := newTestService(t)
	for _, tc := range []struct {
		name string
		req  *gluttonpb.RunScriptRequest
		msg  string
	}{
		{"no script", &gluttonpb.RunScriptRequest{}, "script is required"},
		{"empty block", script(), "script: steps is required"},
		{"unset step", script(&gluttonpb.Step{}), "script.steps[0]: no kind set"},
		{"empty operation", script(op()), "script.steps[0].operation: requests is required"},
		{"unset request", script(op(&gluttonpb.Request{})), "script.steps[0].operation.requests[0]: no kind set"},
		{"negative sleep", script(op(sleepFor(-1))), "requests[0] (sleep)"},
		{"negative budget", script(loop(-1, op(sleepFor(0)))), "script.steps[0].block: loop_duration_ms"},
		{"deep empty block", script(seq(seq(seq()))), "script.steps[0].block.steps[0].block.steps[0].block: steps is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.RunScript(context.Background(), tc.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error = %v, want InvalidArgument", err)
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("error %q does not mention %q", err, tc.msg)
			}
		})
	}
	if _, err := svc.ReadDisk(context.Background(), &gluttonpb.ReadDiskRequest{Key: "scripted"}); status.Code(err) != codes.NotFound {
		t.Errorf("a rejected script left a file behind (ReadDisk err = %v)", err)
	}
}

// A request's own error keeps its code and names the request's path; the
// script stops there.
func TestRunScriptRequestErrorsKeepTheirCode(t *testing.T) {
	svc := newTestService(t)
	for _, tc := range []struct {
		name string
		req  *gluttonpb.RunScriptRequest
		code codes.Code
		msg  string
	}{
		{"bad key", script(op(sleepFor(0)), op(writeDisk("../x", 1))), codes.InvalidArgument, "script.steps[1].operation.requests[0] (write_disk)"},
		{"missing file", script(seq(op(readDisk("nothing")))), codes.NotFound, "script.steps[0].block.steps[0].operation.requests[0] (read_disk)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.RunScript(context.Background(), tc.req)
			st, ok := status.FromError(err)
			if !ok || st.Code() != tc.code {
				t.Fatalf("error = %v, want code %v", err, tc.code)
			}
			if !strings.Contains(st.Message(), tc.msg) {
				t.Errorf("message %q does not mention %q", st.Message(), tc.msg)
			}
		})
	}
	// A failing request inside a loop is an error, not the end of the loop.
	_, err := svc.RunScript(context.Background(), script(loop(100, op(readDisk("nothing")))))
	if status.Code(err) != codes.NotFound {
		t.Errorf("a failing request in a loop returned %v, want NotFound", err)
	}
	// One failing request cancels its siblings in the operation.
	start := time.Now()
	_, err = svc.RunScript(context.Background(), script(op(readDisk("nothing"), sleepFor(5000))))
	if status.Code(err) != codes.NotFound || time.Since(start) > time.Second {
		t.Errorf("sibling failure: err %v after %v, want NotFound promptly", err, time.Since(start))
	}
}

// A caller that goes away ends the script, mid-sleep or at the next
// request, so a disconnected request does not keep the sandbox busy. That
// is an error, unlike a budget running out.
func TestRunScriptStopsWhenTheContextEnds(t *testing.T) {
	svc := newTestService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := svc.RunScript(ctx, script(loop(10000, op(sleepFor(5000)))))
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("error = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %v to notice the context ended", elapsed)
	}
}

// The HTTP route carries the same request and maps a request's status code
// the way every other route does.
func TestRunScriptHTTPRoute(t *testing.T) {
	svc := newTestService(t)
	handler, err := Handler(ModeHTTP, svc)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(handler)
	defer ts.Close()

	post := func(req *gluttonpb.RunScriptRequest) (*http.Response, []byte) {
		t.Helper()
		body, _ := proto.Marshal(req)
		res, err := http.Post(ts.URL+RunScriptRoute, "application/x-protobuf", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST %s: %v", RunScriptRoute, err)
		}
		defer res.Body.Close()
		out, _ := io.ReadAll(res.Body)
		return res, out
	}

	res, body := post(script(op(writeDisk("viahttp", 64)), op(readDisk("viahttp"))))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", res.StatusCode, body)
	}
	var resp gluttonpb.RunScriptResponse
	if err := proto.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.GetResult().GetStats().GetRequestsRun() != 2 {
		t.Errorf("requests_run = %d, want 2", resp.GetResult().GetStats().GetRequestsRun())
	}

	res, body = post(script(op(readDisk("absent"))))
	if res.StatusCode != http.StatusNotFound || !strings.Contains(string(body), "(read_disk)") {
		t.Errorf("missing file over HTTP: status %d body %q, want 404 naming the request", res.StatusCode, body)
	}
}
