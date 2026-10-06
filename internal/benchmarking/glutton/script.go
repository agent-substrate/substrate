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
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

// RunScript runs the request's script tree in-process and holds the
// request open until it finishes. The tree is checked for shape before
// anything runs; a request's own argument errors surface when it runs,
// with its path in the tree and kind prepended and its status code kept,
// so a caller can tell a bad script from a failed request. See the proto
// for what a block, an operation, and the stats mean.
func (s *Service) RunScript(ctx context.Context, req *gluttonpb.RunScriptRequest) (*gluttonpb.RunScriptResponse, error) {
	if req.GetScript() == nil {
		return nil, status.Error(codes.InvalidArgument, "script is required")
	}
	if err := validateBlock(req.GetScript(), "script"); err != nil {
		return nil, err
	}
	result, err := s.runBlock(ctx, req.GetScript(), "script", time.Time{})
	if err != nil {
		return nil, err
	}
	return &gluttonpb.RunScriptResponse{Result: result}, nil
}

// validateBlock rejects what can be seen without running anything: an
// empty block or operation, a step or request with nothing set, a negative
// budget or sleep. path names the node in messages.
func validateBlock(block *gluttonpb.Block, path string) error {
	if len(block.GetSteps()) == 0 {
		return status.Errorf(codes.InvalidArgument, "%s: steps is required", path)
	}
	if block.GetLoopDurationMs() < 0 {
		return status.Errorf(codes.InvalidArgument, "%s: loop_duration_ms must be non-negative", path)
	}
	for i, step := range block.GetSteps() {
		stepPath := fmt.Sprintf("%s.steps[%d]", path, i)
		switch kind := step.GetKind().(type) {
		case *gluttonpb.Step_Block:
			if err := validateBlock(kind.Block, stepPath+".block"); err != nil {
				return err
			}
		case *gluttonpb.Step_Operation:
			if err := validateOperation(kind.Operation, stepPath+".operation"); err != nil {
				return err
			}
		default:
			return status.Errorf(codes.InvalidArgument, "%s: no kind set", stepPath)
		}
	}
	return nil
}

func validateOperation(op *gluttonpb.Operation, path string) error {
	if len(op.GetRequests()) == 0 {
		return status.Errorf(codes.InvalidArgument, "%s: requests is required", path)
	}
	for i, request := range op.GetRequests() {
		requestPath := fmt.Sprintf("%s.requests[%d]", path, i)
		switch kind := request.GetKind().(type) {
		case nil:
			return status.Errorf(codes.InvalidArgument, "%s: no kind set", requestPath)
		case *gluttonpb.Request_Sleep:
			if kind.Sleep.GetDurationMs() < 0 {
				return status.Errorf(codes.InvalidArgument, "%s (sleep): duration_ms must be non-negative", requestPath)
			}
		}
	}
	return nil
}

// runBlock runs the block's steps in order and, when the block loops,
// again and again until a budget has elapsed. deadline is the earliest
// budget among the block's ancestors, zero for none; the block's own
// budget tightens it for the block and everything beneath. A budget is
// checked between passes only: the pass in progress finishes, so a loop
// overruns its budget by at most one pass and nothing is cut mid-flight.
// The only errors are the caller's context ending and a failing request.
// The context is checked before every step, since most requests run to
// completion without looking at it: a looped block of CPU burns or disk
// writes stops at the next step once the caller is gone, not at its budget.
func (s *Service) runBlock(ctx context.Context, block *gluttonpb.Block, path string, deadline time.Time) (*gluttonpb.StepResult, error) {
	start := time.Now()
	result := newResult(len(block.GetSteps()))
	if budget := block.GetLoopDurationMs(); budget > 0 {
		own := start.Add(time.Duration(budget) * time.Millisecond)
		if deadline.IsZero() || own.Before(deadline) {
			deadline = own
		}
	}
	for {
		result.Stats.Passes++
		for i, step := range block.GetSteps() {
			if err := ctx.Err(); err != nil {
				result.Stats.ElapsedMs = time.Since(start).Milliseconds()
				return result, status.FromContextError(err).Err()
			}
			child, err := s.runStep(ctx, step, fmt.Sprintf("%s.steps[%d]", path, i), deadline)
			if child != nil {
				addResult(result.Children[i], child)
				addTotals(result.Stats, child.Stats)
			}
			if err != nil {
				result.Stats.ElapsedMs = time.Since(start).Milliseconds()
				return result, err
			}
		}
		if block.GetLoopDurationMs() == 0 || !time.Now().Before(deadline) {
			break
		}
	}
	result.Stats.ElapsedMs = time.Since(start).Milliseconds()
	return result, nil
}

func (s *Service) runStep(ctx context.Context, step *gluttonpb.Step, path string, deadline time.Time) (*gluttonpb.StepResult, error) {
	switch kind := step.GetKind().(type) {
	case *gluttonpb.Step_Block:
		return s.runBlock(ctx, kind.Block, path+".block", deadline)
	case *gluttonpb.Step_Operation:
		return s.runOperation(ctx, kind.Operation, path+".operation")
	default:
		return nil, status.Errorf(codes.InvalidArgument, "%s: no kind set", path)
	}
}

// runOperation runs the operation's requests at the same time. The first
// failure cancels the rest and is the error returned; the result still
// carries the stats of every request that ran, failed ones included.
func (s *Service) runOperation(ctx context.Context, op *gluttonpb.Operation, path string) (*gluttonpb.StepResult, error) {
	start := time.Now()
	result := newResult(len(op.GetRequests()))
	group, groupCtx := errgroup.WithContext(ctx)
	for i, request := range op.GetRequests() {
		group.Go(func() error {
			stats, err := s.runRequest(groupCtx, request)
			if stats != nil {
				result.Children[i].Stats = stats
			}
			if err != nil {
				return prefixError(fmt.Sprintf("%s.requests[%d] (%s)", path, i, requestKind(request)), err)
			}
			return nil
		})
	}
	err := group.Wait()
	for _, child := range result.Children {
		addTotals(result.Stats, child.Stats)
	}
	result.Stats.Passes = 1
	result.Stats.ElapsedMs = time.Since(start).Milliseconds()
	return result, err
}

// runRequest dispatches one request to the service method it names and
// reports what it did. Results other than the stats are dropped.
func (s *Service) runRequest(ctx context.Context, request *gluttonpb.Request) (*gluttonpb.Stats, error) {
	start := time.Now()
	stats := &gluttonpb.Stats{Passes: 1, RequestsRun: 1}
	var err error
	switch kind := request.GetKind().(type) {
	case *gluttonpb.Request_WriteRam:
		_, err = s.WriteRAM(ctx, kind.WriteRam)
		if err == nil {
			stats.RamBytesWritten, _ = parseBytes(kind.WriteRam.GetSize())
		}
	case *gluttonpb.Request_ReadRam:
		var resp *gluttonpb.ReadRAMResponse
		resp, err = s.ReadRAM(ctx, kind.ReadRam)
		stats.RamBytesRead = resp.GetSize()
	case *gluttonpb.Request_WriteDisk:
		_, err = s.WriteDisk(ctx, kind.WriteDisk)
		if err == nil {
			stats.DiskBytesWritten = int64(kind.WriteDisk.GetSize())
		}
	case *gluttonpb.Request_ReadDisk:
		var resp *gluttonpb.ReadDiskResponse
		resp, err = s.ReadDisk(ctx, kind.ReadDisk)
		stats.DiskBytesRead = resp.GetSize()
	case *gluttonpb.Request_BurnCpu:
		var resp *gluttonpb.BurnCPUResponse
		resp, err = s.BurnCPU(ctx, kind.BurnCpu)
		stats.BurnIterations = resp.GetIterations()
	case *gluttonpb.Request_Ingest:
		_, err = s.Ingest(ctx, kind.Ingest)
		if err == nil {
			stats.DiskBytesWritten = int64(len(kind.Ingest.GetPayload()))
		}
	case *gluttonpb.Request_Sleep:
		err = sleep(ctx, time.Duration(kind.Sleep.GetDurationMs())*time.Millisecond)
		stats.SleptMs = time.Since(start).Milliseconds()
	default:
		err = status.Errorf(codes.InvalidArgument, "unknown request %T", kind)
	}
	stats.ElapsedMs = time.Since(start).Milliseconds()
	return stats, err
}

// sleep idles for duration or until ctx ends, whichever comes first.
func sleep(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

// newResult is a node with zeroed stats and n zeroed children, ready to
// have each pass added in.
func newResult(n int) *gluttonpb.StepResult {
	result := &gluttonpb.StepResult{Stats: &gluttonpb.Stats{}, Children: make([]*gluttonpb.StepResult, n)}
	for i := range result.Children {
		result.Children[i] = &gluttonpb.StepResult{Stats: &gluttonpb.Stats{}}
	}
	return result
}

// addResult folds one entry's result tree into the accumulated node of the
// same shape. A child the entry did not produce (an operation's request
// that never started) adds nothing.
func addResult(dst, src *gluttonpb.StepResult) {
	addStats(dst.Stats, src.Stats)
	for len(dst.Children) < len(src.Children) {
		dst.Children = append(dst.Children, &gluttonpb.StepResult{Stats: &gluttonpb.Stats{}})
	}
	for i, child := range src.Children {
		if child != nil {
			addResult(dst.Children[i], child)
		}
	}
}

// addStats sums src into dst, field by field: one node's later entry
// added to its earlier ones.
func addStats(dst, src *gluttonpb.Stats) {
	if src == nil {
		return
	}
	dst.ElapsedMs += src.ElapsedMs
	dst.Passes += src.Passes
	addTotals(dst, src)
}

// addTotals sums a child's work into its parent: every field but passes,
// which count the parent's own entries, and elapsed, which the parent
// measures itself.
func addTotals(dst, src *gluttonpb.Stats) {
	if src == nil {
		return
	}
	dst.RequestsRun += src.RequestsRun
	dst.DiskBytesWritten += src.DiskBytesWritten
	dst.DiskBytesRead += src.DiskBytesRead
	dst.RamBytesWritten += src.RamBytesWritten
	dst.RamBytesRead += src.RamBytesRead
	dst.BurnIterations += src.BurnIterations
	dst.SleptMs += src.SleptMs
}

// requestKind is the proto field name of the request that is set.
func requestKind(request *gluttonpb.Request) string {
	switch request.GetKind().(type) {
	case *gluttonpb.Request_WriteRam:
		return "write_ram"
	case *gluttonpb.Request_ReadRam:
		return "read_ram"
	case *gluttonpb.Request_WriteDisk:
		return "write_disk"
	case *gluttonpb.Request_ReadDisk:
		return "read_disk"
	case *gluttonpb.Request_BurnCpu:
		return "burn_cpu"
	case *gluttonpb.Request_Ingest:
		return "ingest"
	case *gluttonpb.Request_Sleep:
		return "sleep"
	}
	return "unset"
}

// prefixError puts the failing request's path ahead of err's message,
// keeping its status code so the HTTP mapping and the caller's
// classification still apply.
func prefixError(where string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return status.Errorf(codes.Internal, "%s: %v", where, err)
	}
	return status.Error(st.Code(), fmt.Sprintf("%s: %s", where, st.Message()))
}
