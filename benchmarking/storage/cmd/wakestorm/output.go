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
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// callRecord is one timed API call.
type callRecord struct {
	Phase   string  `json:"phase"`
	Cycle   int     `json:"cycle"`
	Op      string  `json:"op"`
	Actor   string  `json:"actor"`
	StartS  float64 `json:"start_s"` // offset from the phase start
	EndS    float64 `json:"end_s"`
	Latency float64 `json:"latency_s"`
	OK      bool    `json:"ok"`
	Code    string  `json:"code,omitempty"`
	Err     string  `json:"err,omitempty"`
	Retries int     `json:"retries,omitempty"`
	Node    string  `json:"node,omitempty"`
}

// phaseStats summarizes one batch of calls.
type phaseStats struct {
	Phase       string             `json:"phase"`
	Cycle       int                `json:"cycle"`
	Op          string             `json:"op"`
	Count       int                `json:"count"`
	OK          int                `json:"ok"`
	Failed      int                `json:"failed"`
	Codes       map[string]int     `json:"failure_codes,omitempty"`
	Concurrency int                `json:"concurrency"`
	TotalS      float64            `json:"total_s"` // phase start to the last call's end
	P50         float64            `json:"p50_s"`
	P90         float64            `json:"p90_s"`
	P99         float64            `json:"p99_s"`
	Max         float64            `json:"max_s"`
	Mean        float64            `json:"mean_s"`
	TimeToPct   map[string]float64 `json:"time_to_pct_s,omitempty"` // seconds until that share of calls succeeded
	Retries     int                `json:"retries"`
	StartedAt   time.Time          `json:"started_at"`
	States      map[string]int     `json:"states_after,omitempty"`
}

// runPhase calls fn once per actor, at most conc at a time (0 means all at
// once) and at most rate starts per second (0 means no limit). It records
// every call and returns the batch's stats.
func (r *runner) runPhase(ctx context.Context, actors []string, phase string, cycle int, op string, conc int, rate float64, fn func(context.Context, string) (int, error)) phaseStats {
	if conc <= 0 || conc > len(actors) {
		conc = len(actors)
	}
	logf("=== %s (cycle %d): %s on %d actors, concurrency %d", phase, cycle, op, len(actors), conc)
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var recs []callRecord
	t0 := time.Now()
	for i, name := range actors {
		if ctx.Err() != nil {
			break
		}
		if rate > 0 {
			if d := time.Until(t0.Add(time.Duration(float64(i) / rate * float64(time.Second)))); d > 0 {
				select {
				case <-time.After(d):
				case <-ctx.Done():
				}
			}
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			cctx, cancel := context.WithTimeout(ctx, *flagCallTimeout)
			defer cancel()
			start := time.Now()
			retries, err := fn(cctx, name)
			end := time.Now()
			rec := callRecord{
				Phase: phase, Cycle: cycle, Op: op, Actor: name,
				StartS: start.Sub(t0).Seconds(), EndS: end.Sub(t0).Seconds(),
				Latency: end.Sub(start).Seconds(), OK: err == nil, Retries: retries,
			}
			if err != nil {
				rec.Code = status.Code(err).String()
				rec.Err = truncate(strings.ReplaceAll(err.Error(), "\n", " "), 300)
			}
			mu.Lock()
			recs = append(recs, rec)
			mu.Unlock()
		}()
	}
	wg.Wait()
	st := summarize(phase, cycle, op, conc, t0, recs)
	r.mu.Lock()
	r.calls = append(r.calls, recs...)
	r.stats = append(r.stats, st)
	r.mu.Unlock()
	logf("%s done: total %.1fs, ok %d, failed %d, p50 %.2fs, p90 %.2fs, p99 %.2fs, max %.2fs, retries %d, codes %v",
		phase, st.TotalS, st.OK, st.Failed, st.P50, st.P90, st.P99, st.Max, st.Retries, st.Codes)
	if err := r.save(); err != nil {
		fatal(err)
	}
	return st
}

func (r *runner) replaceLastStats(st phaseStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n := len(r.stats); n > 0 {
		r.stats[n-1] = st
	}
}

// summarize computes a batch's stats. Latency percentiles cover successful
// calls only. TimeToPct counts successes against all calls, so it has no
// entry for a share the batch never reached.
func summarize(phase string, cycle int, op string, conc int, t0 time.Time, recs []callRecord) phaseStats {
	st := phaseStats{
		Phase: phase, Cycle: cycle, Op: op, Count: len(recs), Concurrency: conc,
		Codes: map[string]int{}, TimeToPct: map[string]float64{}, StartedAt: t0.UTC(),
	}
	var lats, ends []float64
	var sum float64
	for _, c := range recs {
		st.Retries += c.Retries
		st.TotalS = max(st.TotalS, c.EndS)
		if !c.OK {
			st.Failed++
			st.Codes[c.Code]++
			continue
		}
		st.OK++
		lats = append(lats, c.Latency)
		ends = append(ends, c.EndS)
		sum += c.Latency
	}
	if len(lats) == 0 {
		return st
	}
	sort.Float64s(lats)
	sort.Float64s(ends)
	st.P50, st.P90, st.P99, st.Max = pct(lats, 50), pct(lats, 90), pct(lats, 99), lats[len(lats)-1]
	st.Mean = sum / float64(len(lats))
	for _, p := range []int{10, 25, 50, 75, 90, 100} {
		idx := int(math.Ceil(float64(p)/100*float64(len(recs)))) - 1
		if idx >= 0 && idx < len(ends) {
			st.TimeToPct[fmt.Sprint(p)] = ends[idx]
		}
	}
	return st
}

// pct returns the nearest-rank p-th percentile of sorted.
func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	return sorted[min(max(idx, 0), len(sorted)-1)]
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// save writes calls.csv and summary.json to the output directory.
func (r *runner) save() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, err := os.Create(filepath.Join(r.outDir, "calls.csv"))
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	rows := [][]string{{"phase", "cycle", "op", "actor", "node", "start_s", "end_s", "latency_s", "ok", "code", "retries", "err"}}
	for _, c := range r.calls {
		node := c.Node
		if node == "" {
			node = r.nodeOf[c.Actor]
		}
		rows = append(rows, []string{c.Phase, fmt.Sprint(c.Cycle), c.Op, c.Actor, node,
			fmt.Sprintf("%.3f", c.StartS), fmt.Sprintf("%.3f", c.EndS), fmt.Sprintf("%.3f", c.Latency),
			fmt.Sprint(c.OK), c.Code, fmt.Sprint(c.Retries), c.Err})
	}
	if err := w.WriteAll(rows); err != nil {
		f.Close()
		return fmt.Errorf("write calls.csv: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	b, err := json.MarshalIndent(map[string]any{
		"started_at": r.started.UTC(),
		"phase":      *flagPhase,
		"n":          *flagN,
		"atespace":   *flagAtespace,
		"template":   *flagTemplateSpace + "/" + *flagTemplate,
		"phases":     r.stats,
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(r.outDir, "summary.json"), b, 0o644)
}

func (r *runner) printSummary() {
	r.mu.Lock()
	defer r.mu.Unlock()
	fmt.Println()
	fmt.Println("| Phase | Cycle | Calls | OK | Failed | Total (s) | P50 (s) | P90 (s) | P99 (s) | Max (s) | Retries |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|---|---|")
	for _, s := range r.stats {
		fmt.Printf("| %s | %d | %d | %d | %d | %.1f | %.2f | %.2f | %.2f | %.2f | %d |\n",
			s.Phase, s.Cycle, s.Count, s.OK, s.Failed, s.TotalS, s.P50, s.P90, s.P99, s.Max, s.Retries)
	}
}

// printLifecycle prints P50, P90 and max per operation across every cycle.
func (r *runner) printLifecycle() {
	r.mu.Lock()
	defer r.mu.Unlock()
	fmt.Println()
	fmt.Println("| Op | Samples | Failed | P50 (s) | P90 (s) | Max (s) |")
	fmt.Println("|---|---|---|---|---|---|")
	for _, row := range lifecycleRows(r.calls) {
		fmt.Printf("| %s | %d | %d | %.3f | %.3f | %.3f |\n", row.op, row.samples, row.failed, row.p50, row.p90, row.max)
	}
}

type opRow struct {
	op              string
	samples, failed int
	p50, p90, max   float64
}

// lifecycleRows groups calls by operation, in the order each operation first
// appears.
func lifecycleRows(calls []callRecord) []opRow {
	byOp := map[string][]float64{}
	failed := map[string]int{}
	var ops []string
	for _, c := range calls {
		if _, ok := byOp[c.Op]; !ok {
			ops = append(ops, c.Op)
			byOp[c.Op] = nil
		}
		if c.OK {
			byOp[c.Op] = append(byOp[c.Op], c.Latency)
		} else {
			failed[c.Op]++
		}
	}
	rows := make([]opRow, 0, len(ops))
	for _, op := range ops {
		l := byOp[op]
		sort.Float64s(l)
		row := opRow{op: op, samples: len(l), failed: failed[op], p50: pct(l, 50), p90: pct(l, 90)}
		if len(l) > 0 {
			row.max = l[len(l)-1]
		}
		rows = append(rows, row)
	}
	return rows
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s %s\n", time.Now().UTC().Format("15:04:05"), fmt.Sprintf(format, args...))
}

func fatal(err error) {
	code := exitCode(err)
	if code == 1 {
		fmt.Fprintln(os.Stderr, "error:", err)
	}
	os.Exit(code)
}

// exitCode returns 130 when err comes from an interrupt and 1 otherwise. A
// cancelled gRPC call returns a Canceled status that does not wrap
// context.Canceled.
func exitCode(err error) int {
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return 130
	}
	return 1
}
