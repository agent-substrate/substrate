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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
)

func TestImageCacheGCTarget(t *testing.T) {
	const gib = int64(1 << 30)
	tests := []struct {
		name                string
		capacity, available uint64
		cacheSize, maxBytes int64
		highPct, lowPct     int
		want                int64
	}{
		{
			name:     "below high watermark: no target",
			capacity: 100 * uint64(gib), available: 30 * uint64(gib), // 70% used
			highPct: 85, lowPct: 80,
			want: 0,
		},
		{
			name:     "at high watermark: free down to low",
			capacity: 100 * uint64(gib), available: 10 * uint64(gib), // 90% used
			cacheSize: 50 * gib, // cache is big enough to cover the shortfall
			highPct:   85, lowPct: 80,
			// available must climb to 20% of capacity: free 20GiB - 10GiB.
			want: 10 * gib,
		},
		{
			name:     "exactly high watermark triggers",
			capacity: 100 * uint64(gib), available: 15 * uint64(gib), // 85% used
			cacheSize: 50 * gib,
			highPct:   85, lowPct: 80,
			want: 5 * gib,
		},
		{
			// The kubelet formula assumes it owns the filesystem; we don't.
			// A near-full boot disk shared with containerd/kubelet/logs must
			// not ask an 11 MiB cache to free 18.9 GiB — uncapped, that
			// evicts the entire cache on every tick forever (0% hit rate)
			// without materially moving disk usage.
			name:     "watermark target capped at what the cache holds",
			capacity: 105 * uint64(gib), available: 2 * uint64(gib), // ~98% used
			cacheSize: 11 << 20,
			highPct:   85, lowPct: 80,
			want: 11 << 20,
		},
		{
			name:     "empty cache under volume pressure: nothing to free",
			capacity: 100 * uint64(gib), available: 1 * uint64(gib),
			cacheSize: 0,
			highPct:   85, lowPct: 80,
			want: 0,
		},
		{
			name:     "max-bytes cap independent of watermarks",
			capacity: 100 * uint64(gib), available: 90 * uint64(gib), // 10% used
			cacheSize: 8 * gib, maxBytes: 5 * gib,
			highPct: 85, lowPct: 80,
			want: 3 * gib,
		},
		{
			name:     "both: larger target wins",
			capacity: 100 * uint64(gib), available: 10 * uint64(gib), // watermark target 10GiB
			cacheSize: 60 * gib, maxBytes: 40 * gib, // cap target 20GiB
			highPct: 85, lowPct: 80,
			want: 20 * gib,
		},
		{
			name:     "max-bytes zero means no cap",
			capacity: 100 * uint64(gib), available: 90 * uint64(gib),
			cacheSize: 500 * gib, maxBytes: 0,
			highPct: 85, lowPct: 80,
			want: 0,
		},
		{
			name:     "zero capacity: watermark half disabled",
			capacity: 0, available: 0,
			cacheSize: 2 * gib, maxBytes: gib,
			highPct: 85, lowPct: 80,
			want: gib,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := imageCacheGCTarget(tc.capacity, tc.available, tc.cacheSize, tc.maxBytes, tc.highPct, tc.lowPct)
			if got != tc.want {
				t.Errorf("imageCacheGCTarget() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestValidateImageCacheGCFlags(t *testing.T) {
	setFlags := func(period time.Duration, high, low int, minAge time.Duration) {
		*imageCacheGCPeriod = period
		*imageCacheHighPct = high
		*imageCacheLowPct = low
		*imageCacheMinAge = minAge
	}
	t.Cleanup(func() { setFlags(5*time.Minute, 85, 80, 2*time.Minute) })

	cases := []struct {
		name      string
		period    time.Duration
		high, low int
		minAge    time.Duration
		wantErr   bool
	}{
		{"defaults", 5 * time.Minute, 85, 80, 2 * time.Minute, false},
		{"boundary high=100 low=0", 5 * time.Minute, 100, 0, 0, false},
		{"zero period disables the periodic pass", 0, 85, 80, 0, false},
		{"negative period would silently disable the loop", -5 * time.Minute, 85, 80, 0, true},
		{"high over 100", 5 * time.Minute, 101, 80, 0, true},
		{"low equals high", 5 * time.Minute, 85, 85, 0, true},
		{"low above high", 5 * time.Minute, 85, 90, 0, true},
		{"negative low", 5 * time.Minute, 85, -1, 0, true},
		{"negative min-age inverts the veto", 5 * time.Minute, 85, 80, -time.Second, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setFlags(tc.period, tc.high, tc.low, tc.minAge)
			err := validateImageCacheGCFlags()
			if (err != nil) != tc.wantErr {
				t.Errorf("period=%v high=%d low=%d minAge=%v: err=%v, wantErr=%v", tc.period, tc.high, tc.low, tc.minAge, err, tc.wantErr)
			}
		})
	}
}

func TestImageCacheDirOutsideBasePath(t *testing.T) {
	cases := []struct {
		name string
		dir  string
		want bool
	}{
		{"inside", filepath.Join(ateompath.BasePath, "image-cache"), false},
		{"inside with doubled separator", ateompath.BasePath + "//image-cache", false},
		{"inside via dot-dot", ateompath.BasePath + "/x/../image-cache", false},
		{"base path itself is not inside", ateompath.BasePath, true},
		{"sibling with the base path as name prefix", ateompath.BasePath + "-other/image-cache", true},
		{"outside", "/var/lib/elsewhere/image-cache", true},
		{"dot-dot escaping the base path", ateompath.BasePath + "/../elsewhere/image-cache", true},
		{"relative resolves against the cwd, not the base path", "image-cache", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := imageCacheDirOutsideBasePath(tc.dir); got != tc.want {
				t.Errorf("imageCacheDirOutsideBasePath(%q) = %v, want %v", tc.dir, got, tc.want)
			}
		})
	}
}

type fakeGCStore struct {
	size         int64
	sizeErr      error
	sizeCalls    int
	evictCalls   int
	gotTarget    int64
	gotDryRun    bool
	evictErr     error
	stats        imagecache.EvictStats
	panicOnEvict bool
}

func (f *fakeGCStore) CacheSize() (int64, error) {
	f.sizeCalls++
	return f.size, f.sizeErr
}

func (f *fakeGCStore) EvictUnused(_ context.Context, target int64, dryRun bool) (imagecache.EvictStats, error) {
	f.evictCalls++
	f.gotTarget = target
	f.gotDryRun = dryRun
	if f.panicOnEvict {
		panic("boom")
	}
	return f.stats, f.evictErr
}

func TestRunPassSkipsOnStatfsFailure(t *testing.T) {
	fake := &fakeGCStore{}
	g := &imageCacheGC{store: fake, cacheDir: filepath.Join(t.TempDir(), "missing"), highPct: 85, lowPct: 80}
	g.runPass(context.Background())
	if fake.sizeCalls != 0 || fake.evictCalls != 0 {
		t.Errorf("statfs failure: sizeCalls=%d evictCalls=%d, want 0/0", fake.sizeCalls, fake.evictCalls)
	}
}

func TestRunPassSkipsOnCacheSizeFailure(t *testing.T) {
	fake := &fakeGCStore{sizeErr: errors.New("unreadable size file")}
	g := &imageCacheGC{store: fake, cacheDir: t.TempDir(), highPct: 85, lowPct: 80}
	g.runPass(context.Background())
	if fake.evictCalls != 0 {
		t.Errorf("CacheSize failure: evictCalls=%d, want 0", fake.evictCalls)
	}
}

func TestRunPassEvictsAndPassesDryRun(t *testing.T) {
	// high=100 sidelines the watermark on any volume with >=1% free, so
	// the max-bytes overage (99) is the target; a near-full host volume
	// can lift it to the cacheSize cap, hence >= not ==.
	fake := &fakeGCStore{size: 100, stats: imagecache.EvictStats{FreedBytes: 100}}
	g := &imageCacheGC{store: fake, cacheDir: t.TempDir(), highPct: 100, lowPct: 0, maxBytes: 1, dryRun: true}
	g.consecutiveShortfalls = 5 // a met target must reset it
	g.runPass(context.Background())
	if fake.evictCalls != 1 || fake.gotTarget < 99 || !fake.gotDryRun {
		t.Errorf("evictCalls=%d target=%d dryRun=%v, want 1/>=99/true", fake.evictCalls, fake.gotTarget, fake.gotDryRun)
	}
	if g.consecutiveShortfalls != 0 {
		t.Errorf("consecutiveShortfalls=%d after met target, want 0", g.consecutiveShortfalls)
	}
}

func TestRunFirstPassIsImmediate(t *testing.T) {
	// A cancelled context and an hour-long period: the single call can
	// only be the immediate first pass, never a tick.
	fake := &fakeGCStore{}
	g := &imageCacheGC{store: fake, cacheDir: t.TempDir(), highPct: 100, lowPct: 0, period: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g.Run(ctx)
	if fake.evictCalls != 1 {
		t.Errorf("evictCalls=%d, want exactly 1 (the immediate first pass)", fake.evictCalls)
	}
}

func TestRunTicks(t *testing.T) {
	fake := &fakeGCStore{}
	g := &imageCacheGC{store: fake, cacheDir: t.TempDir(), highPct: 100, lowPct: 0, period: 10 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	g.Run(ctx)
	if fake.evictCalls < 2 {
		t.Errorf("evictCalls=%d, want >=2 (first pass plus at least one tick)", fake.evictCalls)
	}
}

func TestRunPassRecoversPanic(t *testing.T) {
	g := &imageCacheGC{store: &fakeGCStore{panicOnEvict: true}, cacheDir: t.TempDir(), highPct: 100, lowPct: 0}
	g.runPass(context.Background()) // must not propagate the panic
}

// TestNoteOutcomeShortfallBackoff drives the shortfall cadence end to end:
// warn on the first shortfallWarnLimit consecutive shortfalls, then only
// every shortfallReminderEvery-th, streak preserved across a gated pass,
// reset (re-arming the warnings) on a met or absent target.
func TestNoteOutcomeShortfallBackoff(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx := context.Background()
	g := &imageCacheGC{}
	logCount := func(msg string) int { return strings.Count(buf.String(), msg) }

	for range shortfallWarnLimit {
		g.noteOutcome(ctx, gcPassShortfall, nil, nil)
	}
	if got := logCount("could not reach target"); got != shortfallWarnLimit {
		t.Errorf("initial warns = %d, want %d", got, shortfallWarnLimit)
	}

	buf.Reset()
	for g.consecutiveShortfalls < 2*shortfallReminderEvery {
		g.noteOutcome(ctx, gcPassShortfall, nil, nil)
	}
	if got := logCount("still short of target"); got != 2 {
		t.Errorf("reminders through streak %d = %d, want 2", g.consecutiveShortfalls, got)
	}
	if got := logCount("could not reach target"); got != 0 {
		t.Errorf("warns past the limit = %d, want 0", got)
	}

	streak := g.consecutiveShortfalls
	buf.Reset()
	g.noteOutcome(ctx, gcPassSkipped, errors.New("gated"), nil)
	if g.consecutiveShortfalls != streak {
		t.Errorf("streak after gated pass = %d, want %d (preserved)", g.consecutiveShortfalls, streak)
	}
	if got := logCount("Image cache GC pass skipped"); got != 1 {
		t.Errorf("skip logs = %d, want 1", got)
	}

	buf.Reset()
	g.noteOutcome(ctx, gcPassComplete, errors.New("one dir failed"), nil)
	if g.consecutiveShortfalls != 0 {
		t.Errorf("streak after complete pass = %d, want 0", g.consecutiveShortfalls)
	}
	if got := logCount("pass complete"); got != 1 {
		t.Errorf("complete logs = %d, want 1", got)
	}
	if got := logCount("finished with errors"); got != 1 {
		t.Errorf("per-item error warns = %d, want 1", got)
	}

	buf.Reset()
	g.noteOutcome(ctx, gcPassShortfall, nil, nil)
	if got := logCount("could not reach target"); got != 1 {
		t.Errorf("warns after reset = %d, want 1 (re-armed)", got)
	}

	g.consecutiveShortfalls = shortfallWarnLimit + 1
	buf.Reset()
	g.noteOutcome(ctx, gcPassQuiet, nil, nil)
	if g.consecutiveShortfalls != 0 || buf.Len() != 0 {
		t.Errorf("quiet pass: streak=%d buf=%q, want silent reset", g.consecutiveShortfalls, buf.String())
	}
}

func TestClassifyGCPass(t *testing.T) {
	gated := fmt.Errorf("pass gated: %w", imagecache.ErrIncompleteEnumeration)
	perItem := errors.New("while removing retired layer: permission denied")
	cases := []struct {
		name          string
		err           error
		target, freed int64
		want          gcPassOutcome
	}{
		{"gated pass", gated, 100, 0, gcPassSkipped},
		{"gated wins even with zero target", gated, 0, 0, gcPassSkipped},
		{"per-item errors are not a skip", perItem, 100, 100, gcPassComplete},
		{"per-item errors with shortfall", perItem, 100, 40, gcPassShortfall},
		{"shortfall", nil, 100, 40, gcPassShortfall},
		{"target met", nil, 100, 100, gcPassComplete},
		{"no target", nil, 0, 0, gcPassQuiet},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyGCPass(tc.err, tc.target, tc.freed); got != tc.want {
				t.Errorf("classifyGCPass(%v, %d, %d) = %d, want %d", tc.err, tc.target, tc.freed, got, tc.want)
			}
		})
	}
}
