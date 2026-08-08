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

// The cAdvisor-defined measurement window: waits for the anchor container's timestamp
// to advance, which is what makes [t0,t1) a real interval rather than a guess.

package routercap

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrAnchorMissing means the anchor container was absent from a scrape. It is
// expected across an arm change, when the router pod is replaced, and the
// caller is supposed to re-resolve the pod rather than treat it as a failure.
var ErrAnchorMissing = errors.New("anchor container absent from cadvisor scrape")

// Window is one measurement interval, with its bounds taken from cAdvisor
// rather than from a local timer. The kubelet housekeeps roughly every 10s, so
// the driver waits for the anchor container's measurement to change and then
// asks every other source for the interval that measurement covers.
type Window struct {
	// T0 and T1 are the anchor container's cAdvisor timestamps: the instants
	// its cumulative counters were read.
	T0, T1 time.Time
	// Prev and Cur are the scrapes those timestamps came from.
	Prev, Cur CadvisorScrape
	// Polls is how many fetches it took for the anchor timestamp to advance.
	// One means the kubelet moved faster than the poll interval and the
	// resolution is poll-limited rather than kubelet-limited.
	Polls int
}

// Duration is the length of the interval.
func (w Window) Duration() time.Duration { return w.T1.Sub(w.T0) }

// Mid is the interval midpoint, the natural x value when a chart has to draw
// an interval as a point.
func (w Window) Mid() time.Time { return w.T0.Add(w.Duration() / 2) }

// Usage computes each requested container's usage over the window; containers
// absent from either scrape land in missing rather than being silently
// omitted. Each container's rate uses its own pair of cAdvisor timestamps, and
// spread reports the largest disagreement with the anchor's interval.
func (w Window) Usage(keys []ContainerKey) (usage map[ContainerKey]ContainerUsage, spread time.Duration, missing []ContainerKey, errs []error) {
	usage = make(map[ContainerKey]ContainerUsage, len(keys))
	for _, k := range keys {
		prev, okPrev := w.Prev.Containers[k]
		cur, okCur := w.Cur.Containers[k]
		if !okPrev || !okCur {
			missing = append(missing, k)
			continue
		}
		u, err := usageBetween(prev, cur)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		usage[k] = u
		if d := absDuration(prev.At.Sub(w.T0)); d > spread {
			spread = d
		}
		if d := absDuration(cur.At.Sub(w.T1)); d > spread {
			spread = d
		}
	}
	return usage, spread, missing, errs
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// WindowDriver turns cAdvisor's housekeeping cadence into a stream of
// intervals. Its tick rate is the kubelet's, typically ~10s — the real
// resolution of any container CPU number on a kubelet-managed node.
type WindowDriver struct {
	Client Scraper
	// Anchor is the container whose timestamp defines the tick. It should be
	// the one whose CPU matters most: everything else is then aligned to the
	// series the run is actually about.
	Anchor ContainerKey
	// PollInterval is how often to re-fetch while waiting for the anchor
	// timestamp to move. Well below the kubelet cadence, so the observed
	// interval boundaries are the kubelet's and not an artifact of polling.
	PollInterval time.Duration
	// MaxWait bounds one Next call. Exceeding it means the kubelet stopped
	// housekeeping, which is a broken rig rather than a slow one.
	MaxWait time.Duration

	prev   CadvisorScrape
	prevAt time.Time
	primed bool
}

// Prime takes the first scrape, establishing T0 for the first window. Called
// once per arm, after the router pod is up and before load starts.
func (d *WindowDriver) Prime(ctx context.Context) error {
	s, err := d.Client.Scrape(ctx)
	if err != nil {
		return err
	}
	anchor, ok := s.Containers[d.Anchor]
	if !ok {
		return fmt.Errorf("%w: %s", ErrAnchorMissing, d.Anchor)
	}
	if anchor.At.IsZero() {
		return fmt.Errorf("%s: cadvisor exposed no sample timestamp; window alignment is impossible without it", d.Anchor)
	}
	d.prev, d.prevAt, d.primed = s, anchor.At, true
	return nil
}

// Skew reports how far the anchor's most recent cAdvisor sample lags the local
// clock. It conflates real clock skew with housekeeping age, so it is an upper
// bound rather than a correction.
func (d *WindowDriver) Skew() (time.Duration, bool) {
	if !d.primed {
		return 0, false
	}
	return d.prev.SkewAgainst(d.Anchor)
}

// Next blocks until the anchor's cAdvisor timestamp advances and returns the
// interval between the previous timestamp and the new one.
func (d *WindowDriver) Next(ctx context.Context) (Window, error) {
	if !d.primed {
		if err := d.Prime(ctx); err != nil {
			return Window{}, err
		}
	}
	poll := d.PollInterval
	if poll <= 0 {
		poll = time.Second
	}
	maxWait := d.MaxWait
	if maxWait <= 0 {
		maxWait = 2 * time.Minute
	}
	deadline := time.Now().Add(maxWait)

	for polls := 1; ; polls++ {
		s, err := d.Client.Scrape(ctx)
		if err != nil {
			return Window{}, err
		}
		anchor, ok := s.Containers[d.Anchor]
		if !ok {
			return Window{}, fmt.Errorf("%w: %s", ErrAnchorMissing, d.Anchor)
		}
		if anchor.At.After(d.prevAt) {
			w := Window{T0: d.prevAt, T1: anchor.At, Prev: d.prev, Cur: s, Polls: polls}
			d.prev, d.prevAt = s, anchor.At
			return w, nil
		}
		// The kubelet has not housekept since the last window; emitting a
		// record here would pair fresh load numbers with a stale CPU reading.
		if time.Now().After(deadline) {
			return Window{}, fmt.Errorf("%s: cadvisor timestamp stuck at %s for %v across %d polls",
				d.Anchor, d.prevAt, maxWait, polls)
		}
		select {
		case <-ctx.Done():
			return Window{}, ctx.Err()
		case <-time.After(poll):
		}
	}
}
