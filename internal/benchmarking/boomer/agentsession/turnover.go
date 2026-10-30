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

package agentsession

import (
	"log/slog"
	mathrand "math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Turnover mode (agentsession_awake > 0) keeps a fixed number of sessions awake
// and swaps idle awake sessions with parked ones at target_rps/s. Each session
// registers by creating its actor, running step 1, and parking it. After
// registrationQuiet with no active registrations, the conductor wakes
// agentsession_awake sessions and then swaps at target_rps/s.
const (
	registrationQuiet        = time.Minute
	turnoverStartConcurrency = 48
	fillConcurrency          = 8
	swapTick                 = time.Second
	turnoverParkRetry        = 2 * time.Second
)

var turnoverSessions = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "agentsession_turnover_sessions",
	Help: "Turnover-mode sessions by state (awake, parked, waking, parking).",
}, []string{"state"})

func init() {
	prometheus.MustRegister(turnoverSessions)
}

type turnCmd int

const (
	cmdWake turnCmd = iota + 1
	cmdPark
)

// conductor coordinates session wakes and parks.
type conductor struct {
	now      func() time.Time
	starting func() int64

	mu         sync.Mutex
	parked     []*sessionUser
	idle       []*sessionUser
	busy       map[*sessionUser]bool
	inFlight   map[*sessionUser]turnCmd
	lastJoin   time.Time
	filled     bool
	halted     bool
	warnHalted sync.Once
	credit     float64
}

func newConductor(now func() time.Time) *conductor {
	return &conductor{now: now, busy: map[*sessionUser]bool{}, inFlight: map[*sessionUser]turnCmd{}}
}

func (c *conductor) join(u *sessionUser) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.halted {
		c.warnHalted.Do(func() {
			slog.Warn("agentsession: turnover is off after a stop from the master; restart the worker to run it again")
		})
	}
	c.lastJoin = c.now()
	c.parked = append(c.parked, u)
}

// settle files a session after a command or step finishes. A session whose
// park failed stays in idle until a later park succeeds.
func (c *conductor) settle(u *sessionUser) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inFlight, u)
	delete(c.busy, u)
	switch {
	case u.broken:
	case u.awake || u.hibernatePending:
		c.idle = append(c.idle, u)
	default:
		c.parked = append(c.parked, u)
	}
}

// claim moves an idle session to busy before running a step.
func (c *conductor) claim(u *sessionUser) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := slices.Index(c.idle, u)
	if i < 0 {
		return false
	}
	c.idle = slices.Delete(c.idle, i, i+1)
	c.busy[u] = true
	return true
}

func (c *conductor) halt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.halted = true
}

func (c *conductor) tick(awake int, rate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.export()
	if c.halted {
		return
	}
	if !c.filled {
		c.fill(awake)
		return
	}
	c.balance(awake, fillConcurrency)
	if rate <= 0 {
		c.credit = 0
		return
	}
	perTick := rate * swapTick.Seconds()
	c.credit = min(c.credit+perTick, perTick+1)
	for c.credit >= 1 && len(c.idle) > 0 && len(c.parked) > 0 {
		c.send(&c.idle, cmdPark)
		c.send(&c.parked, cmdWake)
		c.credit--
	}
}

func (c *conductor) fill(awake int) {
	if (c.starting != nil && c.starting() > 0) || c.lastJoin.IsZero() || c.now().Sub(c.lastJoin) < registrationQuiet {
		return
	}
	c.balance(awake, fillConcurrency-c.wakesInFlight())
	if c.resident() < awake && (len(c.parked) > 0 || c.wakesInFlight() > 0) {
		return
	}
	c.filled = true
	slog.Info("agentsession: turnover fill done",
		slog.Int("awake", c.resident()), slog.Int("parked", len(c.parked)))
	if c.resident() < awake {
		slog.Warn("agentsession: this worker has fewer sessions than agentsession_awake",
			slog.Int("awake", c.resident()), slog.Int("agentsession_awake", awake))
	}
	if len(c.parked) == 0 {
		slog.Warn("agentsession: no parked sessions are left to swap; run more users than agentsession_awake")
	}
}

// balance wakes or parks sessions so awake sessions are resident.
func (c *conductor) balance(awake, maxWakes int) {
	want := awake - c.resident() - c.wakesInFlight()
	for ; want > 0 && maxWakes > 0 && len(c.parked) > 0; want, maxWakes = want-1, maxWakes-1 {
		c.send(&c.parked, cmdWake)
	}
	for ; want < 0 && len(c.idle) > 0; want++ {
		i := slices.IndexFunc(c.idle, func(u *sessionUser) bool { return u.hibernatePending })
		if i < 0 {
			i = mathrand.IntN(len(c.idle))
		}
		c.sendAt(&c.idle, i, cmdPark)
	}
}

func (c *conductor) resident() int {
	return len(c.idle) + len(c.busy)
}

func (c *conductor) wakesInFlight() int {
	n := 0
	for _, cmd := range c.inFlight {
		if cmd == cmdWake {
			n++
		}
	}
	return n
}

func (c *conductor) send(from *[]*sessionUser, cmd turnCmd) {
	c.sendAt(from, mathrand.IntN(len(*from)), cmd)
}

func (c *conductor) sendAt(from *[]*sessionUser, i int, cmd turnCmd) {
	s := *from
	u := s[i]
	s[i] = s[len(s)-1]
	*from = s[:len(s)-1]
	c.inFlight[u] = cmd
	u.cmdCh <- cmd
}

func (c *conductor) export() {
	wakes := c.wakesInFlight()
	turnoverSessions.WithLabelValues("awake").Set(float64(c.resident()))
	turnoverSessions.WithLabelValues("parked").Set(float64(len(c.parked)))
	turnoverSessions.WithLabelValues("waking").Set(float64(wakes))
	turnoverSessions.WithLabelValues("parking").Set(float64(len(c.inFlight) - wakes))
}
