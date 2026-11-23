//go:build linux

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
	"bytes"
	"cmp"
	"io"
	"slices"
	"sync"
)

// maxTailBytes caps how much of a container's last line the worker keeps. The
// crash message is truncated from its end downstream, so the end of a long
// line is the part that has to fit.
const maxTailBytes = 1024

// outputTails keeps the last line each application container wrote, for the
// crash report: a container that fails at startup states its reason on its
// output, which otherwise reaches only the worker's log.
//
// The container's stdout and stderr are one stream here, the runsc process's
// own, so the kept line may come from either.
type outputTails struct {
	mu    sync.Mutex
	tails map[string]*outputTail
}

// newOutputTails returns tails for one activation.
func newOutputTails() *outputTails {
	return &outputTails{tails: make(map[string]*outputTail)}
}

// watch returns a writer that records the last line container name writes. It
// is the tee target of the actor log pipe, so it sees the container's output
// without sitting in front of the pipe: the pipe's write end goes to runsc as
// the container's stdout and stderr, and has to stay an *os.File for os/exec
// to hand it over directly.
func (o *outputTails) watch(name string) io.Writer {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.tails[name] == nil {
		o.tails[name] = &outputTail{}
	}
	return &tailSink{tails: o, name: name}
}

// outputTail is one container's last line across writes.
type outputTail struct {
	// line is the last complete line that said anything, without its
	// newline. A blank line does not replace it, so a trailing newline
	// cannot erase the reason the container stated.
	line []byte
	// partial is what was written since the last newline, a line that a
	// crash can interrupt before it finishes.
	partial []byte
}

// tailSink records a container's output. Its Write never fails: it sits on the
// log pipe's read side, where an error would end the container's log.
type tailSink struct {
	tails *outputTails
	name  string
}

func (s *tailSink) Write(p []byte) (int, error) {
	s.tails.record(s.name, p)
	return len(p), nil
}

// record records what container name just wrote.
func (o *outputTails) record(name string, p []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	t := o.tails[name]
	if t == nil {
		return
	}
	for {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			t.partial = appendCapped(t.partial, p)
			return
		}
		if line := bytes.TrimSuffix(appendCapped(t.partial, p[:i]), []byte("\r")); len(bytes.TrimSpace(line)) > 0 {
			t.line = line
		}
		t.partial = nil
		p = p[i+1:]
	}
}

// namedLine is one container's last line.
type namedLine struct {
	container string
	line      string
}

// lines returns the last line of every container that wrote one, in container
// name order. A partial line counts: a crash can interrupt a line before it
// finishes.
func (o *outputTails) lines() []namedLine {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]namedLine, 0, len(o.tails))
	for name, t := range o.tails {
		line := t.partial
		if len(bytes.TrimSpace(line)) == 0 {
			line = t.line
		}
		if len(line) > 0 {
			out = append(out, namedLine{container: name, line: string(line)})
		}
	}
	slices.SortFunc(out, func(a, b namedLine) int {
		return cmp.Compare(a.container, b.container)
	})
	return out
}

// appendCapped appends p to b, keeping only the last maxTailBytes bytes of the
// result.
func appendCapped(b, p []byte) []byte {
	total := len(b) + len(p)
	if total <= maxTailBytes {
		return append(b, p...)
	}
	out := make([]byte, maxTailBytes)
	if len(p) >= maxTailBytes {
		copy(out, p[len(p)-maxTailBytes:])
		return out
	}
	// Drop the front of b, then fill the rest with p.
	copy(out, b[total-maxTailBytes:])
	copy(out[maxTailBytes-len(p):], p)
	return out
}
