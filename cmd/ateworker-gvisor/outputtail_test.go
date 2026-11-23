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
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// The tee keeps the container's last line while the log keeps every byte, the
// way WrapContainerLogs reads the pipe through a TeeReader.
func TestOutputTailsKeepsLastLine(t *testing.T) {
	var logged bytes.Buffer
	tails := newOutputTails()
	sink := tails.watch("app")

	src := strings.NewReader("first\nsecond\nthird\npar")
	if _, err := io.Copy(&logged, io.TeeReader(src, sink)); err != nil {
		t.Fatal(err)
	}

	if got, want := logged.String(), "first\nsecond\nthird\npar"; got != want {
		t.Errorf("logged %q, want %q", got, want)
	}
	want := []namedLine{{container: "app", line: "par"}}
	if got := tails.lines(); !reflect.DeepEqual(got, want) {
		t.Errorf("lines() = %v, want %v", got, want)
	}
}

// The tee target never fails the pipe's read: an error there would end the
// container's log.
func TestTailSinkWriteNeverFails(t *testing.T) {
	tails := newOutputTails()
	sink := tails.watch("app")

	n, err := sink.Write([]byte("anything"))
	if n != len("anything") || err != nil {
		t.Errorf("Write() = %d, %v, want %d, nil", n, err, len("anything"))
	}
}

// A partial line completed by a later write becomes the last line.
func TestOutputTailsPartialLineCompletes(t *testing.T) {
	tails := newOutputTails()
	sink := tails.watch("app")

	_, _ = sink.Write([]byte("first\npar"))
	_, _ = sink.Write([]byte("tial\n"))

	want := []namedLine{{container: "app", line: "partial"}}
	if got := tails.lines(); !reflect.DeepEqual(got, want) {
		t.Errorf("lines() = %v, want %v", got, want)
	}
}

// A blank line is not a reason for a crash, and does not replace the last
// line that stated one.
func TestOutputTailsSkipBlankLines(t *testing.T) {
	tails := newOutputTails()
	sink := tails.watch("app")

	_, _ = sink.Write([]byte("\n \n"))
	if got := tails.lines(); len(got) != 0 {
		t.Errorf("lines() = %v, want no line", got)
	}
	_, _ = sink.Write([]byte("cause\n\n "))
	want := []namedLine{{container: "app", line: "cause"}}
	if got := tails.lines(); !reflect.DeepEqual(got, want) {
		t.Errorf("lines() = %v, want %v", got, want)
	}
}

// A line ending in \r\n keeps neither.
func TestOutputTailsTrimCarriageReturn(t *testing.T) {
	tails := newOutputTails()
	sink := tails.watch("app")

	_, _ = sink.Write([]byte("cause\r\n"))
	want := []namedLine{{container: "app", line: "cause"}}
	if got := tails.lines(); !reflect.DeepEqual(got, want) {
		t.Errorf("lines() = %v, want %v", got, want)
	}
}

// A line longer than the cap keeps its last bytes, the part the crash
// message's own truncation would keep of the message.
func TestOutputTailsCapLongLine(t *testing.T) {
	long := strings.Repeat("a", maxTailBytes) + strings.Repeat("b", maxTailBytes) + "diagnosis"
	for _, tc := range []struct {
		name   string
		writes []string
	}{
		{"one write", []string{long}},
		{"many writes", chunked(long, 200)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tails := newOutputTails()
			sink := tails.watch("app")
			for _, s := range tc.writes {
				if _, err := sink.Write([]byte(s)); err != nil {
					t.Fatal(err)
				}
			}

			want := long[len(long)-maxTailBytes:]
			// While the line is partial.
			if got := tails.lines(); len(got) != 1 || got[0].line != want {
				t.Errorf("partial line = %v, want the last %d bytes", got, maxTailBytes)
			}
			// And once it completes.
			_, _ = sink.Write([]byte("\n"))
			if got := tails.lines(); len(got) != 1 || got[0].line != want {
				t.Errorf("complete line = %v, want the last %d bytes", got, maxTailBytes)
			}
		})
	}
}

// Every container that wrote a line is reported, in container name order.
func TestOutputTailsReportsEveryContainer(t *testing.T) {
	tails := newOutputTails()
	for _, name := range []string{"sidecar", "app", "quiet"} {
		sink := tails.watch(name)
		if name != "quiet" {
			_, _ = sink.Write([]byte(name + " said no\n"))
		}
	}

	want := []namedLine{
		{container: "app", line: "app said no"},
		{container: "sidecar", line: "sidecar said no"},
	}
	if got := tails.lines(); !reflect.DeepEqual(got, want) {
		t.Errorf("lines() = %v, want %v", got, want)
	}
}

// Recording is safe while the crash report reads it: the container can be
// writing at the moment its sandbox dies.
func TestOutputTailsConcurrent(t *testing.T) {
	tails := newOutputTails()
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sink := tails.watch(fmt.Sprintf("c%d", i))
			for j := range 500 {
				_, _ = fmt.Fprintf(sink, "container %d line %d\n", i, j)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	for {
		select {
		case <-done:
			if got := len(tails.lines()); got != 4 {
				t.Fatalf("lines() has %d containers, want 4", got)
			}
			return
		default:
			_ = tails.lines()
		}
	}
}

// A nil tail, as on a hostedActor built outside admitActor, reports nothing.
func TestOutputTailsNil(t *testing.T) {
	var tails *outputTails
	if got := tails.lines(); got != nil {
		t.Errorf("lines() = %v, want nil", got)
	}
}

func chunked(s string, n int) []string {
	var out []string
	for len(s) > 0 {
		chunk := min(n, len(s))
		out = append(out, s[:chunk])
		s = s[chunk:]
	}
	return out
}
