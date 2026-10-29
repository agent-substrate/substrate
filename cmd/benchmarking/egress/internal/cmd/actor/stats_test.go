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

package actor

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata goldens")

// numberList matches an indented JSON array of numbers, so a golden can keep
// each Point's bounds and counts on one line.
var numberList = regexp.MustCompile(`\[\s+(?:-?[0-9.e+-]+,?\s+)+\]`)

// marshalGolden is MarshalIndent with the number arrays folded onto one line.
func marshalGolden(v any) ([]byte, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return numberList.ReplaceAllFunc(b, func(m []byte) []byte {
		return []byte("[" + strings.Join(strings.Fields(string(m[1:len(m)-1])), " ") + "]")
	}), nil
}

// scriptedStats is a fixed run: 60 requests over 4 endpoints, each opened
// with a lookup and a handshake, a few 503s and timeouts, and two
// distributions in the window.
func scriptedStats() Stats {
	const c = 4
	s := Stats{
		Scheme:                SchemeHTTPS,
		Elapsed:               61200 * time.Millisecond,
		OpeningDone:           350 * time.Millisecond,
		RequestsAtOpeningDone: c,
		LongestGap:            400 * time.Millisecond,
		Errors:                map[string]int64{},
		Endpoints:             make([]Endpoint, c),
		Distributions:         map[Distribution]Point{},
	}
	opening, latency := Point{}, Point{}
	for n := range 60 {
		i := n % c
		s.Requests++
		s.Endpoints[i].Requests++
		s.Endpoints[i].LongestGap = 400 * time.Millisecond
		if n < c {
			s.NewConns++
			s.Opened++
		}
		seconds := (time.Duration(1+n) * time.Millisecond).Seconds()
		switch {
		case n%17 == 3:
			s.Errors["HTTP 503"]++
			s.Endpoints[i].Errors++
		case n%23 == 5:
			s.Errors["timeout"]++
			s.Endpoints[i].Errors++
		case n < c:
			s.Successes++
			record(&opening, seconds)
		default:
			s.Successes++
			record(&latency, seconds)
		}
	}
	s.Rounds = 60 / c
	s.Distributions[Opening] = opening
	s.Distributions[Latency] = latency
	return s
}

// TestStatsGolden pins the JSON a /collect answers with, as a driver decodes
// it: field names, duration units, and the shape of a Point.
func TestStatsGolden(t *testing.T) {
	t.Parallel()
	in := scriptedStats()
	got, err := marshalGolden(in)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("testdata", "collect.json")
	if *updateGolden {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("Stats JSON differs from %s (run with -update to accept):\n%s", path, got)
	}
	var back Stats
	if err := json.Unmarshal(want, &back); err != nil {
		t.Fatalf("decoding %s: %v", path, err)
	}
	if !reflect.DeepEqual(back, in) {
		t.Errorf("%s decodes to\n%+v\nwant\n%+v", path, back, in)
	}
}

func TestStatsWithoutDistributionsOmitsThem(t *testing.T) {
	t.Parallel()
	b, err := json.Marshal(Stats{Scheme: SchemeHTTP, Requests: 3})
	if err != nil {
		t.Fatal(err)
	}
	for _, absent := range []string{"distributions", "endpoints", "errors"} {
		if strings.Contains(string(b), absent) {
			t.Errorf("counters-only Stats carries %q: %s", absent, b)
		}
	}
}
