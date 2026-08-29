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
	"context"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/glutton"
)

func TestParseBytes(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{in: "512Mi", want: 512 << 20},
		{in: "2Gi", want: 2 << 30},
		{in: "16Ki", want: 16 << 10},
		{in: "1G", want: 1_000_000_000},
		{in: "4096", want: 4096},
		{in: "", wantErr: true},
		{in: "Gi", wantErr: true},
		{in: "1.5Gi", wantErr: true},
		{in: "-1Gi", want: -(1 << 30)}, // rejected later by WriteRAM
	}
	for _, tc := range cases {
		got, err := parseBytes(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseBytes(%q) = %d, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseBytes(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseBytes(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestWriteRAMSuffixedSize(t *testing.T) {
	svc, err := newGluttonService(t.TempDir())
	if err != nil {
		t.Fatalf("newGluttonService: %v", err)
	}
	defer svc.Close()
	ctx := context.Background()

	if _, err := svc.WriteRAM(ctx, &glutton.WriteRAMRequest{Key: "a", Size: "1Mi"}); err != nil {
		t.Fatalf("WriteRAM(size=1Mi): %v", err)
	}
	svc.mu.Lock()
	got := len(svc.ram["a"])
	svc.mu.Unlock()
	if got != 1<<20 {
		t.Errorf("ram[a] = %d bytes, want %d", got, 1<<20)
	}

	if _, err := svc.WriteRAM(ctx, &glutton.WriteRAMRequest{Key: "b", Size: "zap"}); err == nil {
		t.Error("WriteRAM(size=zap) succeeded, want error")
	}
	if _, err := svc.WriteRAM(ctx, &glutton.WriteRAMRequest{Key: "c", Size: "-1Gi"}); err == nil {
		t.Error("WriteRAM(size=-1Gi) succeeded, want error")
	}
	if _, err := svc.WriteRAM(ctx, &glutton.WriteRAMRequest{Key: "d"}); err == nil {
		t.Error("WriteRAM(no size) succeeded, want error")
	}
}
