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

package procstats

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestRead(t *testing.T) {
	s := Read()
	if s.GOMAXPROCS == 0 || s.Goroutines == 0 || s.HeapInUseBytes == 0 || s.GoTotalBytes == 0 {
		t.Errorf("snapshot missing runtime figures: %+v", s)
	}
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		if s.CPUSeconds <= 0 || s.RSSBytes == 0 {
			t.Errorf("snapshot missing process figures: %+v", s)
		}
	}
}

func TestReadCgroup(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if c := readCgroup(dir); c != nil {
		t.Fatalf("readCgroup of an empty directory = %+v, want nil", c)
	}
	write("cpu.stat", "usage_usec 2500000\nuser_usec 2000000\nsystem_usec 500000\n")
	// The root cgroup has cpu.stat but no memory.current: not a container.
	if c := readCgroup(dir); c != nil {
		t.Fatalf("readCgroup without memory.current = %+v, want nil", c)
	}
	write("memory.current", "104857600\n")
	write("memory.peak", "209715200\n")
	write("cpu.max", "150000 100000\n")
	got := readCgroup(dir)
	want := Cgroup{CPUSeconds: 2.5, MemoryCurrentBytes: 100 << 20, MemoryPeakBytes: 200 << 20, CPULimitCores: 1.5}
	if got == nil || *got != want {
		t.Fatalf("readCgroup = %+v, want %+v", got, want)
	}
	write("cpu.max", "max 100000\n")
	if got := readCgroup(dir); got.CPULimitCores != 0 {
		t.Errorf("unlimited cpu.max read as %v cores", got.CPULimitCores)
	}
}
