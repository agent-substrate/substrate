//go:build linux

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

package netns

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Only EROFS takes the remount path: any other error is reported as it is,
// and /proc/sys is left as it was found. Remounting it read-only on the way
// out would break every later write.
func TestSetSysctlReportsAnUnrelatedError(t *testing.T) {
	err := SetSysctls(Sysctl{Key: "net/ipv4/workernet_no_such_sysctl", Value: "0"})
	if !errors.Is(err, unix.ENOENT) {
		t.Fatalf("SetSysctls() on a missing key: got %v, want ENOENT", err)
	}
	var st unix.Statfs_t
	if err := unix.Statfs("/proc/sys", &st); err != nil {
		t.Fatalf("statfs /proc/sys: %v", err)
	}
	if st.Flags&unix.ST_RDONLY != 0 {
		t.Error("/proc/sys was left read-only")
	}
}

// Concurrent callers share one /proc/sys mount; none may fail because another
// restored it read-only before its write.
func TestSetSysctlSerializesTheRemount(t *testing.T) {
	const callers = 16
	fake := &fakeProcSys{readOnly: true, values: map[string]string{}}
	start := make(chan struct{})
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			<-start
			errs[i] = fake.procSys().set([]Sysctl{{Key: fmt.Sprintf("net/ipv4/caller_%d", i), Value: "0"}})
		})
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: %v", i, err)
		}
	}
	if fake.maxRemounted != 1 {
		t.Errorf("%d callers had /proc/sys remounted read-write at once, want 1", fake.maxRemounted)
	}
	if !fake.readOnly {
		t.Error("/proc/sys was left read-write")
	}
}

// forwardingSysctls span both families, so a kernel without IPv6 exercises
// the missing-key path.
var forwardingSysctls = []Sysctl{
	{Key: "net/ipv4/conf/all/forwarding", Value: "0"},
	{Key: "net/ipv4/conf/default/forwarding", Value: "0"},
	{Key: "net/ipv6/conf/all/forwarding", Value: "0"},
	{Key: "net/ipv6/conf/default/forwarding", Value: "0"},
}

// Setting multiple sysctls performs at most one read-write remount cycle.
func TestSetSysctlsBatchesRemount(t *testing.T) {
	fake := &fakeProcSys{readOnly: true, values: map[string]string{
		"/proc/sys/net/ipv4/ip_unprivileged_port_start": "1024",
		"/proc/sys/net/ipv4/conf/all/forwarding":        "1",
		"/proc/sys/net/ipv4/conf/default/forwarding":    "1",
		"/proc/sys/net/ipv6/conf/all/forwarding":        "1",
		"/proc/sys/net/ipv6/conf/default/forwarding":    "1",
	}}
	sysctls := append([]Sysctl{
		{Key: "net/ipv4/ip_unprivileged_port_start", Value: "0"},
	}, forwardingSysctls...)
	if err := fake.procSys().set(sysctls); err != nil {
		t.Fatalf("set: %v", err)
	}
	if fake.remountCalls != 2 {
		t.Errorf("remount calls = %d, want 2 (one rw, one ro)", fake.remountCalls)
	}
	for key, want := range map[string]string{
		"/proc/sys/net/ipv4/ip_unprivileged_port_start": "0",
		"/proc/sys/net/ipv4/conf/all/forwarding":        "0",
		"/proc/sys/net/ipv4/conf/default/forwarding":    "0",
		"/proc/sys/net/ipv6/conf/all/forwarding":        "0",
		"/proc/sys/net/ipv6/conf/default/forwarding":    "0",
	} {
		if fake.values[key] != want {
			t.Errorf("%s = %q, want %q", key, fake.values[key], want)
		}
	}
}

func TestSetSysctlsWritesEveryFamily(t *testing.T) {
	fake := &fakeProcSys{readOnly: true, values: map[string]string{
		"/proc/sys/net/ipv4/conf/all/forwarding":     "1",
		"/proc/sys/net/ipv4/conf/default/forwarding": "1",
		"/proc/sys/net/ipv6/conf/all/forwarding":     "1",
		"/proc/sys/net/ipv6/conf/default/forwarding": "1",
	}}
	if err := fake.procSys().set(forwardingSysctls); err != nil {
		t.Fatalf("set: %v", err)
	}
	for key, v := range fake.values {
		if v != "0" {
			t.Errorf("%s = %q, want 0", key, v)
		}
	}
}

func TestSetSysctlsSkipsMissingIPv6Keys(t *testing.T) {
	values := map[string]string{}
	p := procSys{
		writeFile: func(path string, data []byte) error {
			if strings.HasPrefix(path, "/proc/sys/net/ipv6/") {
				return &os.PathError{Op: "open", Path: path, Err: unix.ENOENT}
			}
			values[path] = strings.TrimSpace(string(data))
			return nil
		},
		remount: func(bool) error {
			t.Error("remounted /proc/sys for a missing key")
			return nil
		},
	}
	if err := p.set(forwardingSysctls); err != nil {
		t.Fatalf("set without IPv6: %v", err)
	}
	for _, key := range []string{"/proc/sys/net/ipv4/conf/all/forwarding", "/proc/sys/net/ipv4/conf/default/forwarding"} {
		if values[key] != "0" {
			t.Errorf("%s = %q, want 0", key, values[key])
		}
	}
}

func TestSetSysctlsReportsAMissingIPv4Key(t *testing.T) {
	p := procSys{
		writeFile: func(path string, data []byte) error { return &os.PathError{Op: "open", Path: path, Err: unix.ENOENT} },
		remount:   func(bool) error { return nil },
	}
	if err := p.set(forwardingSysctls); !errors.Is(err, unix.ENOENT) {
		t.Fatalf("set with no IPv4 keys: got %v, want ENOENT", err)
	}
}

// A key that already reads as wanted is still written: writing
// conf/all/forwarding changes conf/all/accept_redirects, so a value read
// before the batch says nothing about the value after it.
func TestSetSysctlsWritesKeysThatAlreadyMatch(t *testing.T) {
	fake := &fakeProcSys{values: map[string]string{"/proc/sys/net/ipv4/conf/all/accept_redirects": "0"}}
	if err := fake.procSys().set([]Sysctl{{Key: "net/ipv4/conf/all/accept_redirects", Value: "0"}}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if fake.writes != 1 {
		t.Errorf("writes = %d, want 1", fake.writes)
	}
}

// fakeProcSys is a read-only /proc/sys mount shared by all callers. Its
// read-write remount is slow, so unserialized callers overlap inside it.
type fakeProcSys struct {
	mu       sync.Mutex
	readOnly bool
	values   map[string]string
	writes   int
	// remounted counts callers inside the read-write window; maxRemounted is
	// its peak.
	remounted, maxRemounted, remountCalls int
}

func (f *fakeProcSys) procSys() procSys {
	return procSys{
		writeFile: func(path string, data []byte) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.readOnly {
				return &os.PathError{Op: "open", Path: path, Err: unix.EROFS}
			}
			f.values[path] = strings.TrimSpace(string(data))
			f.writes++
			return nil
		},
		remount: func(readOnly bool) error {
			f.mu.Lock()
			f.remountCalls++
			if readOnly {
				f.readOnly = true
				f.remounted--
				f.mu.Unlock()
				return nil
			}
			time.Sleep(time.Millisecond)
			f.readOnly = false
			f.remounted++
			f.maxRemounted = max(f.maxRemounted, f.remounted)
			f.mu.Unlock()
			time.Sleep(time.Millisecond)
			return nil
		},
	}
}
