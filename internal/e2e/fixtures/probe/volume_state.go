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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/agent-substrate/substrate/internal/e2e/probeapi"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
)

// volumeStateProbe retains guest process state across snapshots. A controller
// primes it once before capture and only GETs after restore. Re-priming a
// restored process would hide cold-boot fallbacks and stale filesystem state.
// Use only a disposable writable fixture directory containing cached.txt,
// held.txt, unlinked.txt and directory/, with negative.txt absent.
type volumeStateProbe struct {
	mu                        sync.Mutex
	root, challenge           string
	cached                    string
	held, unlinked, directory *os.File
	reads                     uint64
}

func volumeResult(data []byte, err error) probeapi.VolumeObservation {
	if err != nil {
		out := probeapi.VolumeObservation{Error: err.Error(), NotExist: errors.Is(err, os.ErrNotExist)}
		if errors.Is(err, syscall.EIO) {
			out.ErrorCode = "EIO"
		}
		return out
	}
	return probeapi.VolumeObservation{Data: string(data)}
}

// Fixtures are deliberately small; oversized data is an error, never a
// truncated success that could hide the distinguishing part of a sentinel.
func volumeRead(f *os.File) probeapi.VolumeObservation {
	b := make([]byte, 4097)
	n, err := f.ReadAt(b, 0)
	if err == io.EOF {
		err = nil
	}
	if n > 4096 {
		err = fmt.Errorf("fixture exceeds 4096 bytes")
	}
	return volumeResult(b[:n], err)
}

func volumeReadPath(path string) probeapi.VolumeObservation {
	f, err := os.Open(path)
	if err != nil {
		return volumeResult(nil, err)
	}
	defer f.Close()
	return volumeRead(f)
}

func (p *volumeStateProbe) close() {
	for _, f := range []*os.File{p.held, p.unlinked, p.directory} {
		if f != nil {
			_ = f.Close()
		}
	}
	p.held, p.unlinked, p.directory = nil, nil, nil
	p.root, p.challenge, p.cached, p.reads = "", "", "", 0
}

func (p *volumeStateProbe) prime(root, challenge string) (err error) {
	if !filepath.IsAbs(root) || challenge == "" || len(challenge) > 256 {
		return fmt.Errorf("absolute root and challenge of 1..256 bytes required")
	}
	defer func() {
		if err != nil {
			p.close()
		}
	}()
	cached := volumeReadPath(filepath.Join(root, "cached.txt"))
	if cached.Error != "" {
		return fmt.Errorf("cached.txt: %s", cached.Error)
	}
	if _, err = os.Stat(filepath.Join(root, "negative.txt")); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("negative.txt must be absent (stat: %v)", err)
	}
	if p.held, err = os.Open(filepath.Join(root, "held.txt")); err != nil {
		return err
	}
	if p.directory, err = os.Open(filepath.Join(root, "directory")); err != nil {
		return err
	}
	if _, err = p.directory.Readdirnames(-1); err != nil {
		return err
	}
	if p.unlinked, err = os.Open(filepath.Join(root, "unlinked.txt")); err != nil {
		return err
	}
	// Warm the kernel's file data while retaining descriptors. The response
	// separately exposes an application-memory copy that remounting cannot reset.
	for _, f := range []*os.File{p.held, p.unlinked} {
		if v := volumeRead(f); v.Error != "" {
			return fmt.Errorf("warming fixture: %s", v.Error)
		}
	}
	if err = os.Remove(filepath.Join(root, "unlinked.txt")); err != nil {
		return err
	}
	p.root, p.challenge, p.cached = root, challenge, cached.Data
	return nil
}

func (p *volumeStateProbe) observe() probeapi.VolumeStateResponse {
	p.reads++
	out := probeapi.VolumeStateResponse{
		Challenge: p.challenge, Reads: p.reads, Cached: p.cached,
		ByPath: volumeReadPath(filepath.Join(p.root, "cached.txt")),
		Held:   volumeRead(p.held), Unlinked: volumeRead(p.unlinked),
		Negative: volumeReadPath(filepath.Join(p.root, "negative.txt")),
	}
	if _, err := p.directory.Seek(0, io.SeekStart); err != nil {
		out.DirectoryError = err.Error()
	} else {
		names, err := p.directory.Readdirnames(-1)
		out.Directory = names
		sort.Strings(out.Directory)
		if err != nil {
			out.DirectoryError = err.Error()
		}
	}
	return out
}

func (p *volumeStateProbe) serveHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch r.Method {
	case http.MethodPost:
		if p.challenge != "" {
			http.Error(w, "already primed", http.StatusConflict)
			return
		}
		if err := p.prime(r.URL.Query().Get("root"), r.URL.Query().Get("challenge")); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		if p.challenge == "" {
			http.Error(w, "not primed", http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.observe())
	case http.MethodDelete:
		p.close()
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "POST, GET, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
