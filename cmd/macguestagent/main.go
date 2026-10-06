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

// Command macguestagent mounts a Mac Actor's durable VirtioFS shares and
// reports ready only while all configured filesystems are present.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

const configTag = "ate-config"

type guestVolume struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	Tag       string `json:"tag"`
}

type mountOperations interface {
	Mounted(context.Context, string) (bool, error)
	MountVirtioFS(context.Context, string, string) error
}

type hostMountOperations struct{}

func (hostMountOperations) Mounted(ctx context.Context, target string) (bool, error) {
	out, err := exec.CommandContext(ctx, "/usr/bin/stat", "-f", "%T\n%m", target).Output()
	if err != nil {
		return false, err
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	return len(lines) == 2 && lines[0] == "virtiofs" && lines[1] == target, nil
}

func (hostMountOperations) MountVirtioFS(ctx context.Context, tag, target string) error {
	output, err := exec.CommandContext(ctx, "/sbin/mount_virtiofs", tag, target).CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount_virtiofs %q: %w: %s", tag, err, strings.TrimSpace(string(output)))
	}
	return nil
}

type reconciler struct {
	configMount string
	mounts      mountOperations
}

func (r *reconciler) reconcile(ctx context.Context) error {
	if err := ensureMountPoint(r.configMount); err != nil {
		return err
	}
	mounted, err := r.mounts.Mounted(ctx, r.configMount)
	if err != nil {
		return fmt.Errorf("inspect configuration share: %w", err)
	}
	if !mounted {
		if err := requireEmpty(r.configMount); err != nil {
			return fmt.Errorf("configuration mount point: %w", err)
		}
		if err := r.mounts.MountVirtioFS(ctx, configTag, r.configMount); err != nil {
			return err
		}
	}
	volumes, err := readVolumes(filepath.Join(r.configMount, "volumes.json"))
	if err != nil {
		return err
	}
	for _, volume := range volumes {
		if err := ensureMountPoint(volume.MountPath); err != nil {
			return fmt.Errorf("volume %q: %w", volume.Name, err)
		}
		mounted, err := r.mounts.Mounted(ctx, volume.MountPath)
		if err != nil {
			return fmt.Errorf("inspect volume %q: %w", volume.Name, err)
		}
		if mounted {
			continue
		}
		if err := requireEmpty(volume.MountPath); err != nil {
			return fmt.Errorf("volume %q mount point: %w", volume.Name, err)
		}
		if err := r.mounts.MountVirtioFS(ctx, volume.Tag, volume.MountPath); err != nil {
			return fmt.Errorf("volume %q: %w", volume.Name, err)
		}
	}
	return nil
}

func readVolumes(filename string) ([]guestVolume, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open durable volume configuration: %w", err)
	}
	defer f.Close()
	var volumes []guestVolume
	decoder := json.NewDecoder(io.LimitReader(f, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&volumes); err != nil {
		return nil, fmt.Errorf("decode durable volume configuration: %w", err)
	}
	if len(volumes) > 32 {
		return nil, errors.New("at most 32 durable volumes are supported")
	}
	tags, paths := make(map[string]bool, len(volumes)), make(map[string]bool, len(volumes))
	for _, volume := range volumes {
		if volume.Name == "" || !validTag(volume.Tag) || !validMountPath(volume.MountPath) || tags[volume.Tag] || paths[volume.MountPath] {
			return nil, errors.New("invalid durable volume configuration")
		}
		for prior := range paths {
			if strings.HasPrefix(volume.MountPath, prior+"/") || strings.HasPrefix(prior, volume.MountPath+"/") {
				return nil, errors.New("durable volume mount paths must not nest")
			}
		}
		tags[volume.Tag], paths[volume.MountPath] = true, true
	}
	return volumes, nil
}

func validTag(value string) bool {
	if value == "" || len(value) > 36 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("-_.", r)) {
			return false
		}
	}
	return true
}

func validMountPath(value string) bool {
	return len(value) > 1 && len(value) <= 4096 && strings.HasPrefix(value, "/") &&
		!strings.HasSuffix(value, "/") && !strings.Contains(value, "//") && !strings.Contains(value, ":") &&
		path.Clean(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func ensureMountPoint(target string) error {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(target, 0o700)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("mount point is not a real directory")
	}
	return nil
}

func requireEmpty(target string) error {
	entries, err := os.ReadDir(target)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return errors.New("refusing to cover a non-empty directory")
	}
	return nil
}

func run(ctx context.Context, listen, configMount string, retry time.Duration) error {
	if listen == "" || !filepath.IsAbs(configMount) || retry <= 0 {
		return errors.New("listen address, absolute config mount, and positive retry interval are required")
	}
	var ready atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "durable volumes are not mounted", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	defer server.Shutdown(context.WithoutCancel(ctx))

	r := &reconciler{configMount: configMount, mounts: hostMountOperations{}}
	ticker := time.NewTicker(retry)
	defer ticker.Stop()
	for {
		err := r.reconcile(ctx)
		ready.Store(err == nil)
		if err != nil {
			log.Printf("durable volume reconciliation failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case err := <-serverErr:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ticker.C:
		}
	}
}

func main() {
	listen := flag.String("listen-address", ":8123", "HTTP health and readiness address")
	configMount := flag.String("config-mount", "/var/run/agent-substrate/config", "fixed VirtioFS configuration mount point")
	retry := flag.Duration("reconcile-interval", 2*time.Second, "durable volume reconciliation interval")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *listen, *configMount, *retry); err != nil {
		log.Fatal(err)
	}
}
