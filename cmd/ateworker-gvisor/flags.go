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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"
)

// commandFlags holds per-command flags for a single command.
type commandFlags struct {
	// GlobalFlags are passed before the command name (e.g.
	// "--cpu-num-from-quota", "-allow-connected-on-save").
	GlobalFlags []string `json:"globalFlags,omitempty"`
	// SubcommandFlags are passed after the command name (e.g.
	// "-background", "-detach", "-force", "-quiet").
	SubcommandFlags []string `json:"subcommandFlags,omitempty"`
}

// flagConfig defines global and per-command flags passed to runsc.
type flagConfig struct {
	// GlobalFlags are global flags passed to every command before the command name.
	GlobalFlags []string `json:"globalFlags"`
	// Commands maps a command name (e.g. "create", "restore") to its
	// per-command flags.
	Commands map[string]commandFlags `json:"commands"`
}

// globalFlags returns the combined global flags for command: top-level
// GlobalFlags followed by the command's own GlobalFlags.
func (c *flagConfig) globalFlags(command string) []string {
	cmdFlags := c.Commands[command]
	out := make([]string, 0, len(c.GlobalFlags)+len(cmdFlags.GlobalFlags))
	out = append(out, c.GlobalFlags...)
	out = append(out, cmdFlags.GlobalFlags...)
	return out
}

// subcommandFlags returns the subcommand flags for command.
func (c *flagConfig) subcommandFlags(command string) []string {
	return c.Commands[command].SubcommandFlags
}

// parseFlagConfig parses and validates a JSON flags configuration.
func parseFlagConfig(data []byte) (*flagConfig, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var cfg flagConfig
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("invalid flags JSON: %w", err)
	}
	if dec.More() {
		return nil, errors.New("invalid flags JSON: unexpected trailing data")
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("invalid flags JSON: unexpected trailing tokens")
	}
	return &cfg, nil
}

// loadFlagsFile reads and parses a JSON flags configuration file.
func loadFlagsFile(path string) (*flagConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("while reading flags config %q: %w", path, err)
	}
	cfg, err := parseFlagConfig(data)
	if err != nil {
		return nil, fmt.Errorf("while parsing flags config %q: %w", path, err)
	}
	return cfg, nil
}

// flagStore holds the active flagConfig in memory behind a lock and watches
// its backing file to reload changes asynchronously without file I/O on the hot
// path.
type flagStore struct {
	path string

	mu  sync.RWMutex
	cfg *flagConfig

	// onReload is an optional hook called after each reload attempt (for tests).
	onReload func()
}

// newFlagStore creates an empty store if path is empty, or reads path once to
// populate the initial configuration and starts watching it for changes until
// ctx is cancelled.
func newFlagStore(ctx context.Context, path string) (*flagStore, error) {
	if path == "" {
		return &flagStore{
			cfg: &flagConfig{},
		}, nil
	}

	cleanPath := filepath.Clean(path)
	cfg, err := loadFlagsFile(cleanPath)
	if err != nil {
		return nil, err
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("while creating fsnotify watcher for %q: %w", cleanPath, err)
	}

	// Watch the parent directory so both in-place writes and atomic file
	// replacements (renames) are detected.
	dir := filepath.Dir(cleanPath)
	if err := watcher.Add(dir); err != nil {
		_ = watcher.Close()
		return nil, fmt.Errorf("while watching directory %q for %q: %w", dir, cleanPath, err)
	}

	store := &flagStore{
		path: cleanPath,
		cfg:  cfg,
	}
	go store.watchLoop(ctx, watcher)
	return store, nil
}

// get returns the current immutable flagConfig under a read lock.
func (s *flagStore) get() *flagConfig {
	s.mu.RLock()
	cfg := s.cfg
	s.mu.RUnlock()
	return cfg
}

func (s *flagStore) watchLoop(ctx context.Context, watcher *fsnotify.Watcher) {
	defer func() {
		if err := watcher.Close(); err != nil {
			slog.Warn("Failed to close flags config watcher", slog.String("path", s.path), slog.Any("err", err))
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if s.isConfigEvent(event) {
				s.reload()
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			slog.Error("Error watching flags configuration file",
				slog.String("path", s.path),
				slog.Any("err", err))
		}
	}
}

func (s *flagStore) isConfigEvent(event fsnotify.Event) bool {
	if filepath.Clean(event.Name) != s.path {
		return false
	}
	return event.Has(fsnotify.Write) || event.Has(fsnotify.Create)
}

func (s *flagStore) reload() {
	defer func() {
		if s.onReload != nil {
			s.onReload()
		}
	}()

	cfg, err := loadFlagsFile(s.path)
	if err != nil {
		slog.Error("Failed to reload flags configuration; ignoring file changes",
			slog.String("path", s.path),
			slog.Any("err", err))
		return
	}

	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
	slog.Info("Reloaded flags configuration", slog.String("path", s.path))
}
