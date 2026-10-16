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

package register

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// watchPollInterval is how often WatchFilePath signals when fsnotify is
// unavailable (for example, when the node's inotify instance budget is
// exhausted). A var so tests can shorten it.
var watchPollInterval = 5 * time.Second

// WatchFilePath signals on the returned channel whenever the file at path is
// created, written, or replaced, or when the underlying watcher reports an
// error (such as an event queue overflow) so the caller can re-read the file.
// It watches path's parent directory, which must exist, so atomic file
// replacements via rename are detected even when the file itself does not
// exist yet at startup.
//
// If fsnotify cannot watch the parent directory (for example, when the host
// UID's fs.inotify.max_user_instances budget is exhausted), WatchFilePath logs a
// warning and falls back to signaling periodically at watchPollInterval until
// ctx ends. The returned channel is closed when ctx ends.
func WatchFilePath(ctx context.Context, path string) <-chan struct{} {
	cleanPath := filepath.Clean(path)
	changes := make(chan struct{}, 1)
	w, err := fsnotify.NewWatcher()
	if err != nil {
		slog.WarnContext(ctx, "Could not create fsnotify watcher; falling back to polling",
			slog.String("path", cleanPath), slog.Duration("interval", watchPollInterval), slog.Any("err", err))
		go pollFileLoop(ctx, watchPollInterval, changes)
		return changes
	}
	dir := filepath.Dir(cleanPath)
	if err := w.Add(dir); err != nil {
		_ = w.Close()
		slog.WarnContext(ctx, "Could not watch directory with fsnotify; falling back to polling",
			slog.String("dir", dir), slog.String("path", cleanPath), slog.Duration("interval", watchPollInterval), slog.Any("err", err))
		go pollFileLoop(ctx, watchPollInterval, changes)
		return changes
	}
	go func() {
		defer close(changes)
		defer func() {
			if err := w.Close(); err != nil {
				slog.WarnContext(ctx, "Failed to close file watcher", slog.String("path", cleanPath), slog.Any("err", err))
			}
		}()
		watchFileLoop(ctx, cleanPath, w.Events, w.Errors, changes)
	}()
	return changes
}

func pollFileLoop(ctx context.Context, interval time.Duration, changes chan<- struct{}) {
	defer close(changes)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			select {
			case changes <- struct{}{}:
			default:
			}
		}
	}
}

func watchFileLoop(ctx context.Context, cleanPath string, events <-chan fsnotify.Event, errs <-chan error, changes chan<- struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			if !isFileModifyEvent(cleanPath, ev) {
				continue
			}
			select {
			case changes <- struct{}{}:
			default: // a signal is already pending
			}
		case err, ok := <-errs:
			if !ok {
				return
			}
			slog.WarnContext(ctx, "File watch error; signaling re-read", slog.String("path", cleanPath), slog.Any("err", err))
			select {
			case changes <- struct{}{}:
			default:
			}
		}
	}
}

func isFileModifyEvent(cleanPath string, ev fsnotify.Event) bool {
	if filepath.Clean(ev.Name) != cleanPath {
		return false
	}
	return ev.Has(fsnotify.Create) || ev.Has(fsnotify.Write)
}
