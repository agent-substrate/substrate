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

// Command fspersist is a simple server used as an actor workload. It listens on port
// 80 and, on each request, returns the IP of the pod where it is running along
// with a history of past appends read from a file on its root filesystem.
//
// It exists alongside the counter demo to show that two ActorTemplates running
// two entirely different binaries can share a single WorkerPool. Whereas the
// counter demo proves that *memory* survives gVisor suspend/resume by returning
// an incremented in-memory count on each request, this binary proves that the
// *filesystem* does: on each request it prepends a line recording the current
// pod IP and a running count to a history file (capped at 20 lines), and the
// count is read back from that persisted file rather than from memory. Because
// the file follows the actor across checkpoint/restore, its history accumulates
// and the recorded pod IPs reveal each move onto a new worker.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// historyPath is the file on the actor's root filesystem whose persistence
// across checkpoint/restore this demo exercises.
const historyPath = "/pod-history.log"

// maxLines caps the history file so it stays small and readable.
const maxLines = 20

// fileMu prevents concurrent writes to the history file.
var fileMu sync.Mutex

func main() {
	flag.Parse()
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// On each request, append a new line to the persisted history file and
		// return the result, mirroring how the counter demo returns an
		// incremented count on each request.
		ip := getCurrentIP()
		data, err := appendHistory(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to persist history", slog.Any("err", err))
			http.Error(w, "failed to persist history", http.StatusInternalServerError)
			return
		}
		response := fmt.Sprintf("pod: %s\n--- history ---\n%s", ip, data)
		slog.InfoContext(ctx, "Handled request", slog.String("response", response))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(response))
	})

	slog.InfoContext(ctx, "Starting fspersist server on port 80")
	if err := http.ListenAndServe(":80", mux); err != nil {
		slog.ErrorContext(ctx, "Error starting server", slog.Any("err", err))
		os.Exit(1)
	}
}

// appendHistory prepends a line recording the current pod IP and the next count
// to the history file, capping it at maxLines, and returns the file's new
// contents. The count is derived from the persisted file rather than from
// memory, so it survives checkpoint/restore.
func appendHistory(ctx context.Context) ([]byte, error) {
	fileMu.Lock()
	defer fileMu.Unlock()

	ip := getCurrentIP()

	var lines []string
	if data, err := os.ReadFile(historyPath); err == nil {
		for _, l := range strings.Split(string(data), "\n") {
			if strings.TrimSpace(l) != "" {
				lines = append(lines, l)
			}
		}
	}

	count := nextCount(lines)
	line := fmt.Sprintf("pod=%s | count=%d | time=%s", ip, count, time.Now().Format(time.RFC3339))
	lines = append([]string{line}, lines...)
	if len(lines) > maxLines {
		lines = lines[:maxLines]
	}

	out := []byte(strings.Join(lines, "\n") + "\n")
	if err := os.WriteFile(historyPath, out, 0o644); err != nil {
		slog.ErrorContext(ctx, "Error writing history file", slog.Any("err", err))
		return out, err
	}
	slog.InfoContext(ctx, "Appended history line", slog.String("line", line))
	return out, nil
}

// nextCount derives the next count from the most recent (first) history line,
// returning 0 when there is no prior history. The count is read back from the
// persisted file so it accumulates across checkpoint/restore.
func nextCount(lines []string) int {
	if len(lines) == 0 {
		return 0
	}
	for _, field := range strings.Split(lines[0], "|") {
		field = strings.TrimSpace(field)
		if v, ok := strings.CutPrefix(field, "count="); ok {
			if n, err := strconv.Atoi(v); err == nil {
				return n + 1
			}
		}
	}
	return 0
}

func getCurrentIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		slog.Error("Error getting interface addresses", slog.Any("err", err))
		return "x.x.x.x"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "y.y.y.y"
}
