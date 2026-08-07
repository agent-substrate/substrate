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

// Command counter is a simple server that will be used as a worker pod. It listens on ports 80
// and returns a greeting with the IP of the pod where it is running.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/spf13/pflag"
)

var (
	requestCount uint64
	ready        atomic.Bool
	fileMutex    sync.Mutex
)

func incrementFileCounter(filePath string) int {
	fileMutex.Lock()
	defer fileMutex.Unlock()
	counter := 0
	data, err := os.ReadFile(filePath)
	if err == nil {
		if i, err := strconv.Atoi(string(data)); err == nil {
			counter = i
		}
	}
	counter++
	err = os.WriteFile(filePath, []byte(strconv.Itoa(counter)), 0o644)
	if err != nil {
		return -1
	}
	return counter
}

func main() {
	fileCounterDirectory := pflag.String("file-counter-directory", "/home/counter", "Directory for file counter")
	secondFileCounterDirectory := pflag.String("second-file-counter-directory", "", "Directory for a second file counter; empty disables it. Used to exercise an Actor with more than one durable volume")
	validateExistingFilePath := pflag.String("validate-existing-file-path", "", "Path to existing file to validate reading")
	pflag.Parse()
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	defaultMux := http.NewServeMux()
	defaultMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		fileCounter := incrementFileCounter(filepath.Join(*fileCounterDirectory, "a.txt"))
		memoryCounter := atomic.AddUint64(&requestCount, 1)
		currentIP := getCurrentIP()

		fileContentStr := ""
		if *validateExistingFilePath != "" {
			fileContent, err := os.ReadFile(*validateExistingFilePath)
			if err != nil {
				fileResponse := fmt.Sprintf("failed to read test file: %s\n", err.Error())
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(fileResponse))
				return
			}
			fileContentStr = fmt.Sprintf(" | file content: %s", string(fileContent))
		}

		// A second counter in another directory, so an Actor with more than one
		// durable volume can show each of them persisting independently.
		secondFileCounterStr := ""
		if *secondFileCounterDirectory != "" {
			secondFileCounter := incrementFileCounter(filepath.Join(*secondFileCounterDirectory, "a.txt"))
			secondFileCounterStr = fmt.Sprintf(" | preserved second file counter: %d", secondFileCounter)
		}

		response := fmt.Sprintf("hello from: %s | preserved memory count: %d | preserved file counter: %d%s%s\n", currentIP, memoryCounter, fileCounter, secondFileCounterStr, fileContentStr)
		slog.InfoContext(ctx, "Handled request", slog.String("response", response))

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(response))
	})
	// /readyz is the endpoint the ateom-gvisor readyz probe polls. It returns
	// 200 only once initialization (the random-file write) has completed.
	// After a checkpoint+restore the atomic flag is part of the snapshot, so
	// the endpoint returns 200 immediately on resume.
	defaultMux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})

	// TODO(dberkov): remove the temporary workaround for adding a file to durDir
	// othereise gVisor does not take golden snapshot correclty.
	incrementFileCounter(filepath.Join(*fileCounterDirectory, "tmp.txt"))

	ready.Store(true)
	slog.InfoContext(ctx, "Readyz now reports OK")

	// The server is the process's reason to live: block on it instead of a
	// keep-alive loop, so the process exits when (and only when) it fails.
	slog.InfoContext(ctx, "Starting counter server on port 80")
	if err := http.ListenAndServe(":80", defaultMux); err != nil {
		slog.ErrorContext(ctx, "Error starting server", slog.Any("err", err))
		os.Exit(1)
	}
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
