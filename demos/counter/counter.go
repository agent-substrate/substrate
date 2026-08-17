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
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/pflag"
)

var (
	requestCount             uint64
	ready                    atomic.Bool
	fileMutex                sync.Mutex
	sigtermSleepDurationSecs atomic.Int64
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
	sigtermSleepDurationSecs.Store(15)
	fileCounterDirectory := pflag.String("file-counter-directory", "/home/counter", "Directory for file counter")
	secondFileCounterDirectory := pflag.String("second-file-counter-directory", "", "Directory for a second file counter; empty disables it. Used to exercise an Actor with more than one durable volume")
	validateExistingFilePath := pflag.String("validate-existing-file-path", "", "Path to existing file to validate reading")
	extraPort := pflag.Int("extra-port", 0, "Additional port to listen on, for exercising atenet-router's arbitrary-port ingress support; 0 disables it")
	pflag.Parse()
	ctx := context.Background()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.InfoContext(ctx, "Received signal, waiting before exiting", slog.String("signal", sig.String()), slog.Int64("sleep_secs", sigtermSleepDurationSecs.Load()))
		time.Sleep(time.Duration(sigtermSleepDurationSecs.Load()) * time.Second)
		slog.InfoContext(ctx, "Exiting now")
		os.Exit(0)
	}()

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

	defaultMux.HandleFunc("/set-sigterm-sleep", func(w http.ResponseWriter, r *http.Request) {
		durationStr := r.URL.Query().Get("duration")
		if durationStr == "" {
			http.Error(w, "missing duration parameter", http.StatusBadRequest)
			return
		}
		d, err := strconv.Atoi(durationStr)
		if err != nil || d < 0 {
			http.Error(w, "invalid duration parameter", http.StatusBadRequest)
			return
		}
		sigtermSleepDurationSecs.Store(int64(d))
		response := fmt.Sprintf("SIGTERM sleep duration set to %d seconds\n", d)
		slog.InfoContext(r.Context(), "Updated SIGTERM sleep duration", slog.Int("duration_secs", d))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(response))
	})

	go func() {
		slog.InfoContext(ctx, "Starting counter server on port 80")
		if err := http.ListenAndServe(":80", defaultMux); err != nil {
			slog.ErrorContext(ctx, "Error starting server", slog.Any("err", err))
			os.Exit(1)
		}
	}()

	// A second, independent listener a test can address to prove traffic
	// actually reached this port rather than falling through to the default
	// one -- see atenet-router's arbitrary-port ingress support.
	if *extraPort > 0 {
		extraMux := http.NewServeMux()
		extraMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			response := fmt.Sprintf("hello from extra port %d on pod %s\n", *extraPort, getCurrentIP())
			slog.InfoContext(r.Context(), "Handled extra-port request", slog.String("response", response))
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(response))
		})
		go func() {
			addr := fmt.Sprintf(":%d", *extraPort)
			slog.InfoContext(ctx, "Starting counter extra-port server", slog.Int("port", *extraPort))
			if err := http.ListenAndServe(addr, extraMux); err != nil {
				slog.ErrorContext(ctx, "Error starting extra-port server", slog.Any("err", err))
				os.Exit(1)
			}
		}()
	}

	// Write some random data to a file in the root filesystem, to test
	// filesystem checkpoint/restore.
	if err := writeRandomFile(); err != nil {
		slog.InfoContext(ctx, "Error writing random file", slog.Any("err", err))
	} else {
		slog.InfoContext(ctx, "Wrote content to random file", slog.String("fshash", hashRandomFile()))
	}

	ready.Store(true)
	slog.InfoContext(ctx, "Readyz now reports OK")

	count := 0
	slog.InfoContext(ctx, "Count", slog.Int("count", count), slog.String("fshash", hashRandomFile()))
	count++

	for range time.Tick(10 * time.Second) {
		// TODO: Test outbound connectivity by pinging google.com
		slog.InfoContext(ctx, "Count", slog.Int("count", count), slog.String("fshash", hashRandomFile()))
		count++
	}
}

func writeRandomFile() error {
	rf, err := os.Create("/random-content-file")
	if err != nil {
		return fmt.Errorf("while opening file: %w", err)
	}
	defer rf.Close()

	_, err = io.CopyN(rf, rand.Reader, 1*1024*1024)
	if err != nil {
		return fmt.Errorf("while copying rand data: %w", err)
	}

	return nil
}

func hashRandomFile() string {
	rfBytes, err := os.ReadFile("/random-content-file")
	if err != nil {
		panic(err)
	}

	hash := sha256.Sum256(rfBytes)
	return base64.RawStdEncoding.EncodeToString(hash[:])
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
