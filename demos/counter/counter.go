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

func main() {
	counterDir := pflag.String("file-counter-directory", "/home/counter", "Directory for file counter")
	secondCounterDir := pflag.String("second-file-counter-directory", "", "Directory for a second file counter; empty disables it. Used to exercise an Actor with more than one durable volume")
	validateExistingFilePath := pflag.String("validate-existing-file-path", "", "Path to an existing file to validate reading; empty disables it")
	extraPort := pflag.Int("extra-port", 0, "Additional port to listen on to test atenet-router arbitrary-port ingress; 0 disables it")
	tcpPort := pflag.Int("tcp-port", 0, "Port for TCP echo to test atunnel CONNECT ingress; 0 disables it")
	pflag.Parse()

	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	srv := newServer(*counterDir, *secondCounterDir, *validateExistingFilePath)
	srv.handleSignals(ctx)

	go startHTTPServer(ctx, ":80", srv, "Starting counter server on port 80")

	if *extraPort > 0 {
		go startExtraPortServer(ctx, *extraPort)
	}

	if *tcpPort > 0 {
		go startTCPEchoServer(ctx, *tcpPort)
	}

	// Write random data to a file in the root filesystem to test checkpoint/restore.
	if err := writeRandomFile(); err != nil {
		slog.InfoContext(ctx, "Error writing random file", slog.Any("err", err))
	} else {
		slog.InfoContext(ctx, "Wrote content to random file", slog.String("fshash", hashRandomFile()))
	}

	srv.setReady()
	slog.InfoContext(ctx, "Readyz now reports OK")

	logPeriodically(ctx)
}

type server struct {
	mux *http.ServeMux

	counterDir               string
	secondCounterDir         string
	validateExistingFilePath string

	requestCount atomic.Uint64
	ready        atomic.Bool

	shutdownDelaySecs atomic.Int64

	fileMu sync.Mutex // guards file operations
}

func newServer(counterDir, secondCounterDir, validateExistingFilePath string) *server {
	s := &server{
		mux:                      http.NewServeMux(),
		counterDir:               counterDir,
		secondCounterDir:         secondCounterDir,
		validateExistingFilePath: validateExistingFilePath,
	}
	s.mux.HandleFunc("/", s.handle)
	s.mux.HandleFunc("/readyz", s.handleReadyz)
	s.mux.HandleFunc("/set-sigterm-sleep", s.storeShutdownDelay)
	s.shutdownDelaySecs.Store(15)
	return s
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *server) setReady() {
	s.ready.Store(true)
}

func (s *server) increment(path string) int {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()

	var counter int
	if data, err := os.ReadFile(path); err == nil {
		if i, err := strconv.Atoi(string(data)); err == nil {
			counter = i
		}
	}
	counter++

	if err := os.WriteFile(path, []byte(strconv.Itoa(counter)), 0o644); err != nil {
		return -1
	}
	return counter
}

func (s *server) handleSignals(ctx context.Context) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM)
	go func() {
		sig := <-ch
		secs := s.shutdownDelaySecs.Load()
		slog.InfoContext(ctx, "Received signal, waiting before exiting", slog.String("signal", sig.String()), slog.Int64("sleep_secs", secs))
		time.Sleep(time.Duration(secs) * time.Second)
		slog.InfoContext(ctx, "Exiting now")
		os.Exit(0)
	}()
}

func (s *server) handle(w http.ResponseWriter, r *http.Request) {
	fileCounter := s.increment(filepath.Join(s.counterDir, "a.txt"))
	memoryCounter := s.requestCount.Add(1)
	currentIP := resolveCurrentIP()

	var content string
	if s.validateExistingFilePath != "" {
		fileContent, err := os.ReadFile(s.validateExistingFilePath)
		if err != nil {
			fmt.Fprintf(w, "failed to read test file: %s\n", err)
			return
		}
		content = fmt.Sprintf(" | file content: %s", string(fileContent))
	}

	// Optional second counter in another directory for multi-volume persistence test.
	var secondContent string
	if s.secondCounterDir != "" {
		secondCounter := s.increment(filepath.Join(s.secondCounterDir, "a.txt"))
		secondContent = fmt.Sprintf(" | preserved second file counter: %d", secondCounter)
	}

	body := fmt.Sprintf("hello from: %s | preserved memory count: %d | preserved file counter: %d%s%s\n",
		currentIP, memoryCounter, fileCounter, secondContent, content)
	slog.InfoContext(r.Context(), "Handled request", slog.String("body", body))

	fmt.Fprint(w, body)
}

// handleReadyz is polled by the ateom-gvisor ready probe.
func (s *server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	fmt.Fprint(w, "ok\n")
}

func (s *server) storeShutdownDelay(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("duration")
	if raw == "" {
		http.Error(w, "missing duration parameter", http.StatusBadRequest)
		return
	}
	secs, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || secs < 0 {
		http.Error(w, "invalid duration parameter", http.StatusBadRequest)
		return
	}
	s.shutdownDelaySecs.Store(secs)
	slog.InfoContext(r.Context(), "Updated SIGTERM sleep duration", slog.Int64("duration_secs", secs))
	fmt.Fprintf(w, "SIGTERM sleep duration set to %d seconds\n", secs)
}

func startHTTPServer(ctx context.Context, addr string, handler http.Handler, msg string) {
	slog.InfoContext(ctx, msg)
	if err := http.ListenAndServe(addr, handler); err != nil {
		slog.ErrorContext(ctx, "Error starting HTTP server", slog.String("addr", addr), slog.Any("err", err))
		os.Exit(1)
	}
}

func startExtraPortServer(ctx context.Context, port int) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body := fmt.Sprintf("hello from extra port %d on pod %s\n", port, resolveCurrentIP())
		slog.InfoContext(r.Context(), "Handled extra-port request", slog.String("body", body))
		fmt.Fprint(w, body)
	})

	addr := fmt.Sprintf(":%d", port)
	startHTTPServer(ctx, addr, mux, fmt.Sprintf("Starting counter extra-port server on port %d", port))
}

func startTCPEchoServer(ctx context.Context, port int) {
	addr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		slog.ErrorContext(ctx, "Error starting counter TCP echo server", slog.Int("port", port), slog.Any("err", err))
		os.Exit(1)
	}
	defer listener.Close()
	slog.InfoContext(ctx, "Starting counter TCP echo server", slog.Int("port", port))

	for {
		conn, err := listener.Accept()
		if err != nil {
			slog.ErrorContext(ctx, "Counter TCP echo accept failed", slog.Any("err", err))
			return
		}
		go func() {
			defer conn.Close()
			_, _ = io.Copy(conn, conn)
		}()
	}
}

func logPeriodically(ctx context.Context) {
	count := 0
	slog.InfoContext(ctx, "Count", slog.Int("count", count), slog.String("fshash", hashRandomFile()))
	count++

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// TODO(liorlieberman): Test outbound connectivity by pinging google.com
		slog.InfoContext(ctx, "Count", slog.Int("count", count), slog.String("fshash", hashRandomFile()))
		count++
	}
}

func writeRandomFile() error {
	rf, err := os.Create("/random-content-file")
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer rf.Close()

	if _, err := io.CopyN(rf, rand.Reader, 1*1024*1024); err != nil {
		return fmt.Errorf("copying random data: %w", err)
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

func resolveCurrentIP() string {
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
