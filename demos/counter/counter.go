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
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

var requestCount uint64

func main() {
	flag.Parse()
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	defaultMux := http.NewServeMux()
	defaultMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		count := atomic.AddUint64(&requestCount, 1)
		currentIP := getCurrentIP()
		response := fmt.Sprintf("hello from: %s | preserved memory count: %d\n", currentIP, count)
		slog.InfoContext(ctx, "Handled request", slog.String("response", response))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(response))
	})

	go func() {
		//time.Sleep(60 * time.Second)
		slog.InfoContext(ctx, "Starting server on port 80")
		if err := http.ListenAndServe(":80", defaultMux); err != nil {
			slog.ErrorContext(ctx, "Error starting server", slog.Any("err", err))
			os.Exit(1)
		}
	}()

	// Write some random data to a file in the root filesystem, to test
	// filesystem checkpoint/restore.
	if err := writeRandomFile(); err != nil {
		slog.InfoContext(ctx, "Error writing random file", slog.Any("err", err))
	}

	count := 0
	if err := pingGoogle(ctx); err != nil {
		slog.ErrorContext(ctx, "Error pinging Google", slog.Any("err", err))
	}
	slog.InfoContext(ctx, "Count", slog.Int("count", count), slog.String("fshash", hashRandomFile()))
	count++

	for range time.Tick(10 * time.Second) {
		if err := pingGoogle(ctx); err != nil {
			slog.ErrorContext(ctx, "Error pinging Google", slog.Any("err", err))
		}
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

// Test outbound connectivity
func pingGoogle(ctx context.Context) error {
	// resp, err := http.Get("https://www.google.com")
	// if err != nil {
	// 	return fmt.Errorf("while requesting https://www.google.com: %w", err)
	// }
	// defer resp.Body.Close()
	// bodyBytes, err := io.ReadAll(resp.Body)
	// if err != nil {
	// 	return fmt.Errorf("while reading body: %w", err)
	// }

	// if resp.StatusCode != 200 {
	// 	return fmt.Errorf("bad response code=%d body=%s", resp.StatusCode, string(bodyBytes))
	// }

	return nil
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
