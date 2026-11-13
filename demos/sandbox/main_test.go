// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func TestHandleProcessCancellation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout string
		parent  string
		code    int
	}{
		{name: "cancel", parent: "parent", code: -1},
		{name: "cancel after output closes", parent: "parent-closed", code: -1},
		{name: "timeout", timeout: "5s", parent: "parent", code: -1},
		{name: "cancel after parent exit", parent: "parent-exit", code: -1},
		{name: "timeout after parent exit", timeout: "5s", parent: "parent-exit", code: -1},
		{name: "nonzero parent exit", parent: "parent-exit", code: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { listener.Close() })
			if err := listener.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}

			body, err := json.Marshal(ProcessRequest{
				Command: []string{os.Args[0], "-test.run=^TestProcessHelper$"},
				EnvVars: map[string]string{
					"SANDBOX_TEST_PROCESS": tc.parent,
					"SANDBOX_TEST_ADDR":    listener.Addr().String(),
					"SANDBOX_TEST_EXIT":    strconv.Itoa(max(tc.code, 0)),
				},
				Timeout: tc.timeout,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			req := httptest.NewRequest(http.MethodPost, "/process", bytes.NewReader(body)).WithContext(ctx)
			rec := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				handleProcess(rec, req)
				close(done)
			}()

			conn, err := listener.AcceptTCP()
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			var pid, parentPID int
			if _, err := fmt.Fscanln(conn, &pid, &parentPID); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				// Release the helper even if cancellation leaves it running.
				conn.CloseWrite()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("handler did not finish after helper cleanup")
				}
			})

			if tc.parent == "parent-exit" {
				exited, err := processExited(parentPID)
				if err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-exited:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("parent did not exit")
				}
			}
			if tc.timeout == "" {
				cancel()
			}
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("handler blocked after cancellation")
			}
			var resp ProcessResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
			if rec.Code != http.StatusOK || resp.ExitCode != tc.code || resp.Error == "" {
				t.Errorf("status = %d, response = %+v", rec.Code, resp)
			}
			if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
				t.Errorf("descendant %d still holds its connection: %v", pid, err)
			}
		})
	}
}

func TestHandleProcessResult(t *testing.T) {
	for _, code := range []int{0, 7} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			body, err := json.Marshal(ProcessRequest{
				Command: []string{os.Args[0], "-test.run=^TestProcessHelper$"},
				EnvVars: map[string]string{
					"SANDBOX_TEST_PROCESS": "result",
					"SANDBOX_TEST_EXIT":    strconv.Itoa(code),
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			handleProcess(rec, httptest.NewRequest(http.MethodPost, "/process", bytes.NewReader(body)))
			var resp ProcessResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
			if rec.Code != http.StatusOK || resp.ExitCode != code || resp.Stdout != "stdout\n" || resp.Stderr != "stderr\n" {
				t.Errorf("status = %d, response = %+v", rec.Code, resp)
			}
			if (resp.Error != "") != (code != 0) {
				t.Errorf("exit code = %d, error = %q", code, resp.Error)
			}
		})
	}
}

func TestProcessHelper(t *testing.T) {
	switch os.Getenv("SANDBOX_TEST_PROCESS") {
	case "parent", "parent-exit", "parent-closed":
		cmd := exec.Command(os.Args[0], "-test.run=^TestProcessHelper$")
		cmd.Env = append(os.Environ(), "SANDBOX_TEST_PROCESS=child", "SANDBOX_TEST_PARENT="+strconv.Itoa(os.Getpid()))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if os.Getenv("SANDBOX_TEST_PROCESS") == "parent-closed" {
			os.Stdout.Close()
			os.Stderr.Close()
			cmd.Stdout = nil
			cmd.Stderr = nil
		}
		if err := cmd.Start(); err != nil {
			os.Exit(1)
		}
		if os.Getenv("SANDBOX_TEST_PROCESS") == "parent-exit" {
			code, err := strconv.Atoi(os.Getenv("SANDBOX_TEST_EXIT"))
			if err != nil {
				os.Exit(1)
			}
			os.Exit(code)
		}
		if err := cmd.Wait(); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	case "child":
		conn, err := net.Dial("tcp", os.Getenv("SANDBOX_TEST_ADDR"))
		if err != nil {
			os.Exit(1)
		}
		if _, err := fmt.Fprintln(conn, os.Getpid(), os.Getenv("SANDBOX_TEST_PARENT")); err != nil {
			os.Exit(1)
		}
		io.Copy(io.Discard, conn)
		os.Exit(0)
	case "result":
		fmt.Fprintln(os.Stdout, "stdout")
		fmt.Fprintln(os.Stderr, "stderr")
		code, err := strconv.Atoi(os.Getenv("SANDBOX_TEST_EXIT"))
		if err != nil {
			os.Exit(1)
		}
		os.Exit(code)
	}
}
