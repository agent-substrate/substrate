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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

type ProcessRequest struct {
	Command []string          `json:"command"`
	EnvVars map[string]string `json:"envvars,omitempty"`
	Cwd     string            `json:"cwd,omitempty"`
	Timeout string            `json:"timeout,omitempty"`
}

type ProcessResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exitCode"`
	Error    string `json:"error,omitempty"`
}

func main() {

	pattern := "/process"
	http.HandleFunc("POST "+pattern, handleProcess)

	port := os.Getenv("PORT")
	if port == "" {
		port = "80"
	}

	log.Printf("Stateless Sandbox serving at port %s, path: %s", port, pattern)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleProcess(w http.ResponseWriter, r *http.Request) {
	var req ProcessRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	if len(req.Command) == 0 {
		http.Error(w, "Command required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if req.Timeout != "" {
		duration, err := time.ParseDuration(req.Timeout)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid timeout: %v", err), http.StatusBadRequest)
			return
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, duration)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, req.Command[0], req.Command[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = nil

	if req.Cwd != "" {
		cmd.Dir = req.Cwd
	}

	if len(req.EnvVars) > 0 {
		cmd.Env = os.Environ()
		for k, v := range req.EnvVars {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	var stdout, stderr strings.Builder
	err := runCommand(ctx, cmd, &stdout, &stderr)

	resp := ProcessResponse{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}

	if err != nil {
		resp.Error = err.Error()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			resp.ExitCode = exitErr.ExitCode()
		} else {
			// If it's not an ExitError (e.g. command not found), set exit code to -1 or similar
			resp.ExitCode = -1
		}
	} else {
		resp.ExitCode = 0
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func runCommand(ctx context.Context, cmd *exec.Cmd, stdout, stderr io.Writer) error {
	out, outWrite, err := os.Pipe()
	if err != nil {
		return err
	}
	defer out.Close()
	defer outWrite.Close()
	errOut, errWrite, err := os.Pipe()
	if err != nil {
		return err
	}
	defer errOut.Close()
	defer errWrite.Close()
	cmd.Stdout = outWrite
	cmd.Stderr = errWrite
	if err := cmd.Start(); err != nil {
		return err
	}
	outWrite.Close()
	errWrite.Close()

	copied := make(chan error, 2)
	go func() { _, err := io.Copy(stdout, out); copied <- err }()
	go func() { _, err := io.Copy(stderr, errOut); copied <- err }()
	canceled := make(chan error, 1)
	stop := context.AfterFunc(ctx, func() {
		canceled <- syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	})

	exited, err := processExited(cmd.Process.Pid)
	if err == nil {
		err = <-exited
	}
	if err != nil {
		if killErr := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
			err = errors.Join(err, killErr)
		}
		out.Close()
		errOut.Close()
	}
	err = errors.Join(err, <-copied, <-copied)
	if !stop() {
		if cancelErr := <-canceled; cancelErr != nil && !errors.Is(cancelErr, syscall.ESRCH) {
			err = errors.Join(err, fmt.Errorf("kill process group: %w", cancelErr))
		}
		if err == nil {
			err = ctx.Err()
		}
	}
	// Keep the leader unreaped until group signaling and output collection finish.
	waitErr := cmd.Wait()
	if err != nil {
		return errors.Join(err, waitErr)
	}
	return waitErr
}
