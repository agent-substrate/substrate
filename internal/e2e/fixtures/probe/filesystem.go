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
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

const filesystemContents = "worker filesystem state"

var filesystemPaths = []string{
	"/var/worker-security/owned",
	"/home/counter/worker-security/owned",
}

type filesystemFile struct {
	Path     string `json:"path"`
	UID      uint32 `json:"uid"`
	GID      uint32 `json:"gid"`
	Mode     uint32 `json:"mode"`
	Contents string `json:"contents"`
}

type filesystemResponse struct {
	Files []filesystemFile `json:"files"`
	Error string           `json:"error,omitempty"`
}

// filesystem creates non-root-owned files on POST and reports their actual
// metadata on GET. The same paths survive the actor's checkpoint and restore.
func filesystem(w http.ResponseWriter, r *http.Request) {
	response := filesystemResponse{}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if r.Method == http.MethodPost {
		for _, path := range filesystemPaths {
			if err := initializeFilesystemFile(path); err != nil {
				response.Error = err.Error()
				writeJSON(w, response)
				return
			}
		}
	}
	for _, path := range filesystemPaths {
		file, err := readFilesystemFile(path)
		if err != nil {
			response.Error = err.Error()
			writeJSON(w, response)
			return
		}
		response.Files = append(response.Files, file)
	}
	if r.URL.Query().Get("nonroot") == "1" {
		if err := checkNonRootFilesystem(); err != nil {
			response.Error = err.Error()
		}
	}
	writeJSON(w, response)
}

func initializeFilesystemFile(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o777); err != nil {
		parent, statErr := os.Stat(filepath.Dir(dir))
		if statErr != nil {
			return fmt.Errorf("prepare %s as uid=%d gid=%d: %w (parent stat: %v)", dir, os.Geteuid(), os.Getegid(), err, statErr)
		}
		owner := parent.Sys().(*syscall.Stat_t)
		return fmt.Errorf("prepare %s as uid=%d gid=%d: %w (parent uid=%d gid=%d mode=%o)", dir, os.Geteuid(), os.Getegid(), err, owner.Uid, owner.Gid, owner.Mode)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(filesystemContents), 0o600); err != nil {
		return err
	}
	if err := os.Chown(path, 1000, 1000); err != nil {
		return err
	}
	return os.Chmod(path, 0o755|os.ModeSetgid)
}

func readFilesystemFile(path string) (filesystemFile, error) {
	st, err := os.Stat(path)
	if err != nil {
		return filesystemFile{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return filesystemFile{}, err
	}
	stat := st.Sys().(*syscall.Stat_t)
	return filesystemFile{Path: path, UID: stat.Uid, GID: stat.Gid, Mode: uint32(stat.Mode) & 0o7777, Contents: string(data)}, nil
}

// checkNonRootFilesystem also exercises virtiofsd's credential switching and
// device-node creation. The caller enables it only on the micro-VM lane.
func checkNonRootFilesystem() error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	data, err := os.ReadFile(self)
	if err != nil {
		return err
	}
	const executable = "/var/worker-security/child"
	if err := os.WriteFile(executable, data, 0o755); err != nil {
		return err
	}
	const private = "/var/worker-security/private"
	run := func(uid uint32, operation string) ([]byte, error) {
		cmd := exec.Command(executable, "filesystem-child", operation, private)
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: uid, NoSetGroups: true}}
		return cmd.CombinedOutput()
	}
	if output, err := run(1000, "write"); err != nil {
		return fmt.Errorf("UID 1000 create: %w: %s", err, output)
	}
	if output, err := run(1000, "read"); err != nil || string(output) != filesystemContents {
		return fmt.Errorf("UID 1000 read: err=%v contents=%q", err, output)
	}
	if output, err := run(2000, "read"); err == nil || !strings.Contains(string(output), "permission denied") {
		return fmt.Errorf("UID 2000 private-file read: err=%v output=%q, want permission denied", err, output)
	}
	if err := syscall.Mknod("/var/worker-security/null", syscall.S_IFCHR|0o600, (1<<8)|3); err != nil && !os.IsExist(err) {
		return fmt.Errorf("guest device-node creation: %w", err)
	}
	return nil
}

func filesystemChild(operation, path string) error {
	switch operation {
	case "write":
		return os.WriteFile(path, []byte(filesystemContents), 0o600)
	case "read":
		data, err := os.ReadFile(path)
		if err == nil {
			_, err = os.Stdout.Write(data)
		}
		return err
	default:
		return fmt.Errorf("unknown filesystem operation %q", operation)
	}
}
