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
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// fakeRunsc mocks the runsc binary. It lets a test mock the state, list and
// delete commands and assert the calls made. Files under $FAKE_RUNSC_DIR drive
// it: <name>.state is a container runsc has a record of, <name>.filestore-gone
// makes its delete drop the record and then fail. Every state/delete/list call
// is appended to $FAKE_RUNSC_DIR/calls.
const fakeRunsc = `#!/bin/sh
cmd=""
name=""
while [ $# -gt 0 ]; do
  case "$1" in
    -root|-log-format) shift 2 ;;
    --alsologtostderr|-force|-quiet) shift ;;
    kill|wait|state|delete|list) cmd="$1"; shift ;;
    *) [ -z "$name" ] && name="$1"; shift ;;
  esac
done
dir="$FAKE_RUNSC_DIR"
case "$cmd" in
  kill|wait) exit 0 ;;
  list)
    echo list >> "$dir/calls"
    for f in "$dir"/*.state; do [ -e "$f" ] || continue; b=$(basename "$f"); echo "${b%.state}"; done
    exit 0 ;;
esac
echo "$cmd $name" >> "$dir/calls"
if [ ! -e "$dir/$name.state" ]; then
  echo '{"msg":"FetchSpec failed: loading container: file does not exist","level":"error"}' >&2
  exit 128
fi
if [ "$cmd" = delete ]; then
  rm "$dir/$name.state"
  if [ -e "$dir/$name.filestore-gone" ]; then
    echo '{"msg":"FATAL ERROR: destroying container: failed to delete filestore file: no such file or directory","level":"error"}' >&2
    exit 128
  fi
fi
exit 0
`

// newFakeRunsc installs fakeRunsc and returns a runsc wrapper pointing at it
// together with the directory holding the fake's per-container files.
func newFakeRunsc(t *testing.T) (*runsc, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "runsc")
	if err := os.WriteFile(path, []byte(fakeRunsc), 0o700); err != nil {
		t.Fatalf("write fake runsc: %v", err)
	}
	t.Setenv("FAKE_RUNSC_DIR", dir)
	return &runsc{path: path, actorUID: "actor-uid"}, dir
}

// record marks a container as one runsc still holds a state record for.
func record(t *testing.T, dir, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name+".state"), nil, 0o600); err != nil {
		t.Fatalf("write %s.state: %v", name, err)
	}
}

// markFailure configures a failure for the named container.
func markFailure(t *testing.T, dir, name, marker string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name+"."+marker), nil, 0o600); err != nil {
		t.Fatalf("write %s.%s: %v", name, marker, err)
	}
}

func recorded(dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, name+".state"))
	return err == nil
}

func fakeRunscCalls(t *testing.T, dir string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "calls"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read calls: %v", err)
	}
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

func assertCalls(t *testing.T, dir string, want ...string) {
	t.Helper()
	got := fakeRunscCalls(t, dir)
	if !slices.Equal(got, want) {
		t.Errorf("runsc calls = %v, want %v", got, want)
	}
}

var appContainers = []*ateompb.Container{{Name: "app"}}

func TestCleanupContainers_DeletesEveryContainerRunscKnows(t *testing.T) {
	rcmd, dir := newFakeRunsc(t)
	record(t, dir, "app")
	record(t, dir, "_pause")

	if err := rcmd.cleanupContainers(context.Background(), appContainers); err != nil {
		t.Fatalf("cleanupContainers: %v", err)
	}

	// Happy path: no `runsc list` at all.
	assertCalls(t, dir, "state app", "state _pause", "delete app", "delete _pause")
	for _, name := range []string{"app", "_pause"} {
		if recorded(dir, name) {
			t.Errorf("container %q survived cleanup", name)
		}
	}
	if err := rcmd.cleanupContainers(context.Background(), appContainers); err != nil {
		t.Fatalf("repeated cleanupContainers: %v", err)
	}
}

func TestCleanupContainers_SkipsContainersRunscHasNoRecordOf(t *testing.T) {
	rcmd, dir := newFakeRunsc(t)
	record(t, dir, "app")

	if err := rcmd.cleanupContainers(context.Background(), appContainers); err != nil {
		t.Fatalf("cleanupContainers: %v", err)
	}

	assertCalls(t, dir, "state app", "state _pause", "list", "delete app")
}

func TestCleanupContainers_SucceedsWhenEverythingIsAlreadyGone(t *testing.T) {
	rcmd, dir := newFakeRunsc(t)

	if err := rcmd.cleanupContainers(context.Background(), appContainers); err != nil {
		t.Fatalf("cleanupContainers: %v", err)
	}

	assertCalls(t, dir, "state app", "list", "state _pause", "list")
}

func TestCleanupContainers_RetrySucceedsAfterDeleteDroppedTheRecord(t *testing.T) {
	rcmd, dir := newFakeRunsc(t)
	record(t, dir, "app")
	record(t, dir, "_pause")
	markFailure(t, dir, "_pause", "filestore-gone")

	if err := rcmd.cleanupContainers(context.Background(), appContainers); err == nil {
		t.Fatal("cleanupContainers succeeded, want the pause delete failure")
	}
	assertCalls(t, dir, "state app", "state _pause", "delete app", "delete _pause")

	if err := rcmd.cleanupContainers(context.Background(), appContainers); err != nil {
		t.Fatalf("retried cleanupContainers: %v", err)
	}
	assertCalls(t, dir, "state app", "state _pause", "delete app", "delete _pause", "state app", "list", "state _pause", "list")
}
