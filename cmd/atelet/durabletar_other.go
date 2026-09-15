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

//go:build !linux

package main

import (
	"context"
	"errors"
	"io"
)

// tarutil archives the ownership, device nodes, and xattrs a durable-dir
// restore depends on, none of which are portable, so it is Linux-only. atelet
// runs on Linux; these keep the package building for local development
// elsewhere.

func streamDurableTarEnabled() bool { return false }

func streamDurableDirTar(_ context.Context, _ string, _ func(io.Reader) error) (int64, error) {
	return 0, errors.New("streaming durable-dir capture is only implemented on Linux")
}
