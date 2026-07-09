//go:build !linux

/*
Copyright 2024 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package atomicwriter

import (
	"log/slog"
	"runtime"
)

// lchown changes the numeric uid and gid of the named file.
// If the file is a symbolic link, it changes the uid and gid of the link itself.
// This is a no-op on unsupported platforms.
func (w *AtomicWriter) lchown(name string, uid, _ /* gid */ int) error {
	slog.Warn("skipping change of Linux owner; unsupported on this platform", slog.Int("uid", uid), slog.String("name", name), slog.String("goos", runtime.GOOS))
	return nil
}
