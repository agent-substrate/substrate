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

// Package parser builds a protoreflect.FileDescriptor for pkg/proto/ateapipb/ateapi.proto.
package parser

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	targetProtoFile = "pkg/proto/ateapipb/ateapi.proto"
	rootModule      = "module github.com/agent-substrate/substrate"
)

// Parse parses a returns the proto FileDescription of Substrate public API.
func Parse(ctx context.Context) (fd protoreflect.FileDescriptor, err error) {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return nil, err
	}
	return buildFileDescriptor(ctx, repoRoot)
}

func findRepoRoot() (string, error) {
	start, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("while getting working directory: %w", err)
	}
	for dir := start; ; {
		goMod, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err == nil {
			first, _, _ := strings.Cut(string(goMod), "\n")
			if strings.TrimSpace(first) == rootModule {
				return dir, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find a go.mod declaring %q above %s", rootModule, start)
		}
		dir = parent
	}
}

func buildFileDescriptor(ctx context.Context, repoRoot string) (protoreflect.FileDescriptor, error) {
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: []string{repoRoot},
		}),
		SourceInfoMode: protocompile.SourceInfoStandard,
	}
	files, err := compiler.Compile(ctx, targetProtoFile)
	if err != nil {
		return nil, fmt.Errorf("while compiling %s: %w", targetProtoFile, err)
	}
	if len(files) != 1 {
		return nil, fmt.Errorf("expected exactly one compiled file for %s, got %d", targetProtoFile, len(files))
	}
	return files[0], nil
}
