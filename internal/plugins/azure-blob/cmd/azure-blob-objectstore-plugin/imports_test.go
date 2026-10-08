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
	"bufio"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const substrateModule = "go.ate.dev/substrate"

func TestImportsOnlySubstratePublicPackages(t *testing.T) {
	root := filepath.Join("..", "..")
	self := modulePath(t, filepath.Join(root, "go.mod"))
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(filePath string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); filePath != root && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(filePath, ".go") {
			return nil
		}
		file, err := parser.ParseFile(fset, filePath, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			imp, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			switch {
			case imp == self || strings.HasPrefix(imp, self+"/"):
			case strings.HasPrefix(imp, substrateModule+"/pkg/"):
			case imp == substrateModule || strings.HasPrefix(imp, substrateModule+"/"):
				t.Errorf("%s imports %s; this module may import only %s/pkg/...", filePath, imp, substrateModule)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking module: %v", err)
	}
}

func modulePath(t *testing.T, goMod string) string {
	t.Helper()
	file, err := os.Open(goMod)
	if err != nil {
		t.Fatalf("opening %s: %v", goMod, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if module, ok := strings.CutPrefix(strings.TrimSpace(scanner.Text()), "module "); ok {
			return strings.TrimSpace(module)
		}
	}
	t.Fatalf("%s names no module", goMod)
	return ""
}
