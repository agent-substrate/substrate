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

package atepg

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"
)

type migrationGuard struct {
	name string
	re   *regexp.Regexp
}

var upMigrationGuards = []migrationGuard{
	{"CREATE TABLE", regexp.MustCompile(`(?i)\bCREATE\s+(?:UNLOGGED\s+)?TABLE\s+(\w+)`)},
	{"CREATE INDEX", regexp.MustCompile(`(?i)\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(\w+)`)},
	{"CREATE EXTENSION", regexp.MustCompile(`(?i)\bCREATE\s+EXTENSION\s+(\w+)`)},
	{"ADD COLUMN", regexp.MustCompile(`(?i)\bADD\s+COLUMN\s+(\w+)`)},
}

var downMigrationGuards = []migrationGuard{
	{"DROP TABLE", regexp.MustCompile(`(?i)\bDROP\s+TABLE\s+(\w+)`)},
	{"DROP INDEX", regexp.MustCompile(`(?i)\bDROP\s+INDEX\s+(\w+)`)},
	{"DROP EXTENSION", regexp.MustCompile(`(?i)\bDROP\s+EXTENSION\s+(\w+)`)},
	{"DROP COLUMN", regexp.MustCompile(`(?i)\bDROP\s+COLUMN\s+(\w+)`)},
}

func TestMigrationGuards(t *testing.T) {
	err := fs.WalkDir(migrationFiles, "migrations", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		var guards []migrationGuard
		switch {
		case strings.HasSuffix(path, ".up.sql"):
			guards = upMigrationGuards
		case strings.HasSuffix(path, ".down.sql"):
			guards = downMigrationGuards
		default:
			return nil
		}
		data, err := fs.ReadFile(migrationFiles, path)
		if err != nil {
			return err
		}
		for _, guard := range guards {
			for _, match := range guard.re.FindAllStringSubmatch(string(data), -1) {
				if !strings.EqualFold(match[1], "if") {
					t.Errorf("%s requires an existence guard in %s: %q", guard.name, path, match[0])
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking PostgreSQL migrations: %v", err)
	}
}
