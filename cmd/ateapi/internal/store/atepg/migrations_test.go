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

var transactionControl = regexp.MustCompile(`(?im)^\s*(BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK)\s*;`)

func TestMigrationPolicy(t *testing.T) {
	err := fs.WalkDir(migrationFiles, "migrations", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".sql") {
			return nil
		}
		data, err := fs.ReadFile(migrationFiles, path)
		if err != nil {
			return err
		}
		sql := string(data)
		upperSQL := strings.ToUpper(sql)
		if strings.Count(sql, "-- +goose Up") != 1 {
			t.Errorf("%s must contain exactly one Goose Up annotation", path)
		}
		if strings.Contains(upperSQL, "-- +GOOSE DOWN") {
			t.Errorf("%s must not contain a Goose Down migration", path)
		}
		if strings.Contains(upperSQL, "-- +GOOSE NO TRANSACTION") {
			t.Errorf("%s must run in a PostgreSQL transaction", path)
		}
		if strings.Contains(upperSQL, "IF NOT EXISTS") {
			t.Errorf("%s must not contain an IF NOT EXISTS guard", path)
		}
		if strings.Contains(upperSQL, "-- +GOOSE ENVSUB") {
			t.Errorf("%s must not use Goose environment substitution", path)
		}
		if transactionControl.MatchString(sql) {
			t.Errorf("%s must let Goose control the PostgreSQL transaction", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("check PostgreSQL migrations: %v", err)
	}
}
