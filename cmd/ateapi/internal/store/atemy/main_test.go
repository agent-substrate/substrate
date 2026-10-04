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

package atemy

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/dockerenv"
)

// One MySQL container serves every test in this package, and clearAll isolates
// them, so tests here are not safe to run with -parallel.
var (
	containerOnce  sync.Once
	containerDB    *sql.DB
	containerDSN   string
	containerMySQL *mysql.MySQLContainer
	containerErr   error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if containerDB != nil {
		containerDB.Close()
	}
	if containerMySQL != nil {
		if err := containerMySQL.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "terminating MySQL testcontainer: %v\n", err)
			if code == 0 {
				code = 1
			}
		}
	}
	os.Exit(code)
}

func requireDB(t *testing.T) *sql.DB {
	t.Helper()
	containerOnce.Do(func() {
		ctx := context.Background()
		if err := dockerenv.Configure(ctx); err != nil {
			containerErr = err
			return
		}
		c, err := mysql.Run(ctx, "mysql:8.4",
			mysql.WithDatabase("atemy"),
			mysql.WithUsername("atemy"),
			mysql.WithPassword("atemy"),
		)
		if err != nil {
			containerErr = err
			return
		}
		containerMySQL = c
		dsn, err := c.ConnectionString(ctx)
		if err != nil {
			containerErr = err
			return
		}
		containerDSN = dsn
		db, err := Open(dsn)
		if err != nil {
			containerErr = err
			return
		}
		var pingErr error
		for range 30 {
			if pingErr = db.PingContext(ctx); pingErr == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if pingErr != nil {
			containerErr = fmt.Errorf("pinging MySQL testcontainer after retries: %w", pingErr)
			return
		}
		containerDB = db
	})
	if containerErr != nil {
		if dockerenv.Required() {
			t.Fatalf("MySQL testcontainer unavailable and required (CI or REQUIRE_DOCKER is set): %v", containerErr)
		}
		t.Skipf("MySQL testcontainer unavailable (requires Docker): %v", containerErr)
	}
	return containerDB
}
