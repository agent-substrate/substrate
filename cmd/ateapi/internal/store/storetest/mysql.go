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

package storetest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/atemy"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/dockerenv"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

var (
	mysqlOnce      sync.Once
	mysqlAdmin     *sql.DB
	mysqlAdminDSN  string
	containerMySQL *mysql.MySQLContainer
	mysqlErr       error
)

// SetupMySQLPersistence returns an isolated atemy persistence instance.
func SetupMySQLPersistence(t *testing.T) *atemy.Persistence {
	t.Helper()
	ctx := context.Background()
	admin := requireMySQLAdmin(t)
	databaseName := nextDatabaseName()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+databaseName); err != nil {
		t.Fatalf("creating MySQL test database: %v", err)
	}

	cfg, err := gomysql.ParseDSN(mysqlAdminDSN)
	if err != nil {
		t.Fatalf("parsing MySQL test DSN: %v", err)
	}
	cfg.DBName = databaseName
	db, err := atemy.Open(cfg.FormatDSN())
	if err != nil {
		t.Fatalf("connecting to MySQL test database: %v", err)
	}
	persistence, err := atemy.NewPersistence(ctx, db)
	if err != nil {
		db.Close()
		t.Fatalf("creating MySQL persistence: %v", err)
	}
	t.Cleanup(func() {
		persistence.Close()
		db.Close()
		if _, err := admin.ExecContext(context.Background(), "DROP DATABASE "+databaseName); err != nil {
			t.Errorf("dropping MySQL test database: %v", err)
		}
	})
	// The server always wires a PolicyManager, so test stores do too.
	fgaServer, err := authz.NewOpenFGAServer(authz.MySQLBackend(db))
	if err != nil {
		t.Fatalf("creating OpenFGA server: %v", err)
	}
	t.Cleanup(fgaServer.Close)
	_, policyManager, err := authz.New(ctx, authz.MySQLBackend(db), fgaServer, nil)
	if err != nil {
		t.Fatalf("initializing OpenFGA authz: %v", err)
	}
	persistence.SetPolicyManager(policyManager)
	return persistence
}

// shutdownMySQL terminates the shared MySQL container, if one was started.
func shutdownMySQL() {
	if mysqlAdmin != nil {
		mysqlAdmin.Close()
		mysqlAdmin = nil
	}
	if containerMySQL != nil {
		if err := containerMySQL.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "terminating MySQL testcontainer: %v\n", err)
		}
		containerMySQL = nil
	}
}

// requireMySQLAdmin starts the shared MySQL container as root, which test
// setup needs to create a database per test. Production atemy never creates
// databases.
func requireMySQLAdmin(t *testing.T) *sql.DB {
	t.Helper()
	mysqlOnce.Do(func() {
		ctx := context.Background()
		if err := dockerenv.Configure(ctx); err != nil {
			mysqlErr = err
			return
		}
		containerMySQL, mysqlErr = mysql.Run(ctx, "mysql:8.4",
			mysql.WithDatabase("storetest"),
			mysql.WithUsername("root"),
			mysql.WithPassword("root"),
		)
		if mysqlErr != nil {
			return
		}
		mysqlAdminDSN, mysqlErr = containerMySQL.ConnectionString(ctx)
		if mysqlErr != nil {
			return
		}
		mysqlAdmin, mysqlErr = atemy.Open(mysqlAdminDSN)
		if mysqlErr != nil {
			return
		}
		var pingErr error
		for range 30 {
			if pingErr = mysqlAdmin.PingContext(ctx); pingErr == nil {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		mysqlAdmin.Close()
		mysqlAdmin = nil
		mysqlErr = fmt.Errorf("pinging MySQL testcontainer after retries: %w", pingErr)
	})
	if mysqlErr != nil {
		if dockerenv.Required() {
			t.Fatalf("MySQL testcontainer unavailable and required (CI or REQUIRE_DOCKER is set): %v", mysqlErr)
		}
		t.Skipf("MySQL testcontainer unavailable (requires Docker): %v", mysqlErr)
	}
	return mysqlAdmin
}
