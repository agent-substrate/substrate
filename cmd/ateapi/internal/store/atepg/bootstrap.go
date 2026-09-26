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
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	OwnerRoleName            = "substrate_owner"
	ReadWriteRoleName        = "substrate_readwrite"
	OwnerUserName            = "substrate_admin_user"
	ReadWriteUserName        = "substrate_readwrite_user"
	defaultOwnerPassword     = "substrate-admin"
	defaultReadWritePassword = "substrate-readwrite"
)

//go:embed identity.sql
var identitySQL string

// BootstrapConfig contains the first-install PostgreSQL credentials.
// EndpointSource is the fixed owner login connection string.
type BootstrapConfig struct {
	EndpointSource  string
	ReadWriteSource string
	AdminUsername   string
	AdminPassword   string
	Schema          string
}

// Bootstrap creates the fixed Substrate identities and schema permissions.
// It does not change passwords for existing users.
func Bootstrap(ctx context.Context, cfg BootstrapConfig) error {
	if cfg.EndpointSource == "" {
		return errors.New("PostgreSQL connection string must not be empty")
	}
	if cfg.ReadWriteSource == "" {
		return errors.New("PostgreSQL read/write connection string must not be empty")
	}
	if cfg.Schema == "" {
		return errors.New("PostgreSQL schema must not be empty")
	}
	for name, value := range map[string]string{
		"administrator username": cfg.AdminUsername,
		"administrator password": cfg.AdminPassword,
	} {
		if value == "" {
			return fmt.Errorf("PostgreSQL %s must not be empty", name)
		}
	}

	source, err := newConnectionStringSource(cfg.EndpointSource)
	if err != nil {
		return err
	}
	dsn, err := source()
	if err != nil {
		return err
	}
	ownerPoolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return errors.New("parsing PostgreSQL bootstrap connection string: invalid value")
	}
	connConfig := ownerPoolConfig.ConnConfig
	readWriteSource, err := newConnectionStringSource(cfg.ReadWriteSource)
	if err != nil {
		return err
	}
	readWriteDSN, err := readWriteSource()
	if err != nil {
		return err
	}
	readWritePoolConfig, err := pgxpool.ParseConfig(readWriteDSN)
	if err != nil {
		return errors.New("parsing PostgreSQL read/write connection string: invalid value")
	}
	readWriteConfig := readWritePoolConfig.ConnConfig
	if connConfig.User != OwnerUserName {
		return fmt.Errorf("PostgreSQL owner connection string must contain the %q user", OwnerUserName)
	}
	if readWriteConfig.User != ReadWriteUserName {
		return fmt.Errorf("PostgreSQL read/write connection string must contain the %q user", ReadWriteUserName)
	}
	if connConfig.Password != defaultOwnerPassword || readWriteConfig.Password != defaultReadWritePassword {
		return errors.New("PostgreSQL bootstrap connection strings do not match the fixed development passwords")
	}
	if connConfig.Host != readWriteConfig.Host || connConfig.Port != readWriteConfig.Port || connConfig.Database != readWriteConfig.Database {
		return errors.New("PostgreSQL bootstrap connection strings must target the same database")
	}
	connConfig.User = cfg.AdminUsername
	connConfig.Password = cfg.AdminPassword
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return fmt.Errorf("connecting as PostgreSQL administrator: %w", err)
	}
	defer conn.Close(ctx) //nolint:errcheck // The transaction result decides success.

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("starting PostgreSQL bootstrap transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // Commit or the returned error decides the outcome.

	for setting, value := range map[string]string{
		"substrate.bootstrap_owner_username":     OwnerUserName,
		"substrate.bootstrap_owner_password":     defaultOwnerPassword,
		"substrate.bootstrap_owner_role":         OwnerRoleName,
		"substrate.bootstrap_readwrite_username": ReadWriteUserName,
		"substrate.bootstrap_readwrite_password": defaultReadWritePassword,
		"substrate.bootstrap_readwrite_role":     ReadWriteRoleName,
		"substrate.bootstrap_schema":             cfg.Schema,
	} {
		if _, err := tx.Exec(ctx, `SELECT set_config($1, $2, true)`, setting, value); err != nil {
			return fmt.Errorf("set PostgreSQL bootstrap parameter %q: %w", setting, err)
		}
	}
	if _, err := tx.Exec(ctx, identitySQL); err != nil {
		return fmt.Errorf("apply PostgreSQL identity SQL: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing PostgreSQL bootstrap: %w", err)
	}
	return nil
}
