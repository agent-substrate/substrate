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

// Package postgressetup provides the PostgreSQL identity setup shared by
// Substrate installers.
package postgressetup

import _ "embed"

const (
	Schema            = "substrate"
	OwnerRole         = "substrate_owner"
	OwnerUser         = "substrate_owner_user"
	OwnerPassword     = "substrate-owner"
	ReadWriteRole     = "substrate_readwrite"
	ReadWriteUser     = "substrate_readwrite_user"
	ReadWritePassword = "substrate-readwrite"
)

// Config names the schema and identities created by SQL.
type Config struct {
	Schema            string
	OwnerRole         string
	OwnerUser         string
	OwnerPassword     string
	ReadWriteRole     string
	ReadWriteUser     string
	ReadWritePassword string
}

// DefaultConfig returns the identities used by bundled development installs.
func DefaultConfig() Config {
	return Config{
		Schema:            Schema,
		OwnerRole:         OwnerRole,
		OwnerUser:         OwnerUser,
		OwnerPassword:     OwnerPassword,
		ReadWriteRole:     ReadWriteRole,
		ReadWriteUser:     ReadWriteUser,
		ReadWritePassword: ReadWritePassword,
	}
}

// PSQLArgs returns the psql variable arguments required by SQL.
func (c Config) PSQLArgs() []string {
	return []string{
		"--set=substrate_schema=" + c.Schema,
		"--set=substrate_owner_role=" + c.OwnerRole,
		"--set=substrate_owner_user=" + c.OwnerUser,
		"--set=substrate_owner_password=" + c.OwnerPassword,
		"--set=substrate_readwrite_role=" + c.ReadWriteRole,
		"--set=substrate_readwrite_user=" + c.ReadWriteUser,
		"--set=substrate_readwrite_password=" + c.ReadWritePassword,
	}
}

//go:embed setup.sql
var sql string

// SQL returns the parameterized psql script that creates Substrate's roles,
// users, and schema. The caller supplies Config.PSQLArgs and owns the
// surrounding transaction.
func SQL() string { return sql }

// Script returns the psql script as a complete transaction for direct execution.
func Script() string { return "BEGIN;\n" + sql + "\nCOMMIT;\n" }
