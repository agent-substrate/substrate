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

import "github.com/agent-substrate/substrate/internal/env"

var postgresConnectionStringEnv = env.Var[string]{
	Name:    "ATE_API_POSTGRES_CONNECTION_STRING",
	Default: "",
	Description: `PostgreSQL connection string in DSN or URI form. Read only when --postgres-connection-string=@env;
otherwise the flag supplies the value. Unset or empty values are rejected when @env is selected.`,
}

var postgresSchemaEnv = env.Var[string]{
	Name:    "ATE_API_POSTGRES_SCHEMA",
	Default: "",
	Description: `PostgreSQL schema name. Read only when --postgres-schema=@env; otherwise the flag supplies
the value and defaults to public. Unset or empty values are rejected when @env is selected.`,
}
