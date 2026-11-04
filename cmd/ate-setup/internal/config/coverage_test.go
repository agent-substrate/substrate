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

package config

import (
	"maps"
	"reflect"
	"slices"
	"strconv"
	"testing"
)

// notFromASetting are Config fields no setting feeds: derived values, the
// repository root, and the companions that record whether a setting was
// supplied rather than holding a value of their own.
var notFromASetting = map[string]bool{
	"Root":                     true,
	"Images":                   true, // assembled from images.repo and images.tag
	"CloudSQL":                 true, // assembled from the ateapi.postgres.cloudsql.* settings
	"PostgresOwnerRoleSet":     true,
	"PostgresReadWriteRoleSet": true,

	// kindCluster.enabled feeds Kind, but the fixture sets it false to reach
	// the cloud path, so its zero value here says nothing.
	"Kind": true,
}

// A Config field that no setting feeds stays at its zero value however the
// install is configured, and nothing says so: the build succeeds and the
// value is silently dropped. That is what an upstream merge adding a field
// looks like, so every field is required to be reachable from the registry.
func TestEveryConfigFieldIsFedBySomeSetting(t *testing.T) {
	// Give every setting a value of its kind, so a populated field cannot be
	// zero by coincidence.
	env := map[string]string{}
	for _, s := range All() {
		switch s.Kind {
		case KindBool:
			// A Kind install clears the GCP coordinates by design, so the
			// cloud path is the one that exercises every field.
			env[s.Env] = strconv.FormatBool(s.Key != "kindCluster.enabled")
		case KindInt:
			env[s.Env] = "7"
		case KindDuration:
			env[s.Env] = "7m0s"
		case KindPathList:
			env[s.Env] = writeComponent(t, t.TempDir(), "Component")
		default:
			env[s.Env] = valueFor(s.Key)
		}
	}

	// The microVM asset location applies only to the external backend, which
	// refuses the fake data plane, so a second pass covers it.
	delete(env, "MICROVM_ASSET_LOCATION")
	external := maps.Clone(env)
	external["STORAGE_BACKEND"] = StorageBackendExternal
	external["ATE_INSTALL_FAKE_DATA_PLANE"] = "false"
	external["MICROVM_ASSET_LOCATION"] = "az://acct/assets"

	var passes []reflect.Value
	for _, env := range []map[string]string{env, external} {
		r, err := Resolve(nil, ResolveOptions{Env: env})
		if err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		cfg, err := buildConfig(t.TempDir(), env, r)
		if err != nil {
			t.Fatalf("buildConfig() error = %v", err)
		}
		passes = append(passes, reflect.ValueOf(*cfg))
	}

	typ := passes[0].Type()
	for i := range typ.NumField() {
		name := typ.Field(i).Name
		if !typ.Field(i).IsExported() || notFromASetting[name] {
			continue
		}
		t.Run(name, func(t *testing.T) {
			if !slices.ContainsFunc(passes, func(v reflect.Value) bool { return !v.Field(i).IsZero() }) {
				t.Errorf("Config.%s is zero with every setting supplied; "+
					"no registry entry feeds it, so it can never be configured", name)
			}
		})
	}
}

// valueFor is a value the setting will accept. A few settings validate their
// contents, so a generic string will not do.
func valueFor(key string) string {
	switch key {
	case "atenet.dataplane":
		return RouterEnvoy
	case "storage.backend":
		return StorageBackendGCS
	case "clusterSize":
		return ClusterSizeSize0
	case "ingressAllowedSPIFFEIDs", "ateapi.authz.egressGatewayExtraSPIFFEIDs":
		return "spiffe://cluster.local/ns/a/sa/b"
	case "ateapi.postgres.cloudsql.ipType":
		return CloudSQLIPTypePSC
	case "atenet.egress.additionalExtprocService":
		return "ns/svc:8080"
	case "atenet.egress.credentialProvider":
		return `{"name":"k8s.io"}`
	case "actorJWT.algorithm":
		return "ES256"
	case "actorJWT.issuer":
		return "https://idp.example.com"
	case "ateapi.logLevel":
		return "debug"
	case "csi.setup":
		return "none"
	case "preview":
		return "ALL"
	}
	return "set"
}
