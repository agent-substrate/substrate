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

// Package credplugin implements the plugin side of the kubeconfig exec
// credential protocol for `kubectl ate credential`.
package credplugin

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientauthv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
)

const (
	// InfoEnv carries the ExecCredential a client passes to its plugin.
	InfoEnv = "KUBERNETES_EXEC_INFO"

	APIVersionV1      = "client.authentication.k8s.io/v1"
	APIVersionV1beta1 = "client.authentication.k8s.io/v1beta1"
)

// Info is what the client told the plugin through InfoEnv.
type Info struct {
	// APIVersion is the version the response must use.
	APIVersion  string
	Interactive bool
	// Audience is spec.cluster.config.audience, if the client sent cluster
	// info.
	Audience string
}

// ParseInfo parses the value of InfoEnv. An empty value, as when the plugin
// is run by hand, yields APIVersionV1 and no audience.
func ParseInfo(env string) (Info, error) {
	if env == "" {
		return Info{APIVersion: APIVersionV1}, nil
	}
	// v1 and v1beta1 share the fields read here.
	var cred clientauthv1.ExecCredential
	if err := json.Unmarshal([]byte(env), &cred); err != nil {
		return Info{}, fmt.Errorf("parse %s: %w", InfoEnv, err)
	}
	if cred.APIVersion != APIVersionV1 && cred.APIVersion != APIVersionV1beta1 {
		return Info{}, fmt.Errorf("%s has unsupported apiVersion %q", InfoEnv, cred.APIVersion)
	}
	info := Info{APIVersion: cred.APIVersion, Interactive: cred.Spec.Interactive}
	if c := cred.Spec.Cluster; c != nil && len(c.Config.Raw) > 0 {
		var config struct {
			Audience string `json:"audience"`
		}
		if err := json.Unmarshal(c.Config.Raw, &config); err != nil {
			return Info{}, fmt.Errorf("parse %s spec.cluster.config: %w", InfoEnv, err)
		}
		info.Audience = config.Audience
	}
	return info, nil
}

// WriteCredential writes an ExecCredential carrying token. A zero expiry is
// left out, telling the client to use the token until it is rejected.
func WriteCredential(w io.Writer, apiVersion, token string, expiry time.Time) error {
	cred := clientauthv1.ExecCredential{
		TypeMeta: metav1.TypeMeta{APIVersion: apiVersion, Kind: "ExecCredential"},
		Status:   &clientauthv1.ExecCredentialStatus{Token: token},
	}
	if !expiry.IsZero() {
		exp := metav1.NewTime(expiry)
		cred.Status.ExpirationTimestamp = &exp
	}
	return json.NewEncoder(w).Encode(cred)
}

// JWTExpiry returns the exp claim of a JWT without verifying it, or the zero
// time if it has none.
func JWTExpiry(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, errors.New("not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("decode JWT payload: %w", err)
	}
	var claims struct {
		Exp *json.Number `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, fmt.Errorf("parse JWT payload: %w", err)
	}
	if claims.Exp == nil {
		return time.Time{}, nil
	}
	exp, err := claims.Exp.Float64()
	if err != nil {
		return time.Time{}, fmt.Errorf("parse JWT exp: %w", err)
	}
	return time.Unix(int64(exp), 0), nil
}
