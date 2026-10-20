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

package credplugin

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	clientauthv1beta1 "k8s.io/client-go/pkg/apis/clientauthentication/v1beta1"
)

func TestParseInfo(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     string
		want    Info
		wantErr bool
	}{
		{name: "unset", want: Info{APIVersion: APIVersionV1}},
		{
			name: "v1 with audience",
			env:  `{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","spec":{"interactive":true,"cluster":{"server":"https://x","config":{"audience":"api.ate-system.svc"}}}}`,
			want: Info{APIVersion: APIVersionV1, Interactive: true, Audience: "api.ate-system.svc"},
		},
		{
			name: "v1beta1 without cluster",
			env:  `{"apiVersion":"client.authentication.k8s.io/v1beta1","kind":"ExecCredential","spec":{}}`,
			want: Info{APIVersion: APIVersionV1beta1},
		},
		{name: "v1alpha1", env: `{"apiVersion":"client.authentication.k8s.io/v1alpha1","kind":"ExecCredential"}`, wantErr: true},
		{name: "malformed", env: `{`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseInfo(tc.env)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseInfo err = %v, wantErr %t", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("ParseInfo = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestWriteCredential(t *testing.T) {
	expiry := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	if err := WriteCredential(&buf, APIVersionV1beta1, "tok", expiry); err != nil {
		t.Fatal(err)
	}
	var got clientauthv1beta1.ExecCredential
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.APIVersion != APIVersionV1beta1 || got.Kind != "ExecCredential" {
		t.Errorf("apiVersion/kind = %s/%s", got.APIVersion, got.Kind)
	}
	if got.Status.Token != "tok" || !got.Status.ExpirationTimestamp.Time.Equal(expiry) {
		t.Errorf("status = %+v", got.Status)
	}

	buf.Reset()
	if err := WriteCredential(&buf, APIVersionV1, "tok", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(buf.Bytes(), []byte("expirationTimestamp")) {
		t.Errorf("credential without expiry = %s, want no expirationTimestamp", buf.String())
	}
}

func jwt(payload string) string {
	return "e30." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".sig"
}

func TestJWTExpiry(t *testing.T) {
	if got, err := JWTExpiry(jwt(`{"exp":1760616000}`)); err != nil || got.Unix() != 1760616000 {
		t.Errorf("JWTExpiry = (%v, %v), want 1760616000", got, err)
	}
	if got, err := JWTExpiry(jwt(`{"sub":"a"}`)); err != nil || !got.IsZero() {
		t.Errorf("JWTExpiry without exp = (%v, %v), want zero", got, err)
	}
	if _, err := JWTExpiry("opaque-access-token"); err == nil {
		t.Error("JWTExpiry(opaque token) succeeded, want error")
	}
}

func TestCache(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	c := &Cache{Dir: filepath.Join(t.TempDir(), "cache"), Now: func() time.Time { return now }}
	key := []string{"https://kube", "ctx", "ate-system/ate-client", "api.ate-system.svc"}

	if _, _, ok := c.Get(key, time.Minute); ok {
		t.Fatal("Get on an empty cache hit")
	}
	if err := c.Put(key, "tok", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if tok, _, ok := c.Get(key, time.Minute); !ok || tok != "tok" {
		t.Errorf("Get = (%q, %t), want tok", tok, ok)
	}
	if _, _, ok := c.Get([]string{"https://kube", "ctx", "ate-system/ate-client", "other"}, time.Minute); ok {
		t.Error("Get for another audience hit")
	}
	if _, _, ok := c.Get(key, 2*time.Hour); ok {
		t.Error("Get hit for a token expiring within minRemaining")
	}

	dirInfo, err := os.Stat(c.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if mode := dirInfo.Mode().Perm(); mode != 0o700 {
		t.Errorf("cache dir mode = %o, want 700", mode)
	}
	fileInfo, err := os.Stat(c.path(key))
	if err != nil {
		t.Fatal(err)
	}
	if mode := fileInfo.Mode().Perm(); mode != 0o600 {
		t.Errorf("cache file mode = %o, want 600", mode)
	}
}
