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

package resources

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestToGRPCStatusError(t *testing.T) {
	err := ToGRPCStatusError(field.ErrorList{field.Required(field.NewPath("actor_name"), "")})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", got)
	}
	if !strings.Contains(status.Convert(err).Message(), "actor_name") {
		t.Errorf("message %q does not name the field", status.Convert(err).Message())
	}
}

func TestToAPIError(t *testing.T) {
	err := ToAPIError(field.ErrorList{field.Required(field.NewPath("actor_name"), "")})
	if got := apierror.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", got)
	}
	if !strings.Contains(err.Error(), "actor_name") {
		t.Errorf("message %q does not name the field", err.Error())
	}
}

func TestDeepEqual(t *testing.T) {
	// Proto messages carry internal state that reflect.DeepEqual would
	// compare; proto.Equal compares only field values.
	a := &ateapipb.ObjectRef{Atespace: "a", Name: "x"}
	b := &ateapipb.ObjectRef{Atespace: "a", Name: "x"}
	_ = a.String() // populates a's internal state, not b's

	tests := []struct {
		name string
		got  bool
		want bool
	}{
		{name: "equal protos", got: DeepEqual(a, b), want: true},
		{name: "different protos", got: DeepEqual(a, &ateapipb.ObjectRef{Atespace: "a", Name: "y"}), want: false},
		{name: "nil protos", got: DeepEqual[*ateapipb.ObjectRef](nil, nil), want: true},
		{name: "equal non-protos", got: DeepEqual([]string{"a"}, []string{"a"}), want: true},
		{name: "different non-protos", got: DeepEqual(1, 2), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("DeepEqual() = %v, want %v", tt.got, tt.want)
			}
		})
	}
}

func TestIsValidResourceName(t *testing.T) {
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{"valid lowercase", "my-actor-1", true},
		{"valid single char", "a", true},
		{"missing name", "", false},
		{"invalid uppercase", "My-Actor", false},
		{"invalid start hyphen", "-actor", false},
		{"valid start number", "1actor", true},
		{"invalid end hyphen", "actor-", false},
		{"invalid special chars", "actor@1", false},
		{"invalid length", strings.Repeat("a", 64), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidResourceName(tt.value); got != tt.valid {
				t.Errorf("IsValidResourceName(%q) = %v, want %v", tt.value, got, tt.valid)
			}
		})
	}
}

func TestValidateWorkerPodUID(t *testing.T) {
	tests := []struct {
		name    string
		uid     string
		wantErr bool
	}{
		{"uuid valid", "422938ba-8860-4983-a25d-d6bcb0a69d4e", false},
		{"separator", "a/b", true},
		{"traversal", "..", true},
		{"empty", "", true},
		{"uppercase", "Pod-UID", true},
		{"too long", strings.Repeat("a", 64), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateWorkerPodUID(tt.uid); (err != nil) != tt.wantErr {
				t.Errorf("ValidateWorkerPodUID(%q) err = %v, wantErr %v", tt.uid, err, tt.wantErr)
			}
		})
	}
}

func TestValidateRunscHash(t *testing.T) {
	const valid = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	tests := []struct {
		name    string
		hash    string
		wantErr bool
	}{
		{"valid lowercase", valid, false},
		{"valid uppercase", strings.ToUpper(valid), false},
		{"empty", "", true},
		{"too short", "abc123", true},
		{"too long", valid + "00", true},
		{"separator", strings.Repeat("a", 60) + "/../", true},
		{"non-hex", strings.Repeat("g", 64), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateRunscHash(tt.hash); (err != nil) != tt.wantErr {
				t.Errorf("ValidateRunscHash(%q) err = %v, wantErr %v", tt.hash, err, tt.wantErr)
			}
		})
	}
}

func TestValidateSnapshotLocation(t *testing.T) {
	tests := []struct {
		name    string
		prefix  string
		wantErr bool
	}{
		{"valid with trailing slash", "gs://bucket/actors/1234/snapshots/5678/", false},
		{"valid without path", "gs://bucket", false},
		// Scheme is storage-backend policy, not validated here.
		{"valid alternate scheme", "s3://bucket/path", false},
		{"empty", "", true},
		{"missing bucket", "gs://", true},
		{"no scheme or bucket", "bucket/path", true},
		{"unparseable", "://bucket", true},
		// Appended object names must not be swallowed by URL components.
		{"query", "gs://bucket/path?x=1", true},
		{"fragment", "gs://bucket/path#frag", true},
		{"userinfo", "gs://user@bucket/path", true},
		// Opaque form (no //) parses with an empty host, so it is rejected
		// on either the bucket or the opaque check.
		{"opaque", "gs:bucket/path", true},
		{"dot dot", "gs://bucket/team-a/../team-b", true},
		{"trailing dot dot", "gs://bucket/path/..", true},
		{"escaped dot dot", "gs://bucket/%2e%2e/path", true},
		{"dot", "gs://bucket/./path", true},
		{"triple dot", "gs://bucket/.../path", true},
		{"space padded dot dot", "gs://bucket/a/%20..%20/b", true},
		{"trailing space dot dot", "gs://bucket/a/..%20/b", true},
		{"backslash", `gs://bucket/team-a\..\team-b`, true},
		{"escaped backslash", "gs://bucket/a%5Cb", true},
		{"dots in a name", "gs://bucket/a..b/.c/d.", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateSnapshotLocation(tt.prefix); (err != nil) != tt.wantErr {
				t.Errorf("ValidateSnapshotLocation(%q) err = %v, wantErr %v", tt.prefix, err, tt.wantErr)
			}
		})
	}
}

func TestValidateIP(t *testing.T) {
	tests := []struct {
		name    string
		ip      string
		wantMsg string // empty means valid
	}{
		{"valid ipv4", "192.168.1.1", ""},
		{"valid ipv6", "2001:db8::1", ""},
		{"invalid format", "not-an-ip", "must be a valid IP address"},
		{"ipv4-mapped ipv6", "::ffff:192.168.1.1", "must not be an IPv4-mapped IPv6 address"},
		{"non-canonical ipv6", "2001:db8:0:0:0:0:0:1", "must be in canonical form"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidateIP(tt.ip, field.NewPath("ip"))
			if tt.wantMsg == "" {
				if len(errs) > 0 {
					t.Fatalf("expected 0 errors, got %v", errs)
				}
			} else {
				if len(errs) == 0 {
					t.Fatalf("expected error matching %q, got 0", tt.wantMsg)
				}
				err := errs[0]
				got := err.Error()
				if matched, matchErr := regexp.MatchString(tt.wantMsg, got); matchErr != nil {
					t.Fatalf("failed to compile regex %q: %v", tt.wantMsg, matchErr)
				} else if !matched {
					t.Errorf("expected message matching %q, got %q", tt.wantMsg, got)
				}
			}
		})
	}
}

func TestValidateUUID(t *testing.T) {
	tests := []struct {
		name    string
		uuid    string
		wantMsg string // empty means valid
	}{
		{"valid", "123e4567-e89b-12d3-a456-426614174000", ""},
		{"too short", "123e4567", "must be a lowercase UUID"},
		{"too long", "123e4567-e89b-12d3-a456-4266141740001", "must be a lowercase UUID"},
		{"missing dashes", "123e4567e89b12d3a456426614174000", "must be a lowercase UUID"},
		{"uppercase hex", "123E4567-E89B-12D3-A456-426614174000", "must be a lowercase UUID"},
		{"invalid characters", "123e4567-e89b-12d3-a456-42661417400g", "must be a lowercase UUID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidateUUID(tt.uuid, field.NewPath("uuid"))
			if tt.wantMsg == "" {
				if len(errs) > 0 {
					t.Fatalf("expected 0 errors, got %v", errs)
				}
			} else {
				if len(errs) == 0 {
					t.Fatalf("expected error matching %q, got 0", tt.wantMsg)
				}
				err := errs[0]
				got := err.Error()
				if matched, matchErr := regexp.MatchString(tt.wantMsg, got); matchErr != nil {
					t.Fatalf("failed to compile regex %q: %v", tt.wantMsg, matchErr)
				} else if !matched {
					t.Errorf("expected message matching %q, got %q", tt.wantMsg, got)
				}
			}
		})
	}
}

func TestValidateLimit(t *testing.T) {
	path := field.NewPath("limits").Index(0)
	quantityPath := path.Child("quantity")
	tests := []struct {
		name     string
		limit    string
		quantity string
		want     field.ErrorList
	}{
		{name: "cpu below the bound", limit: "cpu", quantity: "999"},
		{name: "cpu at the millicore bound", limit: "cpu", quantity: "999.999"},
		{name: "memory", limit: "memory", quantity: "1Gi"},
		{name: "memory at the int64 bound", limit: "memory", quantity: "9223372036854775807"},
		{name: "missing quantity left to tags", limit: "cpu"},
		{name: "unsupported name", limit: "gpu", quantity: "1", want: field.ErrorList{field.NotSupported[string](path.Child("name"), nil, nil)}},
		{name: "malformed quantity", limit: "cpu", quantity: "x", want: field.ErrorList{field.Invalid(quantityPath, nil, "")}},
		{name: "zero quantity", limit: "memory", quantity: "0", want: field.ErrorList{field.Invalid(quantityPath, nil, "")}},
		{name: "negative quantity", limit: "memory", quantity: "-1", want: field.ErrorList{field.Invalid(quantityPath, nil, "")}},
		{name: "cpu at 1000 cores", limit: "cpu", quantity: "1000", want: field.ErrorList{field.Invalid(quantityPath, nil, "")}},
		{name: "cpu rounds up to 1000 cores in millicores", limit: "cpu", quantity: "999.9995", want: field.ErrorList{field.Invalid(quantityPath, nil, "")}},
		{name: "cpu far beyond int64 millicores", limit: "cpu", quantity: "1e30", want: field.ErrorList{field.Invalid(quantityPath, nil, "")}},
		{name: "memory past the int64 bound", limit: "memory", quantity: "9223372036854775808", want: field.ErrorList{field.Invalid(quantityPath, nil, "")}},
		{name: "memory rounds up past the int64 bound", limit: "memory", quantity: "9223372036854775807.5", want: field.ErrorList{field.Invalid(quantityPath, nil, "")}},
		{name: "memory far beyond int64 bytes", limit: "memory", quantity: "1e30", want: field.ErrorList{field.Invalid(quantityPath, nil, "")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field.ErrorMatcher{}.ByType().ByField().Test(t, tt.want, ValidateLimit(path, tt.limit, tt.quantity))
		})
	}
}

func testActorDirs() *ateompb.ActorDirs {
	return &ateompb.ActorDirs{
		RootDir:                   "/node/actors/a",
		OciBundleDir:              "/node/actors/a/bundles",
		CheckpointDir:             "/node/actors/a/checkpoint-state",
		RestoreDir:                "/node/actors/a/restore-state",
		DurableDirVolumeMountsDir: "/node/actors/a/durable-dir",
		SystemInfoVolumeRootsDir:  "/node/actors/a/system-info",
		VolumesDir:                "/node/actors/a/volumes",
	}
}

func TestValidateSnapshotFidelity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fidelity ateompb.SnapshotFidelity
		wantErr  bool
	}{
		{"volumes", ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, false},
		{"memory", ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, false},
		{"unspecified", ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, true},
		{"rootfs not supported yet", ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, true},
		{"outside the enum", ateompb.SnapshotFidelity(99), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			errs := ValidateSnapshotFidelity(tc.fidelity, field.NewPath("fidelity"))
			if gotErr := len(errs) != 0; gotErr != tc.wantErr {
				t.Fatalf("ValidateSnapshotFidelity(%v) = %v, wantErr %t", tc.fidelity, errs, tc.wantErr)
			}
			for _, e := range errs {
				if e.Field != "fidelity" {
					t.Errorf("error names field %q, want %q", e.Field, "fidelity")
				}
			}
		})
	}
}

func TestValidateActorDirs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ateompb.ActorDirs) *ateompb.ActorDirs
		// wantField is the field the one expected error names; empty means valid.
		wantField string
	}{
		{"valid", func(actorDirs *ateompb.ActorDirs) *ateompb.ActorDirs { return actorDirs }, ""},
		{"nil", func(*ateompb.ActorDirs) *ateompb.ActorDirs { return nil }, "actor_dirs"},
		{"missing dir", func(actorDirs *ateompb.ActorDirs) *ateompb.ActorDirs { actorDirs.RestoreDir = ""; return actorDirs }, "actor_dirs.restore_dir"},
		{"relative dir", func(actorDirs *ateompb.ActorDirs) *ateompb.ActorDirs {
			actorDirs.OciBundleDir = "bundles"
			return actorDirs
		}, "actor_dirs.oci_bundle_dir"},
		{"unclean dir", func(actorDirs *ateompb.ActorDirs) *ateompb.ActorDirs {
			actorDirs.CheckpointDir = "/node/actors/a/../b/checkpoint-state"
			return actorDirs
		}, "actor_dirs.checkpoint_dir"},
		{"relative root", func(actorDirs *ateompb.ActorDirs) *ateompb.ActorDirs {
			actorDirs.RootDir = "node/actors/a"
			return actorDirs
		}, "actor_dirs.root_dir"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			errs := ValidateActorDirs(tc.mutate(testActorDirs()), field.NewPath("actor_dirs"))
			if tc.wantField == "" {
				if len(errs) != 0 {
					t.Fatalf("ValidateActorDirs() = %v, want no errors", errs)
				}
				return
			}
			if len(errs) != 1 || errs[0].Field != tc.wantField {
				t.Fatalf("ValidateActorDirs() = %v, want one error on %s", errs, tc.wantField)
			}
		})
	}
}

const testPinnedImage = "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// stringRuleCase is one input to a single-string rule and whether it fails.
type stringRuleCase struct {
	name    string
	value   string
	wantErr bool
}

func runStringRule(t *testing.T, rule func(*field.Path, string) field.ErrorList, tests []stringRuleCase) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := rule(field.NewPath("f"), tt.value)
			if (len(errs) > 0) != tt.wantErr {
				t.Errorf("value %q: errs = %v, wantErr %v", tt.value, errs, tt.wantErr)
			}
		})
	}
}

func TestValidateRuntimeAssetPath(t *testing.T) {
	// on macOS the temp dir is behind the /var symlink
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	asset := filepath.Join(root, "runsc-abc")
	if err := os.WriteFile(asset, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "gvisor-abc"), 0o755); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "gvisor-abc", "runsc")
	if err := os.WriteFile(nested, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "evil")
	if err := os.WriteFile(outside, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "runsc-link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	linkedDir := filepath.Join(root, "gvisor-link")
	if err := os.Symlink(filepath.Dir(outside), linkedDir); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"asset", asset, false},
		{"release dir asset", nested, false},
		{"empty", "", true},
		{"relative", "runsc-abc", true},
		{"unclean", root + "/gvisor-abc/../runsc-abc", true},
		{"outside root", outside, true},
		{"root itself", root, true},
		{"directory", filepath.Join(root, "gvisor-abc"), true},
		{"sibling with root prefix", root + "-other/runsc", true},
		{"host binary", "/bin/sh", true},
		{"symlink out of root", link, true},
		{"symlinked directory", filepath.Join(linkedDir, "evil"), true},
		{"missing", filepath.Join(root, "runsc-missing"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			errs := ValidateRuntimeAssetPath(root, tc.path, field.NewPath("runsc_path"))
			if gotErr := len(errs) > 0; gotErr != tc.wantErr {
				t.Errorf("ValidateRuntimeAssetPath(%q) = %v, want error: %v", tc.path, errs, tc.wantErr)
			}
		})
	}
}

func TestValidatePinnedImage(t *testing.T) {
	runStringRule(t, ValidatePinnedImage, []stringRuleCase{
		{"empty is left to tags", "", false},
		{"pinned", testPinnedImage, false},
		{"tag only", "example.com/app:v1", true},
		{"malformed digest", "example.com/app@sha256:abc", true},
		{"malformed reference", "Example.com/App", true},
	})
}

func TestValidateMountPath(t *testing.T) {
	runStringRule(t, ValidateMountPath, []stringRuleCase{
		{"empty is left to tags", "", false},
		{"clean absolute", "/data/cache", false},
		{"relative", "data", true},
		{"root", "/", true},
		{"trailing slash", "/data/", true},
		{"double slash", "/data//x", true},
		{"colon", "/data:x", true},
		{"dot segment", "/data/./x", true},
		{"dot-dot segment", "/data/../x", true},
		{"control character", "/data\x01", true},
	})
}

func TestValidateHTTPGetPath(t *testing.T) {
	runStringRule(t, ValidateHTTPGetPath, []stringRuleCase{
		{"empty is left to tags", "", false},
		{"simple", "/healthz", false},
		{"percent escape", "/a%20b", false},
		{"no leading slash", "healthz", true},
		{"query", "/healthz?x=1", true},
		{"fragment", "/healthz#x", true},
		{"bad percent escape", "/a%2", true},
	})
}

func TestValidateEnvVarName(t *testing.T) {
	runStringRule(t, ValidateEnvVarName, []stringRuleCase{
		{"empty is left to tags", "", false},
		{"conventional", "HOME_DIR", false},
		{"printable punctuation", "a.b-c", false},
		{"equals sign", "A=B", true},
		{"control character", "A\tB", true},
		{"non-ASCII", "é", true},
	})
}

func TestValidateProjectedPath(t *testing.T) {
	runStringRule(t, ValidateProjectedPath, []stringRuleCase{
		{"empty is left to tags", "", false},
		{"relative file", "identity/name", false},
		{"absolute", "/etc/name", true},
		{"escape", "../name", true},
	})
}

func TestValidateStorageVolumeID(t *testing.T) {
	runStringRule(t, ValidateStorageVolumeID, []stringRuleCase{
		{"empty", "", false},
		{"plain", "projects/p/disks/d-1", false},
		{"newline is allowed", "a\nb", false},
		{"NUL", "a\x00b", true},
		{"DEL", "a\x7fb", true},
		{"C1 control", "a\u0085b", true},
	})
}

func TestValidateVolumeType(t *testing.T) {
	runStringRule(t, ValidateVolumeType, []stringRuleCase{
		{"empty", "", false},
		{"dns subdomain", "pd.csi.storage.gke.io", false},
		{"substrate prefix", "substrate.io/gcs", false},
		{"uppercase", "PD", true},
		{"other prefix", "example.io/gcs", true},
	})
}

func TestValidateCapabilities(t *testing.T) {
	tests := []struct {
		name     string
		caps     []string
		allowAll bool
		wantErrs int
	}{
		{"valid names", []string{"NET_BIND_SERVICE", "SYS_PTRACE"}, false, 0},
		{"ALL rejected for add", []string{"ALL"}, false, 1},
		{"ALL accepted for drop", []string{"ALL"}, true, 0},
		{"CAP_ prefix", []string{"CAP_NET_ADMIN"}, false, 1},
		{"lowercase", []string{"net_admin"}, false, 1},
		{"too long", []string{strings.Repeat("A", 64)}, false, 1},
		{"each bad entry reported", []string{"CAP_X", "ok", "NET_ADMIN"}, false, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if errs := ValidateCapabilities(field.NewPath("add"), tt.caps, tt.allowAll); len(errs) != tt.wantErrs {
				t.Errorf("errs = %v, want %d", errs, tt.wantErrs)
			}
		})
	}
}

func TestValidateNestedMountPaths(t *testing.T) {
	path := field.NewPath("volume_mounts")
	tests := []struct {
		name  string
		paths []string
		want  field.ErrorList
	}{
		{name: "siblings", paths: []string{"/a", "/b", "/ab"}},
		{name: "identical paths are left to the list key", paths: []string{"/a", "/a"}},
		{name: "empty paths are skipped", paths: []string{"", "/a"}},
		{
			name:  "nested under an earlier mount",
			paths: []string{"/a", "/a/b"},
			want:  field.ErrorList{field.Invalid(path.Index(1).Child("mount_path"), nil, "")},
		},
		{
			name:  "nested over an earlier mount",
			paths: []string{"/a/b", "/a"},
			want:  field.ErrorList{field.Invalid(path.Index(1).Child("mount_path"), nil, "")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field.ErrorMatcher{}.ByType().ByField().Test(t, tt.want, ValidateNestedMountPaths(path, tt.paths))
		})
	}
}

func TestValidateCSIMap(t *testing.T) {
	path := field.NewPath("publish_context")
	many := make(map[string]string, 100)
	for i := range 100 {
		many[fmt.Sprintf("key-%03d", i)] = "v"
	}
	tests := []struct {
		name string
		m    map[string]string
		want field.ErrorList
	}{
		{name: "nil"},
		{name: "one long value", m: map[string]string{"token": strings.Repeat("x", 4000)}},
		{name: "many small entries", m: many},
		{name: "exactly the limit", m: map[string]string{"k": strings.Repeat("x", 4095)}},
		{name: "one byte over", m: map[string]string{"k": strings.Repeat("x", 4096)}, want: field.ErrorList{field.TooLong(path, nil, 4096)}},
		{name: "over across entries", m: map[string]string{"a": strings.Repeat("x", 2048), "b": strings.Repeat("y", 2048)}, want: field.ErrorList{field.TooLong(path, nil, 4096)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field.ErrorMatcher{}.ByType().ByField().Test(t, tt.want, ValidateCSIMap(path, tt.m))
		})
	}
}

func TestValidateHostPort(t *testing.T) {
	runStringRule(t, ValidateHostPort, []stringRuleCase{
		{name: "empty is left to presence tags", value: ""},
		{name: "dns name", value: "atenet-egress.ate-system.svc:443"},
		{name: "fully qualified dns name", value: "atenet-egress.ate-system.svc.cluster.local.:443"},
		{name: "ipv4", value: "10.0.0.1:8080"},
		{name: "ipv6", value: "[fd00::1]:443"},
		{name: "missing port", value: "atenet-egress.ate-system.svc", wantErr: true},
		{name: "empty host", value: ":443", wantErr: true},
		{name: "bare dot host", value: ".:443", wantErr: true},
		{name: "uppercase host", value: "Egress.Example:443", wantErr: true},
		{name: "host with a path", value: "egress/../x:443", wantErr: true},
		{name: "port zero", value: "egress.example:0", wantErr: true},
		{name: "port too large", value: "egress.example:65536", wantErr: true},
		{name: "named port", value: "egress.example:https", wantErr: true},
		{name: "unbracketed ipv6", value: "fd00::1:443", wantErr: true},
	})
}
