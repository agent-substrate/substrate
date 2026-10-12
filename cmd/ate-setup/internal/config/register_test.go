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
	"strings"
	"testing"

	"github.com/spf13/pflag"
)

func testSetting(key, env, flag string, commands ...string) Setting {
	return Setting{
		Key: key, Env: env, Flag: flag, Kind: KindString,
		Commands: commands, Usage: "test setting",
	}
}

func TestRegisterCommandExtendsAll(t *testing.T) {
	before := len(All())
	useSettings(t, testSetting("test.scoped", "ATE_TEST_SCOPED", "test-scoped", "deploy thing"))

	if got := len(All()); got != before+1 {
		t.Errorf("len(All()) = %d, want %d", got, before+1)
	}
	if _, ok := Lookup("test.scoped"); !ok {
		t.Error("Lookup(test.scoped) = false; a registered setting must be findable")
	}
}

// The file channel is not scoped: a document describes a whole install, so it
// must parse the same way whichever subcommand reads it. ParseFile rejects
// unknown keys by consulting the merged registry, so a scoped key that is not
// in it would make a valid document fail under the wrong command.
func TestScopedSettingsAreVisibleToTheFileChannel(t *testing.T) {
	useSettings(t, testSetting("test.scoped", "ATE_TEST_SCOPED", "test-scoped", "deploy thing"))

	path := writeConfig(t, validHeader+"test:\n  scoped: value\n")
	f, err := ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile() error = %v; a scoped key must not be rejected as unknown", err)
	}
	if got, _ := f.Get("test.scoped"); got != "value" {
		t.Errorf("Get(test.scoped) = %q, want %q", got, "value")
	}
}

// Only the flag channel is scoped. Environment and file must reach a scoped
// setting; the flag reaches it only on its own command.
func TestScopedSettingResolvesFromEveryChannel(t *testing.T) {
	useSettings(t, testSetting("test.scoped", "ATE_TEST_SCOPED", "test-scoped", "deploy thing"))

	for _, tc := range []struct {
		name     string
		env      map[string]string
		file     string
		flag     string
		want     string
		wantFrom Origin
	}{
		{name: "default", want: "", wantFrom: OriginDefault},
		{
			name: "environment", env: map[string]string{"ATE_TEST_SCOPED": "fromenv"},
			want: "fromenv", wantFrom: OriginEnv,
		},
		{
			name: "file", file: "value: fromfile", want: "fromfile", wantFrom: OriginFile,
		},
		{
			name: "flag", flag: "fromflag", want: "fromflag", wantFrom: OriginFlag,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var file *File
			if tc.file != "" {
				file = &File{values: map[string]string{"test.scoped": "fromfile"}}
			}
			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			BindCommandFlags("deploy thing", fs)
			if tc.flag != "" {
				if err := fs.Set("test-scoped", tc.flag); err != nil {
					t.Fatalf("Set: %v", err)
				}
			}

			r, err := Resolve(fs, ResolveOptions{Env: tc.env, File: file})
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if got := r.String("test.scoped"); got != tc.want {
				t.Errorf("String() = %q, want %q", got, tc.want)
			}
			v, _ := r.Value("test.scoped")
			if v.From != tc.wantFrom {
				t.Errorf("From = %v, want %v", v.From, tc.wantFrom)
			}
		})
	}
}

func TestBindCommandFlagsBindsOnlyTheNamedPath(t *testing.T) {
	useSettings(t,
		testSetting("test.one", "ATE_TEST_ONE", "test-one", "deploy one"),
		testSetting("test.both", "ATE_TEST_BOTH", "test-both", "deploy one", "delete one"),
	)

	for _, tc := range []struct {
		path string
		want []string
		skip []string
	}{
		{path: "deploy one", want: []string{"test-one", "test-both"}},
		{path: "delete one", want: []string{"test-both"}, skip: []string{"test-one"}},
		{path: "deploy other", skip: []string{"test-one", "test-both"}},
	} {
		t.Run(tc.path, func(t *testing.T) {
			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			BindCommandFlags(tc.path, fs)
			for _, name := range tc.want {
				if fs.Lookup(name) == nil {
					t.Errorf("--%s was not bound to %q", name, tc.path)
				}
			}
			for _, name := range tc.skip {
				if fs.Lookup(name) != nil {
					t.Errorf("--%s was bound to %q, which does not own it", name, tc.path)
				}
			}
		})
	}
}

// A scoped flag on the root would appear in every subcommand's help, which is
// the reason for scoping it in the first place.
func TestBindFlagsSkipsScopedSettings(t *testing.T) {
	useSettings(t,
		testSetting("test.scoped", "ATE_TEST_SCOPED", "test-scoped", "deploy thing"),
		testSetting("test.global", "ATE_TEST_GLOBAL", "test-global"),
	)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	BindFlags(fs)
	if fs.Lookup("test-scoped") != nil {
		t.Error("--test-scoped was registered on the root flag set")
	}
	if fs.Lookup("test-global") == nil {
		t.Error("--test-global names no command and must be registered on the root")
	}
}

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		add     []Setting
		wantIn  string
		wantErr bool
	}{
		{name: "clean", add: []Setting{testSetting("test.ok", "ATE_TEST_OK", "test-ok", "deploy thing")}},
		{
			name: "duplicate key",
			add: []Setting{
				testSetting("test.dup", "ATE_TEST_A", "test-a", "deploy thing"),
				testSetting("test.dup", "ATE_TEST_B", "test-b", "deploy thing"),
			},
			wantErr: true, wantIn: `key "test.dup"`,
		},
		{
			name: "duplicate environment variable",
			add: []Setting{
				testSetting("test.a", "ATE_TEST_SAME", "test-a", "deploy thing"),
				testSetting("test.b", "ATE_TEST_SAME", "test-b", "deploy thing"),
			},
			wantErr: true, wantIn: `environment variable "ATE_TEST_SAME"`,
		},
		{
			name: "duplicate flag",
			add: []Setting{
				testSetting("test.a", "ATE_TEST_A", "test-same", "deploy thing"),
				testSetting("test.b", "ATE_TEST_B", "test-same", "deploy thing"),
			},
			wantErr: true, wantIn: `flag "test-same"`,
		},

		{
			name:    "collides with a setting the installer owns",
			add:     []Setting{testSetting("test.a", "ATE_NAMESPACE", "test-a", "deploy thing")},
			wantErr: true, wantIn: "namespace",
		},
		{
			name:    "missing environment variable",
			add:     []Setting{{Key: "test.a", Flag: "test-a", Kind: KindString, Usage: "u"}},
			wantErr: true, wantIn: "no environment variable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useSettings(t, tc.add...)
			err := Validate()
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Validate() = nil, want an error")
			}
			if !strings.Contains(err.Error(), tc.wantIn) {
				t.Errorf("Validate() = %q, missing %q", err, tc.wantIn)
			}
		})
	}
}

// A secret must not reach the command line even when its declaration names a
// flag. Validate reports such a declaration as a registry defect, but that
// only helps a caller who ran Validate: the property has to hold at the point
// the flag would be created, on the root and on an owning command alike.
func TestBindingRefusesSecretsThatDeclareAFlag(t *testing.T) {
	useSettings(t,
		Setting{
			Key: "test.secret", Env: "ATE_TEST_SECRET", Flag: "test-secret",
			Kind: KindString, Secret: true, Usage: "a secret that wrongly declares a flag",
		},
		Setting{
			Key: "test.scopedSecret", Env: "ATE_TEST_SCOPED_SECRET", Flag: "test-scoped-secret",
			Kind: KindString, Secret: true, Usage: "the same, scoped to one command",
			Commands: []string{"deploy thing"},
		},
	)

	root := pflag.NewFlagSet("root", pflag.ContinueOnError)
	BindFlags(root)
	if root.Lookup("test-secret") != nil {
		t.Error("BindFlags registered --test-secret; a credential must not be a flag")
	}

	own := pflag.NewFlagSet("own", pflag.ContinueOnError)
	BindCommandFlags("deploy thing", own)
	if own.Lookup("test-scoped-secret") != nil {
		t.Error("BindCommandFlags registered --test-scoped-secret; a credential must not be a flag")
	}
}
