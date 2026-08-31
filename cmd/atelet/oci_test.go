//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"errors"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

func TestResolveActorEnv(t *testing.T) {
	defaultPath := "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

	tests := []struct {
		name        string
		image       *v1.Config
		templateEnv []string
		want        []string
	}{
		{
			name:        "template overrides image by key",
			image:       &v1.Config{Env: []string{"FOO=image"}},
			templateEnv: []string{"FOO=template"},
			want:        []string{"FOO=template", defaultPath},
		},
		{
			name:        "default PATH applies when neither sets it",
			image:       &v1.Config{Env: []string{"FOO=image"}},
			templateEnv: []string{"BAR=template"},
			want:        []string{"BAR=template", "FOO=image", defaultPath},
		},
		{
			name:  "image PATH overrides default",
			image: &v1.Config{Env: []string{"PATH=/image/bin"}},
			want:  []string{"PATH=/image/bin"},
		},
		{
			name:        "template PATH overrides default",
			image:       &v1.Config{},
			templateEnv: []string{"PATH=/template/bin"},
			want:        []string{"PATH=/template/bin"},
		},
		{
			name:  "blank and keyless entries are dropped",
			image: &v1.Config{Env: []string{"", "=novalue"}},
			want:  []string{defaultPath},
		},
		{
			name:        "nil image config uses template env and default PATH",
			image:       nil,
			templateEnv: []string{"FOO=template"},
			want:        []string{"FOO=template", defaultPath},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveActorEnv(tc.image, tc.templateEnv)
			if !slices.Equal(got, tc.want) {
				t.Errorf("resolveActorEnv(%v, %v) =\n  %v\nwant:\n  %v", tc.image, tc.templateEnv, got, tc.want)
			}
		})
	}
}

func TestResolveProcessArgs(t *testing.T) {
	cfg := func(entrypoint, cmd []string) *v1.Config {
		return &v1.Config{Entrypoint: entrypoint, Cmd: cmd}
	}

	tests := []struct {
		name    string
		image   *v1.Config
		command []string
		args    []string
		want    []string
		wantErr bool
	}{
		{
			name:  "image ENTRYPOINT+CMD used when neither is overridden",
			image: cfg([]string{"/app"}, []string{"--serve"}),
			want:  []string{"/app", "--serve"},
		},
		{
			name:  "args override CMD, image ENTRYPOINT kept",
			image: cfg([]string{"/init", "/wrapper.sh"}, nil),
			args:  []string{"serve"},
			want:  []string{"/init", "/wrapper.sh", "serve"},
		},
		{
			name:    "command overrides both ENTRYPOINT and CMD",
			image:   cfg([]string{"/app"}, []string{"--serve"}),
			command: []string{"/other"},
			want:    []string{"/other"},
		},
		{
			name:    "command and args override both",
			image:   cfg([]string{"/app"}, []string{"--serve"}),
			command: []string{"/other"},
			args:    []string{"--flag"},
			want:    []string{"/other", "--flag"},
		},
		{
			name:  "image ENTRYPOINT only, no CMD",
			image: cfg([]string{"/ko-app/counter"}, nil),
			want:  []string{"/ko-app/counter"},
		},
		{
			name:    "no image config, command supplies argv",
			image:   nil,
			command: []string{"/pause"},
			want:    []string{"/pause"},
		},
		{
			name:    "empty argv is an error",
			image:   cfg(nil, nil),
			wantErr: true,
		},
		{
			name:    "nil image and no overrides is an error",
			image:   nil,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveProcessArgs(tc.image, tc.command, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("resolveProcessArgs(%v, %v, %v) err = %v, wantErr %v", tc.image, tc.command, tc.args, err, tc.wantErr)
			}
			if err != nil {
				if !errors.Is(err, ateerrors.ReasonInvalidContainerConfig) {
					t.Errorf("empty-argv error must carry ReasonInvalidContainerConfig, got: %v", err)
				}
				return
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("resolveProcessArgs(%v, %v, %v) = %v, want %v", tc.image, tc.command, tc.args, got, tc.want)
			}
		})
	}
}

// wantDefaultCapabilities is the set a container gets when it asks for no
// adjustment. It is spelled out rather than derived from defaultCapabilities so
// that widening or narrowing the default is a deliberate test change.
var wantDefaultCapabilities = []string{
	"CAP_AUDIT_WRITE",
	"CAP_KILL",
	"CAP_NET_BIND_SERVICE",
}

func withoutCaps(in []string, drop ...string) []string {
	out := slices.Clone(in)
	for _, d := range drop {
		out = slices.DeleteFunc(out, func(c string) bool { return c == d })
	}
	return out
}

func withCaps(in []string, add ...string) []string {
	out := append(slices.Clone(in), add...)
	slices.Sort(out)
	return out
}

func TestResolveCapabilities(t *testing.T) {
	tests := []struct {
		name string
		caps *ateletpb.Capabilities
		want []string
	}{{
		name: "unset keeps the default set",
		caps: nil,
		want: wantDefaultCapabilities,
	}, {
		name: "empty keeps the default set",
		caps: &ateletpb.Capabilities{},
		want: wantDefaultCapabilities,
	}, {
		name: "drop removes from the default set",
		caps: &ateletpb.Capabilities{Drop: []string{"NET_BIND_SERVICE", "AUDIT_WRITE"}},
		want: withoutCaps(wantDefaultCapabilities, "CAP_NET_BIND_SERVICE", "CAP_AUDIT_WRITE"),
	}, {
		name: "add grants on top of the default set",
		caps: &ateletpb.Capabilities{Add: []string{"SYS_ADMIN"}},
		want: withCaps(wantDefaultCapabilities, "CAP_SYS_ADMIN"),
	}, {
		name: "drop ALL clears the default set",
		caps: &ateletpb.Capabilities{Drop: []string{"ALL"}},
		want: nil,
	}, {
		name: "drop ALL with add gives an exact set",
		caps: &ateletpb.Capabilities{Drop: []string{"ALL"}, Add: []string{"NET_ADMIN", "CHOWN"}},
		want: []string{"CAP_CHOWN", "CAP_NET_ADMIN"},
	}, {
		// Drop applies first, so naming a capability in both grants it.
		name: "add wins over drop",
		caps: &ateletpb.Capabilities{Drop: []string{"KILL"}, Add: []string{"KILL"}},
		want: wantDefaultCapabilities,
	}, {
		name: "adding a default capability does not duplicate it",
		caps: &ateletpb.Capabilities{Add: []string{"KILL"}},
		want: wantDefaultCapabilities,
	}, {
		name: "dropping a capability outside the default set is a no-op",
		caps: &ateletpb.Capabilities{Drop: []string{"SYS_ADMIN"}},
		want: wantDefaultCapabilities,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveCapabilities(tt.caps)
			if !slices.Equal(got, tt.want) {
				t.Errorf("resolveCapabilities() = %v, want %v", got, tt.want)
			}
		})
	}
}
