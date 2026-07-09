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
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// Each system-info volume mount becomes a read-only bind mount whose source
// is the per-actor on-host SystemInfoVolumeRoot for that volume name. It is
// delivered as a bind mount rather than environment variables because env
// lives in the checkpointed process memory and would be frozen at the golden
// snapshot's values after a restore; a bind mount is re-attached per-actor on
// every resume.
func TestBuildActorOCISpec_SystemInfoVolumeMounts(t *testing.T) {
	const actorUID = "actor_uid"
	volumeMounts := []*ateletpb.VolumeMount{
		{Name: "sysinfo", MountPath: "/run/ate"},
	}
	volumes := []*ateletpb.Volume{
		{Name: "sysinfo", Source: &ateletpb.Volume_SystemInfo{SystemInfo: &ateletpb.SystemInfoVolume{}}},
	}
	spec := buildActorOCISpec(
		actorUID,
		[]string{"/app"},
		[]string{"FOO=bar"},
		map[string]string{"k": "v"},
		"/run/netns/x",
		volumes,
		volumeMounts,
	)
	found := false
	for _, m := range spec.Mounts {
		if m.Destination != "/run/ate" {
			continue
		}
		found = true
		if want := ateompath.SystemInfoVolumeRoot(actorUID, "sysinfo"); m.Source != want {
			t.Errorf("system-info mount source = %q, want %q", m.Source, want)
		}
		if m.Type != "bind" {
			t.Errorf("system-info mount type = %q, want bind", m.Type)
		}
		if !slices.Contains(m.Options, "ro") {
			t.Errorf("system-info mount must be read-only, options=%v", m.Options)
		}
	}
	if !found {
		t.Fatalf("system-info mount %q missing; mounts=%v", "/run/ate", spec.Mounts)
	}
}

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

// Each durable-dir volume mount becomes a bind mount whose source is the
// per-actor on-host DurableDirVolumeMountPoint for that volume name.
func TestBuildActorOCISpec_DurableDirVolumeMounts(t *testing.T) {
	const actorUID = "actor_uid"
	durableDirs := []*ateletpb.VolumeMount{
		{Name: "data", MountPath: "/var/data"},
		{Name: "cache", MountPath: "/var/cache"},
	}
	volumes := []*ateletpb.Volume{
		{Name: "data", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
		{Name: "cache", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
	}
	spec := buildActorOCISpec(
		actorUID,
		[]string{"/app"}, nil, nil,
		"/run/netns/x",
		volumes,
		durableDirs,
	)

	for _, vm := range durableDirs {
		wantSrc := ateompath.DurableDirVolumeMountPoint(actorUID, vm.Name)
		found := false
		for _, m := range spec.Mounts {
			if m.Destination != vm.MountPath {
				continue
			}
			found = true
			if m.Source != wantSrc {
				t.Errorf("durable-dir %q source = %q, want %q", vm.Name, m.Source, wantSrc)
			}
			if m.Type != "bind" {
				t.Errorf("durable-dir %q type = %q, want bind", vm.Name, m.Type)
			}
		}
		if !found {
			t.Fatalf("durable-dir mount for %q missing; mounts=%v", vm.MountPath, spec.Mounts)
		}
	}
}
