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

package controlapi

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// The auto-injected egress trust volume: under --inject-egress-trust-bundle
// every actor trusts the egress gateway's MITM CA with nothing declared in
// its template.
const (
	// The '.' makes template collisions impossible: template volume names
	// must be DNS labels, and ate.dev names belong to the platform.
	egressTrustVolumeName = "trust.ate.dev"

	// Must stay on atelet's allowlist (cmd/atelet/trustbundle.go), which
	// resolves it at every Run/Restore and fails actor start without it.
	egressTrustBundleName = "egress-mitm.ate.dev"

	egressTrustMountPath  = "/run/substrate/certs"
	egressTrustBundleFile = "egress-mitm.ate.dev.pem"
)

// injectEgressTrustVolume mounts the egress trust volume into every container
// except those that own the path (ownsEgressTrustPath); if all opt out, no
// volume is injected, so the actor is not gated on a bundle nothing reads.
func injectEgressTrustVolume(ctx context.Context, actor *ateapipb.Actor, spec *ateletpb.WorkloadSpec) error {
	for _, vol := range spec.GetVolumes() {
		if vol.GetName() == egressTrustVolumeName {
			return fmt.Errorf("volume name %q is reserved for the injected egress trust bundle", egressTrustVolumeName)
		}
	}
	mounted := false
	for _, ctr := range spec.GetContainers() {
		if path, ok := ownsEgressTrustPath(spec, ctr); ok {
			slog.InfoContext(ctx, "Not mounting the egress trust volume: the template owns the path",
				slog.String("atespace", actor.GetMetadata().GetAtespace()),
				slog.String("actor", actor.GetMetadata().GetName()),
				slog.String("container", ctr.GetName()),
				slog.String("mountPath", path))
			continue
		}
		ctr.VolumeMounts = append(ctr.VolumeMounts, &ateletpb.VolumeMount{
			Name:      egressTrustVolumeName,
			MountPath: egressTrustMountPath,
		})
		mounted = true
	}
	if !mounted {
		return nil
	}
	spec.Volumes = append(spec.Volumes, &ateletpb.Volume{
		Name: egressTrustVolumeName,
		Source: &ateletpb.Volume_SystemInfo{
			SystemInfo: &ateletpb.SystemInfoVolume{
				DataSources: []*ateletpb.SystemInfoDataSource{{
					DataSource: &ateletpb.SystemInfoDataSource_TrustBundle{
						TrustBundle: &ateletpb.TrustBundleDataSource{
							Name: egressTrustBundleName,
							Path: egressTrustBundleFile,
						},
					},
				}},
			},
		},
	})
	return nil
}

// ownsEgressTrustPath returns the template mount that opts ctr out: at or
// below the reserved path, or an image volume above it — micro-VM bind
// ordering would shadow one mount with the other either way.
func ownsEgressTrustPath(spec *ateletpb.WorkloadSpec, ctr *ateletpb.Container) (string, bool) {
	for _, m := range ctr.GetVolumeMounts() {
		mp := m.GetMountPath()
		if mp == egressTrustMountPath || strings.HasPrefix(mp, egressTrustMountPath+"/") {
			return mp, true
		}
		if strings.HasPrefix(egressTrustMountPath, mp+"/") && isImageVolume(spec, m.GetName()) {
			return mp, true
		}
	}
	return "", false
}

func isImageVolume(spec *ateletpb.WorkloadSpec, name string) bool {
	for _, vol := range spec.GetVolumes() {
		if vol.GetName() == name {
			return vol.GetImage() != nil
		}
	}
	return false
}
