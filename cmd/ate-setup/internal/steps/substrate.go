// Copyright 2026 The Agent Substrate Authors
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

package steps

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"sigs.k8s.io/yaml"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
)

// AteClient connects to the ate-api-server, port-forwarding if needed. It
// opts in to preview features if the install enables any.
func (e *Env) AteClient(ctx context.Context) (*ateclient.Client, error) {
	var opts []grpc.DialOption
	if len(e.Cfg.PreviewGates) > 0 {
		opts = preview.DialOptionsWithOptIn()
	}
	return ateclient.NewClient(ctx, e.Cfg.Kubeconfig, e.Cfg.Context, "", "", false, opts...)
}

// EnsureAtespace creates the atespace if it does not already exist. The store
// enforces that an ActorTemplate's atespace exists at create time.
func EnsureAtespace(ctx context.Context, client *ateclient.Client, atespace string) error {
	_, err := client.CreateAtespace(ctx, &ateapi.CreateAtespaceRequest{
		Atespace: &ateapi.Atespace{
			Metadata: &ateapi.ResourceMetadata{Name: atespace},
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		return nil
	}
	return err
}

// unmarshalProtoManifest parses a single protojson-shaped YAML or JSON
// document into dst. Parsing is strict so typos fail instead of being dropped.
func unmarshalProtoManifest(manifest []byte, dst proto.Message) error {
	jsonData, err := yaml.YAMLToJSON(manifest)
	if err != nil {
		return fmt.Errorf("invalid YAML: %w", err)
	}
	if string(jsonData) == "null" {
		return fmt.Errorf("manifest is empty")
	}
	return protojson.Unmarshal(jsonData, dst)
}

// ActorTemplateFromManifest parses a single protojson-shaped YAML or JSON
// document into an ActorTemplate, as `kubectl ate create actor-template`
// does. Parsing is strict: unknown fields are an error, so typos don't
// silently drop configuration.
func ActorTemplateFromManifest(manifest []byte) (*ateapi.ActorTemplate, error) {
	template := &ateapi.ActorTemplate{}
	if err := unmarshalProtoManifest(manifest, template); err != nil {
		return nil, err
	}
	return template, nil
}

// CSIDriverConfigFromManifest parses a single protojson-shaped YAML or JSON
// document into a CsiDriverConfig, as `kubectl ate create csi-driver-config`
// does.
func CSIDriverConfigFromManifest(manifest []byte) (*ateapi.CsiDriverConfig, error) {
	cfg := &ateapi.CsiDriverConfig{}
	if err := unmarshalProtoManifest(manifest, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

type csiDriverConfigClient interface {
	CreateCsiDriverConfig(ctx context.Context, in *ateapi.CreateCsiDriverConfigRequest, opts ...grpc.CallOption) (*ateapi.CsiDriverConfig, error)
	GetCsiDriverConfig(ctx context.Context, in *ateapi.GetCsiDriverConfigRequest, opts ...grpc.CallOption) (*ateapi.CsiDriverConfig, error)
	UpdateCsiDriverConfig(ctx context.Context, in *ateapi.UpdateCsiDriverConfigRequest, opts ...grpc.CallOption) (*ateapi.CsiDriverConfig, error)
}

// RegisterCSIDriverConfigs creates or updates the canonical CsiDriverConfig
// resources for the selected CSI driver when ate-api-server is deployed. If
// ate-api-server rejects the request because the ExternalVolumes preview gate
// is disabled, registration is skipped with a warning.
func (e *Env) RegisterCSIDriverConfigs(ctx context.Context, driver string) error {
	if err := ValidateCSIDriver(driver); err != nil {
		return err
	}
	spec := csiDrivers[driver]
	if spec.installsNothing() {
		return nil
	}
	configs, err := csiDriverConfigsForDriver(e.Cfg, spec)
	if err != nil {
		return err
	}
	present, err := e.Kube.DeploymentExists(ctx, e.Namespace(), "ate-api-server")
	if err != nil {
		return err
	}
	if !present {
		log.Step("ate-api-server not found; skipping CsiDriverConfig registration")
		return nil
	}

	client, err := ateclient.NewClient(ctx, e.Cfg.Kubeconfig, e.Cfg.Context, "", "", false)
	if err != nil {
		return err
	}
	defer client.Close()

	return registerCSIDriverConfigs(ctx, client, configs)
}

func csiDriverConfigsForDriver(cfg *config.Config, spec csiDriverSpec) ([]*ateapi.CsiDriverConfig, error) {
	var paths []string
	if spec.hostpath {
		paths = append(paths, cfg.Path("hack", "csi", "csi-driver-config-hostpath.yaml"))
	}
	if spec.nfs {
		paths = append(paths, cfg.Path("hack", "csi", "csi-driver-config-nfs.yaml"))
	}
	configs := make([]*ateapi.CsiDriverConfig, 0, len(paths))
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("reading csi driver config manifest %s: %w", p, err)
		}
		cdc, err := CSIDriverConfigFromManifest(data)
		if err != nil {
			return nil, fmt.Errorf("parsing csi driver config manifest %s: %w", p, err)
		}
		configs = append(configs, cdc)
	}
	return configs, nil
}

// isExternalVolumesDisabledError reports whether err is ate-api-server's
// validation rejection when the ExternalVolumes preview gate is disabled.
func isExternalVolumesDisabledError(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.InvalidArgument && strings.HasPrefix(st.Message(), "csi_driver_config: Forbidden")
}

func registerCSIDriverConfigs(ctx context.Context, client csiDriverConfigClient, configs []*ateapi.CsiDriverConfig) error {
	// Opt in on the request so ate-setup setup csi works without --preview
	// against an ate-api-server deployed with --preview=ExternalVolumes.
	ctx = metadata.AppendToOutgoingContext(ctx, preview.Header, "true")
	for _, cfg := range configs {
		name := cfg.GetMetadata().GetName()
		_, err := client.CreateCsiDriverConfig(ctx, &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: cfg})
		if err == nil {
			log.Stepf("registered csi driver config %q", name)
			continue
		}
		if isExternalVolumesDisabledError(err) {
			log.Warnf("Skipping CsiDriverConfig registration: %s preview gate is disabled on ate-api-server.", preview.GateExternalVolumes)
			return nil
		}
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("creating csi driver config %q: %w", name, err)
		}
		existing, getErr := client.GetCsiDriverConfig(ctx, &ateapi.GetCsiDriverConfigRequest{
			CsiDriverConfig: &ateapi.ObjectRef{Name: name},
		})
		if status.Code(getErr) == codes.NotFound {
			return fmt.Errorf("csi driver config %q could not be created (AlreadyExists) and was not found by name (another CsiDriverConfig may already register driver %q)", name, cfg.GetDriverName())
		}
		if getErr != nil {
			return fmt.Errorf("getting existing csi driver config %q: %w", name, getErr)
		}
		if existing.GetDriverName() != cfg.GetDriverName() {
			return fmt.Errorf("csi driver config %q already exists with a different driverName (got %q, want %q); delete and recreate it",
				name, existing.GetDriverName(), cfg.GetDriverName())
		}
		if csiDriverConfigSpecEqual(cfg, existing) {
			log.Stepf("csi driver config %q already exists; keeping it", name)
			continue
		}
		updated := proto.CloneOf(cfg)
		updated.Metadata = &ateapi.ResourceMetadata{
			Name:    name,
			Uid:     existing.GetMetadata().GetUid(),
			Version: existing.GetMetadata().GetVersion(),
		}
		if _, err := client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: updated}); err != nil {
			if isExternalVolumesDisabledError(err) {
				log.Warnf("Skipping CsiDriverConfig registration: %s preview gate is disabled on ate-api-server.", preview.GateExternalVolumes)
				return nil
			}
			return fmt.Errorf("updating csi driver config %q: %w", name, err)
		}
		log.Stepf("updated csi driver config %q", name)
	}
	return nil
}

func csiDriverConfigSpecEqual(a, b *ateapi.CsiDriverConfig) bool {
	return a.GetDriverName() == b.GetDriverName() &&
		a.GetControllerEndpoint() == b.GetControllerEndpoint() &&
		a.GetNodeSocketOverride() == b.GetNodeSocketOverride() &&
		a.GetTls().GetEnabled() == b.GetTls().GetEnabled() &&
		a.GetTls().GetUsePodIdentity() == b.GetTls().GetUsePodIdentity() &&
		a.GetTls().GetServerName() == b.GetTls().GetServerName()
}

// CreateActorTemplate creates the template through the ate API. Actor
// templates are immutable (no update RPC), so an existing template is left
// in place: delete the demo and redeploy to change it.
func CreateActorTemplate(ctx context.Context, client *ateclient.Client, template *ateapi.ActorTemplate) error {
	ref := resources.ActorTemplateRefFromActorTemplate(template)
	_, err := client.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: template})
	if status.Code(err) == codes.AlreadyExists {
		log.Stepf("actor template %s already exists; keeping it (delete the demo to replace it)", ref)
		return nil
	}
	if err != nil {
		return fmt.Errorf("creating actor template %s: %w", ref, err)
	}
	return nil
}

// CreateActor creates an actor from template through the ate API, leaving an
// existing actor in place so re-running deploy is idempotent.
func CreateActor(ctx context.Context, client *ateclient.Client, actor resources.ActorRef, template resources.ActorTemplateRef) error {
	_, err := client.CreateActor(ctx, &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata: &ateapi.ResourceMetadata{
				Atespace: actor.Atespace,
				Name:     actor.Name,
			},
			ActorTemplate: template.ToObjectRef(),
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		log.Stepf("actor %s already exists; keeping it", actor)
		return nil
	}
	if err != nil {
		return fmt.Errorf("creating actor %s: %w", actor, err)
	}
	return nil
}

// CreateActorEgressPolicy gives actor an egress policy with rules, leaving an
// existing policy in place so re-running deploy is idempotent.
func CreateActorEgressPolicy(ctx context.Context, client *ateclient.Client, actor resources.ActorRef, rules []*ateapi.EgressRule) error {
	_, err := client.CreateActorEgressPolicy(ctx, &ateapi.CreateActorEgressPolicyRequest{
		Actor: actor.ToObjectRef(),
		EgressPolicy: &ateapi.EgressPolicy{
			Metadata: &ateapi.ResourceMetadata{
				Atespace: actor.Atespace,
				Name:     resources.SingletonName,
			},
			Rules: rules,
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		log.Stepf("egress policy for actor %s already exists; keeping it", actor)
		return nil
	}
	if err != nil {
		return fmt.Errorf("creating egress policy for actor %s: %w", actor, err)
	}
	return nil
}

// WaitActorTemplateGolden blocks until the template's golden snapshot is
// built. It fails fast when the template reconciler reports an error, and
// returns immediately for a template that opted out of the golden snapshot.
func WaitActorTemplateGolden(ctx context.Context, client *ateclient.Client, ref resources.ActorTemplateRef, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	// The last Get error rides along in the timeout error: a wait that spent
	// its whole budget on a broken connection should say so, not point at the
	// template reconciler.
	var lastErr error
	for {
		template, err := client.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: ref.ToObjectRef()})
		lastErr = err
		if err == nil {
			if template.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() == ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED {
				// The template opted out: no golden snapshot will ever appear,
				// and none is needed.
				return nil
			}
			goldenStatus := template.GetStatus().GetGoldenSnapshotStatus()
			if goldenStatus.GetGoldenTag().GetName() != "" {
				return nil
			}
			if msg := goldenStatus.GetErrorMessage(); msg != "" {
				return fmt.Errorf("actor template %s failed: %s", ref, msg)
			}
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("timed out waiting for actor template %s golden snapshot: last error: %w", ref, lastErr)
			}
			return fmt.Errorf("timed out waiting for actor template %s golden snapshot", ref)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// DeleteSubstrateDemo removes a substrate demo's control-plane resources:
// every actor created from the given templates, then the templates (which
// server-side also removes their golden actors and snapshots), then the
// atespaces. A cluster without a reachable
// ate-api-server is not an error -- there is nothing to clean up.
func (e *Env) DeleteSubstrateDemo(ctx context.Context, refs []resources.ActorTemplateRef, atespaces []string) error {
	present, err := e.Kube.DeploymentExists(ctx, e.Namespace(), "ate-api-server")
	if err != nil {
		return err
	}
	if !present {
		log.Step("ate-api-server not found; skipping actor cleanup")
		return nil
	}

	client, err := e.AteClient(ctx)
	if err != nil {
		log.Warnf("could not connect to ate-api-server; skipping actor cleanup: %v", err)
		return nil
	}
	defer client.Close()

	actors, err := listAllActors(ctx, client)
	if err != nil {
		log.Warnf("could not list actors; skipping actor cleanup: %v", err)
		return nil
	}

	for _, ref := range refs {
		log.Stepf("Deleting actors for %s", ref)
		for _, actor := range actors {
			if resources.ActorTemplateRefFromObjectRef(actor.GetActorTemplate()) != ref {
				continue
			}
			actorRef := resources.ActorRefFromActor(actor)
			log.Stepf("  deleting actor %s/%s", actorRef.Atespace, actorRef.Name)
			if _, err := client.DeleteActor(ctx, &ateapi.DeleteActorRequest{
				Actor: actorRef.ToObjectRef(),
			}); err != nil {
				log.Warnf("while deleting actor %s/%s: %v", actorRef.Atespace, actorRef.Name, err)
			}
		}
	}

	for _, ref := range refs {
		if _, err := client.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: ref.ToObjectRef()}); err != nil && status.Code(err) != codes.NotFound {
			log.Warnf("while deleting actor template %s: %v", ref, err)
		}
	}

	for _, atespace := range atespaces {
		if _, err := client.DeleteAtespace(ctx, &ateapi.DeleteAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: atespace}}); err != nil && status.Code(err) != codes.NotFound {
			log.Warnf("while deleting atespace %s: %v", atespace, err)
		}
	}
	return nil
}
