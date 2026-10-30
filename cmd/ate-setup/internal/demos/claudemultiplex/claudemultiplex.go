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

// Package claudemultiplex installs the claude-code-multiplex demo, which runs
// several Claude Code agents side by side on one WorkerPool.
//
// Its workload is a Dockerfile-based Node.js and Bash wrapper around Claude
// Code rather than a Go binary, so the image is built with docker buildx
// instead of ko and the resolved digest is substituted into the agent
// templates.
package claudemultiplex

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/yaml"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/demos"
	"go.ate.dev/substrate/cmd/ate-setup/internal/images"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/steps"
	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/resources"
)

const (
	poolManifest = "demos/claude-code-multiplex/claude-code-multiplex.yaml.tmpl"
	workload     = "demos/claude-code-multiplex/workload"
	// atespace doubles as the pool's k8s namespace.
	atespace = "claude-multiplex-demo"
	// imageName is appended to KO_DOCKER_REPO to form the workload image
	// repository.
	imageName              = "claude-multiplex-demo-workload"
	secretName             = "anthropic-api-key"
	secretKey              = "api-key"
	credentialURI          = "ate-secret://k8s.io/default/" + atespace + "/" + secretName + "/" + secretKey
	credProviderConfigMap  = "k8s-credential-provider-namespace-policy"
	credProviderPolicyKey  = "namespace-policy.yaml"
	credProviderDeployment = "k8s-credential-provider"
)

// demo wraps a demos.Substrate rather than embedding it, so the shared render
// test does not pick this demo up: its agent templates carry a deploy-time
// placeholder (WORKLOAD_IMAGE), and this package's own test renders them with
// a stand-in value instead.
type demo struct {
	sub demos.Substrate
}

func init() {
	d := &demo{}
	d.sub = demos.Substrate{
		DemoName:           "demo-claude-code-multiplex",
		Short:              "Several Claude Code agents multiplexed onto one WorkerPool (requires ANTHROPIC_API_KEY, KO_DOCKER_REPO)",
		WorkerPoolManifest: poolManifest,
		Deployments:        []steps.TemplateRef{{Atespace: atespace, Name: "claude-workerpool"}},
		Templates:          agentTemplates(),
		RenderValues:       d.renderValues,
	}
	demos.Register(d)
}

var agents = []struct {
	actor, template string
}{
	{"luna", "agent-luna"},
	{"mars", "agent-mars"},
	{"orion", "agent-orion"},
}

// agentTemplates lists the demo's three agent ActorTemplates.
func agentTemplates() []demos.SubstrateTemplate {
	templates := make([]demos.SubstrateTemplate, 0, len(agents))
	for _, a := range agents {
		templates = append(templates, demos.SubstrateTemplate{
			Manifest: "demos/claude-code-multiplex/" + a.template + "-template.yaml.tmpl",
			Ref:      resources.ActorTemplateRef{Atespace: atespace, Name: a.template},
		})
	}
	return templates
}

func (d *demo) Name() string               { return d.sub.Name() }
func (d *demo) Description() string        { return d.sub.Description() }
func (d *demo) Settings() []config.Setting { return nil }

func (d *demo) Deploy(ctx context.Context, e *steps.Env) error {
	if e.Cfg.AnthropicAPIKey == "" {
		return fmt.Errorf("ANTHROPIC_API_KEY must be set")
	}
	if e.Cfg.KODockerRepo == "" {
		return fmt.Errorf("KO_DOCKER_REPO must be set (see hack/ate-dev-env.sh.example)")
	}
	if err := d.sub.Deploy(ctx, e); err != nil {
		return err
	}
	if err := e.Kube.ApplySecret(ctx, atespace, secretName, map[string]string{secretKey: e.Cfg.AnthropicAPIKey}); err != nil {
		return err
	}
	if err := allowCredentialProviderNamespace(ctx, e); err != nil {
		return err
	}

	client, err := e.AteClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	rules := []*ateapi.EgressRule{{
		Https: &ateapi.HttpsRule{
			Hostnames: []string{"api.anthropic.com"},
			Effects: &ateapi.HttpRuleEffects{
				ReplaceHeaders: []*ateapi.CredentialHeader{{Header: "x-api-key", CredentialUri: credentialURI}},
			},
		},
	}}
	for _, a := range agents {
		if err := ensureActorAndEgress(ctx, client, a.actor, a.template, rules); err != nil {
			return err
		}
	}
	return nil
}

func ensureActorAndEgress(ctx context.Context, client *ateclient.Client, actor, template string, rules []*ateapi.EgressRule) error {
	_, err := client.CreateActor(ctx, &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: atespace, Name: actor},
			ActorTemplate: &ateapi.ObjectRef{Atespace: atespace, Name: template},
		},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("creating actor %s/%s: %w", atespace, actor, err)
	}

	ref := &ateapi.ObjectRef{Atespace: atespace, Name: actor}
	policy := &ateapi.EgressPolicy{
		Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: resources.SingletonName},
		Rules:    rules,
	}
	_, err = client.CreateActorEgressPolicy(ctx, &ateapi.CreateActorEgressPolicyRequest{Actor: ref, EgressPolicy: policy})
	if status.Code(err) != codes.AlreadyExists {
		return err
	}
	cur, err := client.GetActorEgressPolicy(ctx, &ateapi.GetActorEgressPolicyRequest{Actor: ref})
	if err != nil {
		return err
	}
	policy.Metadata.Version = cur.GetMetadata().GetVersion()
	_, err = client.UpdateActorEgressPolicy(ctx, &ateapi.UpdateActorEgressPolicyRequest{Actor: ref, EgressPolicy: policy})
	return err
}

func allowCredentialProviderNamespace(ctx context.Context, e *steps.Env) error {
	cm, err := e.Kube.GetConfigMap(ctx, e.Namespace(), credProviderConfigMap)
	if err != nil || cm == nil {
		return err
	}
	var doc struct {
		Policies []struct {
			Atespace          string         `json:"atespace"`
			AllowedNamespaces []string       `json:"allowedNamespaces"`
			SecretSelector    map[string]any `json:"secretSelector,omitempty"`
		} `json:"policies"`
	}
	if raw := strings.TrimSpace(cm.Data[credProviderPolicyKey]); raw != "" {
		if err := yaml.UnmarshalStrict([]byte(raw), &doc); err != nil {
			return err
		}
	}
	for _, p := range doc.Policies {
		if p.Atespace == atespace && len(p.SecretSelector) == 0 && slices.Contains(p.AllowedNamespaces, atespace) {
			return nil
		}
	}
	doc.Policies = append(doc.Policies, struct {
		Atespace          string         `json:"atespace"`
		AllowedNamespaces []string       `json:"allowedNamespaces"`
		SecretSelector    map[string]any `json:"secretSelector,omitempty"`
	}{Atespace: atespace, AllowedNamespaces: []string{atespace}})
	out, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	if err := e.Kube.MergePatchConfigMap(ctx, e.Namespace(), credProviderConfigMap, map[string]string{credProviderPolicyKey: string(out)}); err != nil {
		return err
	}
	if ok, err := e.Kube.DeploymentExists(ctx, e.Namespace(), credProviderDeployment); err != nil || !ok {
		return err
	}
	patch := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":%q}}}}}`, time.Now().UTC().Format(time.RFC3339Nano))
	if err := e.Kube.PatchDeployment(ctx, e.Namespace(), credProviderDeployment, []byte(patch)); err != nil {
		return err
	}
	return e.Kube.RolloutStatus(ctx, kube.KindDeployment, e.Namespace(), credProviderDeployment, steps.DemoTimeout)
}

func (d *demo) Delete(ctx context.Context, e *steps.Env) error {
	return d.sub.Delete(ctx, e)
}

func (d *demo) renderValues(ctx context.Context, e *steps.Env) (map[string]string, error) {
	image, err := images.BuildDockerfileImage(ctx, e.Cfg.Root, e.Cfg.KODockerRepo, imageName, e.Cfg.Path(workload), e.Cfg.KODefaultPlatforms, e.Cfg.DockerBuildFlags)
	if err != nil {
		return nil, err
	}
	return map[string]string{"WORKLOAD_IMAGE": image}, nil
}
