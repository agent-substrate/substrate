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

package steps

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"sigs.k8s.io/kustomize/kyaml/kio"
	"sigs.k8s.io/kustomize/kyaml/yaml"
)

const idpIssuerFlag = "--issuer"

// idpServerManifestPath is the ate-idp-server manifest.
func (e *Env) idpServerManifestPath() string {
	return e.Cfg.Manifest("ate-idp-server.yaml")
}

// deployIDPServer applies ate-idp-server on its own.
func (e *Env) deployIDPServer(ctx context.Context) error {
	manifest, err := e.renderIDPServer()
	if err != nil {
		return err
	}
	return e.ResolveAndApplyBytes(ctx, manifest)
}

// renderIDPServer renders the ate-idp-server manifest with the install's
// issuer, as the system bundle carries it.
func (e *Env) renderIDPServer() ([]byte, error) {
	manifest, err := e.render(e.idpServerManifestPath())
	if err != nil {
		return nil, err
	}
	return setIDPServerIssuer(manifest, e.Cfg.ActorJWTIssuer)
}

// setIDPServerIssuer sets --issuer on the ate-idp-server container in manifest,
// replacing any --issuer it already carries.
func setIDPServerIssuer(manifest []byte, issuer string) ([]byte, error) {
	nodes, err := (&kio.ByteReader{
		Reader:                bytes.NewReader(manifest),
		OmitReaderAnnotations: true,
	}).Read()
	if err != nil {
		return nil, fmt.Errorf("while parsing manifest: %w", err)
	}
	for _, node := range nodes {
		if node.GetKind() != "Deployment" || node.GetName() != "ate-idp-server" {
			continue
		}
		container, err := node.Pipe(yaml.Lookup("spec", "template", "spec", "containers", "[name=ate-idp-server]"))
		if err != nil {
			return nil, err
		}
		if container == nil {
			return nil, errors.New("deployment/ate-idp-server has no ate-idp-server container")
		}
		args, err := container.Pipe(yaml.LookupCreate(yaml.SequenceNode, "args"))
		if err != nil {
			return nil, err
		}
		kept := args.YNode().Content[:0]
		for _, item := range args.YNode().Content {
			if !strings.HasPrefix(item.Value, idpIssuerFlag+"=") {
				kept = append(kept, item)
			}
		}
		args.YNode().Content = append(kept, yaml.NewStringRNode(idpIssuerFlag+"="+issuer).YNode())
		var out bytes.Buffer
		if err := (kio.ByteWriter{Writer: &out}).Write(nodes); err != nil {
			return nil, fmt.Errorf("while writing manifest: %w", err)
		}
		return out.Bytes(), nil
	}
	return nil, errors.New("the manifest has no deployment/ate-idp-server")
}
