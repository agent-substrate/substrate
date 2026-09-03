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

// Package counter installs the counter demo: a counter actor exercising
// snapshot, resume, and atenet ingress, optionally with an external volume
// attached so the CSI path is covered end to end.
package counter

import (
	"context"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	namespace = "ate-demo-counter"
	template  = "demos/counter/counter-template.yaml.tmpl"

	defaultStorageClass = "standard"
)

func (d *demo) externalVolumeValues(e *steps.Env) map[string]string {
	sc := defaultStorageClass
	if d.storageClass != "" {
		sc = d.storageClass
	}
	return map[string]string{
		"VALIDATE_EXISTING_FILE_PATH_ARG": "  - --validate-existing-file-path=/external-data/test.txt",
		"EXTERNAL_VOLUME_MOUNTS": "  - name: external-data\n" +
			"    mountPath: /external-data",
		"EXTERNAL_VOLUMES": "- name: external-data\n" +
			"  externalVolumeTemplate:\n" +
			"    capacity: 1Gi\n" +
			"    storageClassName: " + sc,
	}
}

// demo is the counter demo, which can optionally be deployed with an external
// volume attached.
type demo struct {
	demos.Substrate

	withExternalVolume bool
	storageClass       string
}

func init() {
	d := &demo{}
	d.Substrate = demos.Substrate{
		DemoName:           "demo-counter",
		Short:              "A counter actor exercising snapshot, resume, and atenet ingress",
		WorkerPoolManifest: "demos/counter/counter.yaml.tmpl",
		Deployments:        []steps.TemplateRef{{Atespace: namespace, Name: "counter"}},
		Templates: []demos.SubstrateTemplate{{
			Manifest: "demos/counter/counter-template.yaml.tmpl",
			Ref:      resources.ActorTemplateRef{Atespace: namespace, Name: "counter"},
		}},
		RenderValues: d.renderValues,
	}
	demos.Register(d)
}

func (d *demo) Flags(fs *pflag.FlagSet) {
	fs.BoolVar(&d.withExternalVolume, "with-external-volume", false,
		"Attach an external volume and validate a pre-seeded file on it (run \"setup csi\" first)")
	fs.StringVar(&d.storageClass, "storage-class", defaultStorageClass,
		"StorageClass backing the external volume, e.g. csi-nfs-sc or csi-hostpath-sc. Must be used --with-external-volume=true.")
}

// renderValues opts the template's external-volume lines in when the flag is
// set; with no values they are dropped.
func (d *demo) renderValues(_ context.Context, e *steps.Env) (map[string]string, error) {
	if !d.withExternalVolume {
		return nil, nil
	}
	return d.externalVolumeValues(e), nil
}
