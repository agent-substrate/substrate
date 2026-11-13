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

package printer

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPrintCsiDriverConfigs(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinNow(t, now)

	csiNFS := &ateapi.CsiDriverConfig{
		Metadata:           &ateapi.ResourceMetadata{Name: "csi-nfs", Uid: "u-1", Version: 1, CreateTime: timestamppb.New(now.Add(-10 * time.Minute))},
		DriverName:         "nfs.csi.k8s.io",
		ControllerEndpoint: "tcp://csi-nfs.kube-system.svc:50052",
		NodeSocketOverride: "unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock",
		Tls:                &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "csi-nfs.kube-system.svc"},
	}
	csiPD := &ateapi.CsiDriverConfig{
		Metadata:           &ateapi.ResourceMetadata{Name: "csi-pd", CreateTime: timestamppb.New(now.Add(-5 * time.Hour))},
		DriverName:         "pd.csi.storage.gke.io",
		ControllerEndpoint: "dns:///csi-pd.gke-system.svc:9000",
	}
	csiCustomTLS := &ateapi.CsiDriverConfig{
		Metadata:           &ateapi.ResourceMetadata{Name: "csi-custom", CreateTime: timestamppb.New(now.Add(-30 * time.Second))},
		DriverName:         "custom.csi.example.com",
		ControllerEndpoint: "tcp://10.0.0.10:9000",
		Tls:                &ateapi.CsiDriverTlsConfig{Enabled: true},
	}

	var buf bytes.Buffer
	if err := PrintCsiDriverConfigsTo(&buf, []*ateapi.CsiDriverConfig{csiPD, csiNFS, csiCustomTLS}, "table"); err != nil {
		t.Fatalf("PrintCsiDriverConfigsTo(table) error = %v", err)
	}
	wantTable := "NAME         DRIVER                   CONTROLLER-ENDPOINT                   NODE-SOCKET-OVERRIDE                                     TLS                   AGE\n" +
		"csi-custom   custom.csi.example.com   tcp://10.0.0.10:9000                  -                                                        enabled               30s\n" +
		"csi-nfs      nfs.csi.k8s.io           tcp://csi-nfs.kube-system.svc:50052   unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock   mtls (pod-identity)   10m\n" +
		"csi-pd       pd.csi.storage.gke.io    dns:///csi-pd.gke-system.svc:9000     -                                                        disabled              5h\n"
	if diff := cmp.Diff(wantTable, buf.String()); diff != "" {
		t.Errorf("table mismatch (-want +got):\n%s", diff)
	}

	for _, tc := range []struct {
		name, format string
		single       bool
		wantSub      []string
		wantErr      bool
	}{
		{name: "list json", format: "json", wantSub: []string{`"csiDriverConfigs"`, `"driverName": "nfs.csi.k8s.io"`}},
		{name: "list yaml", format: "yaml", wantSub: []string{"csiDriverConfigs:", "driverName: nfs.csi.k8s.io"}},
		{name: "single json bare object", single: true, format: "json", wantSub: []string{`"driverName": "nfs.csi.k8s.io"`, `"uid": "u-1"`}},
		{name: "single yaml bare object", single: true, format: "yaml", wantSub: []string{"driverName: nfs.csi.k8s.io", "uid: u-1", `version: "1"`}},
		{name: "single table", single: true, format: "table", wantSub: []string{"NAME", "csi-nfs   nfs.csi.k8s.io   tcp://csi-nfs.kube-system.svc:50052   unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock   mtls (pod-identity)   10m"}},
		{name: "unsupported list format", format: "xml", wantErr: true},
		{name: "unsupported single format", single: true, format: "xml", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			var err error
			if tc.single {
				err = PrintCsiDriverConfigTo(&out, csiNFS, tc.format)
			} else {
				err = PrintCsiDriverConfigsTo(&out, []*ateapi.CsiDriverConfig{csiNFS}, tc.format)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			for _, w := range tc.wantSub {
				if !strings.Contains(out.String(), w) {
					t.Errorf("output = %q, want substring %q", out.String(), w)
				}
			}
		})
	}
}
