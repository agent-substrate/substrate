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

func TestPrintExternalVolumeClasses(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinNow(t, now)

	fastSSD := &ateapi.ExternalVolumeClass{
		Metadata:   &ateapi.ResourceMetadata{Name: "fast-ssd", Uid: "u-1", Version: 1, CreateTime: timestamppb.New(now.Add(-10 * time.Minute))},
		DriverName: "pd.csi.storage.gke.io",
		Parameters: map[string]string{"type": "pd-ssd", "replication-type": "none"},
	}
	standardRWO := &ateapi.ExternalVolumeClass{
		Metadata:   &ateapi.ResourceMetadata{Name: "standard-rwo", CreateTime: timestamppb.New(now.Add(-5 * time.Hour))},
		DriverName: "pd.csi.storage.gke.io",
	}

	var buf bytes.Buffer
	if err := PrintExternalVolumeClassesTo(&buf, []*ateapi.ExternalVolumeClass{standardRWO, fastSSD}, "table"); err != nil {
		t.Fatalf("PrintExternalVolumeClassesTo(table) error = %v", err)
	}
	wantTable := "NAME           DRIVER                  PARAMETERS                          AGE\n" +
		"fast-ssd       pd.csi.storage.gke.io   replication-type=none,type=pd-ssd   10m\n" +
		"standard-rwo   pd.csi.storage.gke.io   -                                   5h\n"
	if diff := cmp.Diff(wantTable, buf.String()); diff != "" {
		t.Errorf("table mismatch (-want +got):\n%s", diff)
	}

	for _, tc := range []struct {
		name, format string
		single       bool
		wantSub      []string
		wantErr      bool
	}{
		{name: "list json", format: "json", wantSub: []string{`"externalVolumeClasses"`, `"driverName": "pd.csi.storage.gke.io"`}},
		{name: "list yaml", format: "yaml", wantSub: []string{"externalVolumeClasses:", "driverName: pd.csi.storage.gke.io"}},
		{name: "single yaml bare object", single: true, format: "yaml", wantSub: []string{"driverName: pd.csi.storage.gke.io", "uid: u-1", `version: "1"`}},
		{name: "single table", single: true, format: "table", wantSub: []string{"NAME", "fast-ssd   pd.csi.storage.gke.io   replication-type=none,type=pd-ssd   10m"}},
		{name: "unsupported format", single: true, format: "xml", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := PrintExternalVolumeClassesTo(&out, []*ateapi.ExternalVolumeClass{fastSSD}, tc.format)
			if tc.single {
				out.Reset()
				err = PrintExternalVolumeClassTo(&out, fastSSD, tc.format)
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
