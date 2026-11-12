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

package csi

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	atelisters "go.ate.dev/substrate/pkg/client/listers/ate/v1"
	"google.golang.org/protobuf/testing/protocmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

func TestFromK8sCSIDriverConfig(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		in   *atev1.CSIDriverConfig
		want *ateapi.CsiDriverConfig
	}{
		{name: "nil input", in: nil, want: nil},
		{
			name: "full fields with TLS and numeric ResourceVersion",
			in: &atev1.CSIDriverConfig{
				ObjectMeta: metav1.ObjectMeta{Name: "csi-nfs", UID: types.UID("uid-123"), ResourceVersion: "42"},
				Spec: atev1.CSIDriverConfigSpec{
					DriverName:         "nfs.csi.k8s.io",
					ControllerEndpoint: "tcp://csi-nfs.kube-system.svc:50052",
					NodeSocketOverride: "unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock",
					TLS:                &atev1.CSIDriverTLSConfig{Enabled: true, UsePodIdentity: true, ServerName: "csi-nfs.kube-system.svc"},
				},
			},
			want: &ateapi.CsiDriverConfig{
				Metadata:           &ateapi.ResourceMetadata{Name: "csi-nfs", Uid: "uid-123", Version: 42},
				DriverName:         "nfs.csi.k8s.io",
				ControllerEndpoint: "tcp://csi-nfs.kube-system.svc:50052",
				NodeSocketOverride: "unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock",
				Tls:                &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "csi-nfs.kube-system.svc"},
			},
		},
		{
			name: "non-numeric ResourceVersion leaves version zero",
			in: &atev1.CSIDriverConfig{
				ObjectMeta: metav1.ObjectMeta{Name: "pd.csi.storage.gke.io", UID: types.UID("uid-pd"), ResourceVersion: "rv-opaque"},
				Spec:       atev1.CSIDriverConfigSpec{DriverName: "pd.csi.storage.gke.io", ControllerEndpoint: "dns:///pd-csi:9000"},
			},
			want: &ateapi.CsiDriverConfig{
				Metadata:           &ateapi.ResourceMetadata{Name: "pd.csi.storage.gke.io", Uid: "uid-pd"},
				DriverName:         "pd.csi.storage.gke.io",
				ControllerEndpoint: "dns:///pd-csi:9000",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FromK8sCSIDriverConfig(tc.in)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Fatalf("FromK8sCSIDriverConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type fakeK8sCSIDriverConfigLister struct {
	items   []*atev1.CSIDriverConfig
	listErr error
}

func (f *fakeK8sCSIDriverConfigLister) List(labels.Selector) ([]*atev1.CSIDriverConfig, error) {
	return f.items, f.listErr
}

func (f *fakeK8sCSIDriverConfigLister) Get(name string) (*atev1.CSIDriverConfig, error) {
	for _, item := range f.items {
		if item != nil && item.Name == name {
			return item, nil
		}
	}
	return nil, fmt.Errorf("csidriverconfig.ate.dev %q not found", name)
}

var _ atelisters.CSIDriverConfigLister = (*fakeK8sCSIDriverConfigLister)(nil)

func TestNewK8sCSIDriverConfigGetter(t *testing.T) {
	t.Parallel()

	sameNameCRD := &atev1.CSIDriverConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "nfs.csi.k8s.io", UID: types.UID("u-same")},
		Spec:       atev1.CSIDriverConfigSpec{DriverName: "nfs.csi.k8s.io", ControllerEndpoint: "tcp://127.0.0.1:9001"},
	}
	diffNameCRD := &atev1.CSIDriverConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "pd-custom", UID: types.UID("u-diff")},
		Spec:       atev1.CSIDriverConfigSpec{DriverName: "pd.csi.storage.gke.io", ControllerEndpoint: "tcp://127.0.0.1:9002"},
	}
	mismatchedSameNameCRD := &atev1.CSIDriverConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "pd.csi.storage.gke.io", UID: types.UID("u-wrong")},
		Spec:       atev1.CSIDriverConfigSpec{DriverName: "other.csi.io", ControllerEndpoint: "tcp://127.0.0.1:9999"},
	}
	listBoom := errors.New("lister failed")

	for _, tc := range []struct {
		name       string
		lister     atelisters.CSIDriverConfigLister
		driverName string
		want       *ateapi.CsiDriverConfig
		wantErr    string
	}{
		{name: "nil lister", lister: nil, driverName: "nfs.csi.k8s.io", wantErr: "missing csiDriverConfigLister"},
		{
			name:       "metadata.name matches driverName",
			lister:     &fakeK8sCSIDriverConfigLister{items: []*atev1.CSIDriverConfig{sameNameCRD}},
			driverName: "nfs.csi.k8s.io",
			want:       FromK8sCSIDriverConfig(sameNameCRD),
		},
		{
			name:       "metadata.name differs from spec.driverName",
			lister:     &fakeK8sCSIDriverConfigLister{items: []*atev1.CSIDriverConfig{diffNameCRD}},
			driverName: "pd.csi.storage.gke.io",
			want:       FromK8sCSIDriverConfig(diffNameCRD),
		},
		{
			name:       "metadata.name collision with mismatched spec.driverName skips to matching spec.driverName",
			lister:     &fakeK8sCSIDriverConfigLister{items: []*atev1.CSIDriverConfig{mismatchedSameNameCRD, diffNameCRD}},
			driverName: "pd.csi.storage.gke.io",
			want:       FromK8sCSIDriverConfig(diffNameCRD),
		},
		{
			name:       "metadata.name collision with no matching spec.driverName returns not found",
			lister:     &fakeK8sCSIDriverConfigLister{items: []*atev1.CSIDriverConfig{mismatchedSameNameCRD}},
			driverName: "pd.csi.storage.gke.io",
			wantErr:    `CSIDriverConfig for driver "pd.csi.storage.gke.io" not found`,
		},
		{
			name:       "list error after Get miss joins both errors",
			lister:     &fakeK8sCSIDriverConfigLister{listErr: listBoom},
			driverName: "missing.csi.io",
			wantErr:    "lister failed",
		},
		{
			name:       "list error after mismatched Get returns listErr",
			lister:     &fakeK8sCSIDriverConfigLister{items: []*atev1.CSIDriverConfig{mismatchedSameNameCRD}, listErr: listBoom},
			driverName: "pd.csi.storage.gke.io",
			wantErr:    "lister failed",
		},
		{
			name:       "not found returns lister Get error",
			lister:     &fakeK8sCSIDriverConfigLister{items: []*atev1.CSIDriverConfig{sameNameCRD}},
			driverName: "missing.csi.io",
			wantErr:    `csidriverconfig.ate.dev "missing.csi.io" not found`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NewK8sCSIDriverConfigGetter(tc.lister).Get(t.Context(), tc.driverName)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Get(%q) error = %v, want substring %q", tc.driverName, err, tc.wantErr)
				}
				return
			}
			if err != nil || cmp.Diff(tc.want, got, protocmp.Transform()) != "" {
				t.Fatalf("Get(%q) = (%v, %v), want %v", tc.driverName, got, err, tc.want)
			}
		})
	}
}
