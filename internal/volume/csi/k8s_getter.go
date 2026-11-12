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
	"context"
	"errors"
	"fmt"
	"strconv"

	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	atelisters "go.ate.dev/substrate/pkg/client/listers/ate/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// FromK8sCSIDriverConfig converts a Kubernetes CSIDriverConfig CRD object to
// a CsiDriverConfig protobuf message.
func FromK8sCSIDriverConfig(crd *atev1.CSIDriverConfig) *ateapi.CsiDriverConfig {
	if crd == nil {
		return nil
	}
	meta := &ateapi.ResourceMetadata{
		Name: crd.Name,
		Uid:  string(crd.UID),
	}
	if v, err := strconv.ParseInt(crd.ResourceVersion, 10, 64); err == nil && v > 0 {
		meta.Version = v
	}
	out := &ateapi.CsiDriverConfig{
		Metadata:           meta,
		DriverName:         crd.Spec.DriverName,
		ControllerEndpoint: crd.Spec.ControllerEndpoint,
		NodeSocketOverride: crd.Spec.NodeSocketOverride,
	}
	if crd.Spec.TLS != nil {
		out.Tls = &ateapi.CsiDriverTlsConfig{
			Enabled:        crd.Spec.TLS.Enabled,
			UsePodIdentity: crd.Spec.TLS.UsePodIdentity,
			ServerName:     crd.Spec.TLS.ServerName,
		}
	}
	return out
}

type k8sCSIDriverConfigGetter struct {
	lister atelisters.CSIDriverConfigLister
}

// NewK8sCSIDriverConfigGetter adapts a Kubernetes CSIDriverConfigLister to CSIDriverConfigGetter.
func NewK8sCSIDriverConfigGetter(lister atelisters.CSIDriverConfigLister) CSIDriverConfigGetter {
	return &k8sCSIDriverConfigGetter{lister: lister}
}

func (g *k8sCSIDriverConfigGetter) Get(_ context.Context, driverName string) (*ateapi.CsiDriverConfig, error) {
	if g.lister == nil {
		return nil, errors.New("missing csiDriverConfigLister")
	}
	crd, err := g.lister.Get(driverName)
	if err == nil && crd != nil && (crd.Spec.DriverName == "" || crd.Spec.DriverName == driverName) {
		return FromK8sCSIDriverConfig(crd), nil
	}
	crds, listErr := g.lister.List(labels.Everything())
	if listErr != nil {
		if err != nil {
			return nil, errors.Join(err, listErr)
		}
		return nil, listErr
	}
	for _, item := range crds {
		if item != nil && item.Spec.DriverName == driverName {
			return FromK8sCSIDriverConfig(item), nil
		}
	}
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("CSIDriverConfig for driver %q not found", driverName)
}
