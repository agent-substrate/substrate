//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package controlapi

import (
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

func toAteletGpuSpec(r *v1alpha1.ContainerResources) *ateletpb.GpuSpec {
	if r == nil || r.GPU == nil {
		return nil
	}
	g := r.GPU
	return &ateletpb.GpuSpec{
		Count:              g.Count,
		Device:             g.Device,
		DriverCapabilities: g.DriverCapabilities,
		DriverVersion:      g.DriverVersion,
	}
}
