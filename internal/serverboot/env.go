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

package serverboot

import "github.com/agent-substrate/substrate/internal/env"

const (
	tracesSamplerEnv    = "OTEL_TRACES_SAMPLER"
	tracesSamplerArgEnv = "OTEL_TRACES_SAMPLER_ARG"
)

var traceSampler = env.Var[string]{
	Name:    tracesSamplerEnv,
	Default: "",
	Description: `Overrides trace sampling. Accepted names: always_on, always_off, traceidratio,
parentbased_always_on, parentbased_always_off, parentbased_traceidratio. Names are case-insensitive
and surrounding whitespace is ignored. Ratio samplers require OTEL_TRACES_SAMPLER_ARG.
Unset, empty, or invalid settings keep the parent-based defaults: 10% for control-plane components
and ateoms, 1% for atenet router, and no root sampling for glutton.`,
}

var traceSamplerArg = env.Var[string]{
	Name:        tracesSamplerArgEnv,
	Default:     "",
	Description: "Ratio in [0, 1] for traceidratio and parentbased_traceidratio samplers. Missing or invalid ratios keep the component default; other samplers ignore it.",
}
