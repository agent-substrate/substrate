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
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
)

// manifestParkedRequestBudget is the budget the manifest's
// terminationGracePeriodSeconds is sized for: the router default.
const manifestParkedRequestBudget = 5 * time.Second

// configureAtenetRouter sets --route-timeout, --parked-request-budget and
// --extproc-max-requests on the atenet-router container and grows
// terminationGracePeriodSeconds with the budget, since the router's drain
// deadline follows it. A no-op when unset.
func (e *Env) configureAtenetRouter(objs []*unstructured.Unstructured) error {
	routeTimeout, budget, maxRequests := e.Cfg.RouterRouteTimeout, e.Cfg.RouterParkedRequestBudget, e.Cfg.RouterExtProcMaxRequests
	if routeTimeout <= 0 && budget <= 0 && maxRequests <= 0 {
		return nil
	}
	var dep *unstructured.Unstructured
	for _, obj := range objs {
		if obj.GetKind() == "Deployment" && obj.GetNamespace() == e.Namespace() && obj.GetName() == "atenet-router" {
			dep = obj
			break
		}
	}
	if dep == nil {
		return fmt.Errorf("the router manifest has no deployment/atenet-router in %s", e.Namespace())
	}

	containers, _, err := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
	if err != nil {
		return fmt.Errorf("%s: %w", kube.Describe(dep), err)
	}
	idx := -1
	for i, c := range containers {
		if m, ok := c.(map[string]any); ok && m["name"] == "atenet-router" {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("%s has no atenet-router container for the router flags", kube.Describe(dep))
	}
	container := containers[idx].(map[string]any)
	args, _, err := unstructured.NestedSlice(container, "args")
	if err != nil {
		return fmt.Errorf("%s: %w", kube.Describe(dep), err)
	}

	if routeTimeout > 0 {
		log.Infof("Setting the router's --route-timeout to %s", routeTimeout)
		args = setFlag(args, "--route-timeout", routeTimeout.String())
	}
	if budget > 0 {
		log.Infof("Setting the router's --parked-request-budget to %s", budget)
		args = setFlag(args, "--parked-request-budget", budget.String())
		if extra := budget - manifestParkedRequestBudget; extra > 0 {
			grace, err := gracePeriod(dep)
			if err != nil {
				return err
			}
			grace += int64(math.Ceil(extra.Seconds()))
			if err := unstructured.SetNestedField(dep.Object, grace, "spec", "template", "spec", "terminationGracePeriodSeconds"); err != nil {
				return fmt.Errorf("%s: %w", kube.Describe(dep), err)
			}
		}
	}
	if maxRequests > 0 {
		log.Infof("Setting the router's --extproc-max-requests to %d", maxRequests)
		args = setFlag(args, "--extproc-max-requests", strconv.Itoa(maxRequests))
	}
	container["args"] = args
	containers[idx] = container
	return unstructured.SetNestedSlice(dep.Object, containers, "spec", "template", "spec", "containers")
}

// setFlag replaces the --flag=value argument in args, or appends it.
func setFlag(args []any, flag, value string) []any {
	arg := flag + "=" + value
	for i, a := range args {
		if s, ok := a.(string); ok && strings.HasPrefix(s, flag+"=") {
			args[i] = arg
			return args
		}
	}
	return append(args, arg)
}

// gracePeriod reads terminationGracePeriodSeconds; YAML decodes it as float64, JSON as int64.
func gracePeriod(dep *unstructured.Unstructured) (int64, error) {
	raw, found, err := unstructured.NestedFieldNoCopy(dep.Object, "spec", "template", "spec", "terminationGracePeriodSeconds")
	if err != nil {
		return 0, fmt.Errorf("%s: %w", kube.Describe(dep), err)
	}
	if !found {
		return 30, nil // the Kubernetes default
	}
	switch v := raw.(type) {
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("%s: terminationGracePeriodSeconds is %T, want a number", kube.Describe(dep), raw)
	}
}
