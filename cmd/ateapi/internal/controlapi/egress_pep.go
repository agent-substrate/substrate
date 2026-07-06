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

package controlapi

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/agent-substrate/substrate/internal/egress"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

type egressPEPGateway struct {
	namespace string
	name      string
	address   string
	labels    map[string]string
}

func resolveEgressPEPAddress(ctx context.Context, gatewayIndexer cache.Indexer, atespace, actorID string) (string, error) {
	if gatewayIndexer == nil {
		return "", nil
	}

	var candidates []egressPEPGateway
	for _, obj := range gatewayIndexer.List() {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return "", fmt.Errorf("egress PEP cache contained %T, want *unstructured.Unstructured", obj)
		}
		pep, ok, err := egressPEPGatewayFromUnstructured(u)
		if err != nil {
			return "", err
		}
		if !ok {
			continue
		}
		// An actor-scoped PEP needs both labels: the actor label alone scores 0
		// for every actor (egressPEPGatewayScore requires the atespace to match
		// too), so the Gateway silently matches nothing. Warn instead of failing
		// so the actor still falls back to the next-best PEP.
		if pep.labels[egress.LabelActor] != "" && pep.labels[egress.LabelAtespace] == "" {
			slog.WarnContext(ctx, "Egress PEP Gateway has an actor label but no atespace label; it can never match any actor",
				"gateway", pep.namespace+"/"+pep.name)
		}
		candidates = append(candidates, pep)
	}

	bestScore := 0
	var best *egressPEPGateway
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].namespace != candidates[j].namespace {
			return candidates[i].namespace < candidates[j].namespace
		}
		return candidates[i].name < candidates[j].name
	})
	for i := range candidates {
		score := egressPEPGatewayScore(candidates[i].labels, atespace, actorID)
		if score > bestScore {
			bestScore = score
			best = &candidates[i]
		}
	}
	if best == nil {
		return "", nil
	}
	return best.address, nil
}

func egressPEPGatewayScore(labels map[string]string, atespace, actorID string) int {
	if _, ok := labels[egress.LabelPEP]; !ok {
		return 0
	}

	pepAtespace := labels[egress.LabelAtespace]
	pepActor := labels[egress.LabelActor]
	switch {
	case pepActor != "":
		if pepActor == actorID && pepAtespace == atespace {
			return 3
		}
	case pepAtespace != "":
		if pepAtespace == atespace {
			return 2
		}
	default:
		return 1
	}
	return 0
}

func egressPEPGatewayFromUnstructured(u *unstructured.Unstructured) (egressPEPGateway, bool, error) {
	labels := u.GetLabels()
	if _, ok := labels[egress.LabelPEP]; !ok {
		return egressPEPGateway{}, false, nil
	}

	// Only Programmed Gateways are candidates: an unprovisioned dataplane has no
	// working address, and selecting it would hand ateom a dead PEP. Skipping
	// (rather than erroring) lets resolution fall back to the next-best PEP
	// while a Gateway is still being reconciled.
	programmed, err := gatewayProgrammed(u)
	if err != nil {
		return egressPEPGateway{}, false, fmt.Errorf("egress PEP Gateway %s/%s: %w", u.GetNamespace(), u.GetName(), err)
	}
	if !programmed {
		return egressPEPGateway{}, false, nil
	}

	port, ok, err := gatewayHTTPListenerPort(u)
	if err != nil {
		return egressPEPGateway{}, false, fmt.Errorf("egress PEP Gateway %s/%s: %w", u.GetNamespace(), u.GetName(), err)
	}
	if !ok {
		return egressPEPGateway{}, false, fmt.Errorf("egress PEP Gateway %s/%s has no HTTP listener", u.GetNamespace(), u.GetName())
	}

	// Prefer the address the Gateway implementation published in
	// status.addresses; fall back to the agentgateway convention of a Service
	// named after the Gateway when the implementation publishes none (e.g.
	// agentgateway on kind, where the LoadBalancer address stays pending).
	host, err := gatewayStatusAddress(u)
	if err != nil {
		return egressPEPGateway{}, false, fmt.Errorf("egress PEP Gateway %s/%s: %w", u.GetNamespace(), u.GetName(), err)
	}
	if host == "" {
		host = fmt.Sprintf("%s.%s.svc.cluster.local", u.GetName(), u.GetNamespace())
	}

	return egressPEPGateway{
		namespace: u.GetNamespace(),
		name:      u.GetName(),
		address:   net.JoinHostPort(host, strconv.FormatInt(port, 10)),
		labels:    labels,
	}, true, nil
}

// gatewayProgrammed reports whether the Gateway has condition Programmed=True.
func gatewayProgrammed(u *unstructured.Unstructured) (bool, error) {
	conditions, ok, err := unstructured.NestedSlice(u.Object, "status", "conditions")
	if err != nil {
		return false, fmt.Errorf("read status.conditions: %w", err)
	}
	if !ok {
		return false, nil
	}
	for _, condition := range conditions {
		m, ok := condition.(map[string]any)
		if !ok {
			return false, fmt.Errorf("condition had type %T, want map[string]any", condition)
		}
		conditionType, _, err := unstructured.NestedString(m, "type")
		if err != nil {
			return false, fmt.Errorf("read condition type: %w", err)
		}
		if conditionType != "Programmed" {
			continue
		}
		conditionStatus, _, err := unstructured.NestedString(m, "status")
		if err != nil {
			return false, fmt.Errorf("read condition status: %w", err)
		}
		return conditionStatus == "True", nil
	}
	return false, nil
}

// gatewayStatusAddress returns the first address the Gateway implementation
// published in status.addresses, or "" if none is published.
func gatewayStatusAddress(u *unstructured.Unstructured) (string, error) {
	addresses, ok, err := unstructured.NestedSlice(u.Object, "status", "addresses")
	if err != nil {
		return "", fmt.Errorf("read status.addresses: %w", err)
	}
	if !ok {
		return "", nil
	}
	for _, address := range addresses {
		m, ok := address.(map[string]any)
		if !ok {
			return "", fmt.Errorf("address had type %T, want map[string]any", address)
		}
		value, _, err := unstructured.NestedString(m, "value")
		if err != nil {
			return "", fmt.Errorf("read address value: %w", err)
		}
		if value != "" {
			return value, nil
		}
	}
	return "", nil
}

func gatewayHTTPListenerPort(u *unstructured.Unstructured) (int64, bool, error) {
	listeners, ok, err := unstructured.NestedSlice(u.Object, "spec", "listeners")
	if err != nil {
		return 0, false, fmt.Errorf("read spec.listeners: %w", err)
	}
	if !ok {
		return 0, false, nil
	}
	for _, listener := range listeners {
		m, ok := listener.(map[string]any)
		if !ok {
			return 0, false, fmt.Errorf("listener had type %T, want map[string]any", listener)
		}
		protocol, _, err := unstructured.NestedString(m, "protocol")
		if err != nil {
			return 0, false, fmt.Errorf("read listener protocol: %w", err)
		}
		if !strings.EqualFold(protocol, "HTTP") {
			continue
		}
		port, ok, err := listenerPort(m)
		if err != nil {
			return 0, false, err
		}
		if !ok {
			return 0, false, fmt.Errorf("HTTP listener has no port")
		}
		return port, true, nil
	}
	return 0, false, nil
}

func listenerPort(listener map[string]any) (int64, bool, error) {
	port, ok := listener["port"]
	if !ok {
		return 0, false, nil
	}
	switch v := port.(type) {
	case int64:
		return v, true, nil
	case int32:
		return int64(v), true, nil
	case int:
		return int64(v), true, nil
	case float64:
		return int64(v), true, nil
	default:
		return 0, false, fmt.Errorf("HTTP listener port had type %T, want integer", port)
	}
}
