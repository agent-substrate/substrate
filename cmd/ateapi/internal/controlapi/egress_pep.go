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
	"fmt"
	"net"

	"github.com/agent-substrate/substrate/internal/egress"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
	netutils "k8s.io/utils/net"
)

// resolveEgressPEPAddress selects the egress PEP address for an actor by
// consumer-driven precedence, following the Istio ambient istio.io/use-waypoint
// model: the actor's own selector wins, then the atespace's, then the global
// default. Each tier supplies the PEP address directly as "<host>:<port>" via
// the ate.dev/use-egress-pep label (the global default comes from the
// --default-egress-pep flag).
//
// ate-api has no dependency on the Gateway API: it does not look up, watch, or
// validate any Gateway resource. The selected address is passed to ateom as
// given. An empty result means no PEP selected and egress capture stays off. A
// malformed address is a configuration error and fails resolution loudly.
func resolveEgressPEPAddress(actor *ateapipb.Actor, atespace *ateapipb.Atespace, defaultAddr string) (string, error) {
	tiers := []struct {
		source  string
		address string
	}{
		{"actor", actor.GetLabels()[egress.LabelUseEgressPEP]},
		{"atespace", atespace.GetLabels()[egress.LabelUseEgressPEP]},
		{"global", defaultAddr},
	}

	for _, tier := range tiers {
		if tier.address == "" {
			continue
		}
		if err := validatePEPAddress(tier.address); err != nil {
			return "", fmt.Errorf("%s egress PEP: %w", tier.source, err)
		}
		return tier.address, nil
	}
	return "", nil
}

// validateEgressPEPSelector validates the ate.dev/use-egress-pep selector label
// (if present and non-empty) parses as "<host>:<port>". An absent key means no
// selector; an explicit empty value also means no selector — resolution skips
// empty tiers, and on UpdateActor an empty value deletes the key.
func validateEgressPEPSelector(labels map[string]string, fldPath *field.Path) field.ErrorList {
	address := labels[egress.LabelUseEgressPEP]
	if address == "" {
		return nil
	}
	if err := validatePEPAddress(address); err != nil {
		return field.ErrorList{field.Invalid(fldPath.Key(egress.LabelUseEgressPEP), address, err.Error())}
	}
	return nil
}

const (
	// maxLabels bounds the number of selector-label entries on an actor or
	// atespace, mirroring maxSelectorMatchLabels for worker selectors.
	maxLabels = 10
	// maxLabelValueLength bounds label values. Values are not restricted to the
	// Kubernetes label-value charset because the egress PEP selector carries a
	// "<host>:<port>" address; the cap allows a maximal DNS name (253) plus a
	// colon and port with margin.
	maxLabelValueLength = 320
)

// validateLabels bounds a request's selector-label map: entry count, Kubernetes
// label-key syntax, and value length. Value contents beyond length are only
// checked for keys with dedicated validators (validateEgressPEPSelector).
func validateLabels(labels map[string]string, fldPath *field.Path) field.ErrorList {
	var errs field.ErrorList
	if n := len(labels); n > maxLabels {
		return field.ErrorList{field.TooMany(fldPath, n, maxLabels)}
	}
	for k, v := range labels {
		for _, msg := range content.IsLabelKey(k) {
			errs = append(errs, field.Invalid(fldPath.Key(k), k, msg))
		}
		if len(v) > maxLabelValueLength {
			errs = append(errs, field.TooLong(fldPath.Key(k), v, maxLabelValueLength))
		}
	}
	return errs
}

// ValidateDefaultEgressPEPAddress validates the global-default egress PEP address
// (the --default-egress-pep flag) at boot. Empty is allowed (no global default).
// A malformed non-empty address is a configuration error the caller surfaces so
// a typo does not silently reach ateom and fail every actor resume that falls
// through to the global tier; ate-api logs it and degrades to no global default.
func ValidateDefaultEgressPEPAddress(address string) error {
	if address == "" {
		return nil
	}
	return validatePEPAddress(address)
}

// normalizeLabels returns a copy of labels without empty-valued entries (an
// empty value means "no selector"), or nil when nothing remains. Create paths
// use it so an explicit empty selector is stored the same as an absent one.
func normalizeLabels(labels map[string]string) map[string]string {
	var out map[string]string
	for k, v := range labels {
		if v == "" {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[k] = v
	}
	return out
}

// validatePEPAddress checks that an egress PEP address is a "<host>:<port>" pair
// with a numeric port in range. ate-api does not verify the host resolves or
// that a Gateway actually serves it — the address is used as given.
func validatePEPAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf("egress PEP address %q must be in the form <host>:<port>", address)
	}
	// ParsePort rejects sign prefixes ("+80") and out-of-range ports that
	// SplitHostPort passes through unchecked.
	if _, err := netutils.ParsePort(port, false); err != nil {
		return fmt.Errorf("egress PEP address %q has an invalid port %q", address, port)
	}
	return nil
}
