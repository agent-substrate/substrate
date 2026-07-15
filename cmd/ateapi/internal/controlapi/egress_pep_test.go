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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/egress"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func actorWithPEP(address string) *ateapipb.Actor {
	a := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "space-a",
			Name:     "actor-a",
		},
	}
	if address != "" {
		a.Labels = map[string]string{egress.LabelUseEgressPEP: address}
	}
	return a
}

func atespaceWithPEP(address string) *ateapipb.Atespace {
	s := &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "space-a"}}
	if address != "" {
		s.Labels = map[string]string{egress.LabelUseEgressPEP: address}
	}
	return s
}

func TestResolveEgressPEPAddressNoSelector(t *testing.T) {
	got, err := resolveEgressPEPAddress(actorWithPEP(""), atespaceWithPEP(""), "")
	if err != nil {
		t.Fatalf("resolveEgressPEPAddress() error = %v", err)
	}
	if got != "" {
		t.Fatalf("resolveEgressPEPAddress() = %q, want empty", got)
	}
}

func TestResolveEgressPEPAddressPrefersActorThenAtespaceThenGlobal(t *testing.T) {
	const (
		actorPEP  = "ate-egress-actor.agentgateway-system.svc.cluster.local:15008"
		spacePEP  = "ate-egress-space.agentgateway-system.svc.cluster.local:15008"
		globalPEP = "ate-egress.agentgateway-system.svc.cluster.local:15008"
	)

	// Actor selector wins over atespace and global default.
	got, err := resolveEgressPEPAddress(actorWithPEP(actorPEP), atespaceWithPEP(spacePEP), globalPEP)
	if err != nil {
		t.Fatalf("resolveEgressPEPAddress() error = %v", err)
	}
	if got != actorPEP {
		t.Fatalf("resolveEgressPEPAddress() = %q, want %q", got, actorPEP)
	}

	// No actor selector: atespace wins over global default.
	got, err = resolveEgressPEPAddress(actorWithPEP(""), atespaceWithPEP(spacePEP), globalPEP)
	if err != nil {
		t.Fatalf("resolveEgressPEPAddress() error = %v", err)
	}
	if got != spacePEP {
		t.Fatalf("resolveEgressPEPAddress() = %q, want %q", got, spacePEP)
	}

	// No actor or atespace selector: global default is used.
	got, err = resolveEgressPEPAddress(actorWithPEP(""), atespaceWithPEP(""), globalPEP)
	if err != nil {
		t.Fatalf("resolveEgressPEPAddress() error = %v", err)
	}
	if got != globalPEP {
		t.Fatalf("resolveEgressPEPAddress() = %q, want %q", got, globalPEP)
	}
}

func TestResolveEgressPEPAddressMalformedErrors(t *testing.T) {
	if _, err := resolveEgressPEPAddress(actorWithPEP("no-port-here"), atespaceWithPEP(""), ""); err == nil {
		t.Fatal("resolveEgressPEPAddress() error = nil, want error for malformed address")
	}
}

func TestValidateDefaultEgressPEPAddress(t *testing.T) {
	// Empty is allowed: no global default configured.
	if err := ValidateDefaultEgressPEPAddress(""); err != nil {
		t.Fatalf("ValidateDefaultEgressPEPAddress(\"\") error = %v, want nil", err)
	}
	if err := ValidateDefaultEgressPEPAddress("ate-egress.agentgateway-system.svc.cluster.local:15008"); err != nil {
		t.Fatalf("ValidateDefaultEgressPEPAddress() valid address error = %v", err)
	}
	if err := ValidateDefaultEgressPEPAddress("no-port-here"); err == nil {
		t.Fatal("ValidateDefaultEgressPEPAddress() malformed address error = nil, want error")
	}
}

func TestValidateEgressPEPSelector(t *testing.T) {
	if errs := validateEgressPEPSelector(map[string]string{egress.LabelUseEgressPEP: "pep.example:15008"}, nil); len(errs) != 0 {
		t.Fatalf("validateEgressPEPSelector() valid address returned errors: %v", errs)
	}
	if errs := validateEgressPEPSelector(map[string]string{egress.LabelUseEgressPEP: "missing-port"}, nil); len(errs) == 0 {
		t.Fatal("validateEgressPEPSelector() malformed address returned no errors")
	}
	// A signed port must be rejected (ParsePort, unlike Atoi, refuses "+80").
	if errs := validateEgressPEPSelector(map[string]string{egress.LabelUseEgressPEP: "pep.example:+80"}, nil); len(errs) == 0 {
		t.Fatal("validateEgressPEPSelector() signed port returned no errors")
	}
	if errs := validateEgressPEPSelector(nil, nil); len(errs) != 0 {
		t.Fatalf("validateEgressPEPSelector() absent label returned errors: %v", errs)
	}
	// An explicit empty value means "no selector" (delete on UpdateActor) and
	// must be accepted.
	if errs := validateEgressPEPSelector(map[string]string{egress.LabelUseEgressPEP: ""}, nil); len(errs) != 0 {
		t.Fatalf("validateEgressPEPSelector() empty value returned errors: %v", errs)
	}
}

func TestValidateLabels(t *testing.T) {
	if errs := validateLabels(map[string]string{"team": "a", egress.LabelUseEgressPEP: "pep.example:15008"}, nil); len(errs) != 0 {
		t.Fatalf("validateLabels() valid labels returned errors: %v", errs)
	}
	if errs := validateLabels(map[string]string{"not a label key!": "v"}, nil); len(errs) == 0 {
		t.Fatal("validateLabels() invalid key returned no errors")
	}
	if errs := validateLabels(map[string]string{"k": strings.Repeat("v", maxLabelValueLength+1)}, nil); len(errs) == 0 {
		t.Fatal("validateLabels() oversized value returned no errors")
	}
	tooMany := map[string]string{}
	for i := 0; i <= maxLabels; i++ {
		tooMany[fmt.Sprintf("key-%d", i)] = "v"
	}
	if errs := validateLabels(tooMany, nil); len(errs) == 0 {
		t.Fatal("validateLabels() too many entries returned no errors")
	}
}

func TestNormalizeLabels(t *testing.T) {
	got := normalizeLabels(map[string]string{"keep": "v", "drop": ""})
	if len(got) != 1 || got["keep"] != "v" {
		t.Fatalf("normalizeLabels() = %v, want only the non-empty entry", got)
	}
	if got := normalizeLabels(map[string]string{"drop": ""}); got != nil {
		t.Fatalf("normalizeLabels() all-empty = %v, want nil", got)
	}
	if got := normalizeLabels(nil); got != nil {
		t.Fatalf("normalizeLabels(nil) = %v, want nil", got)
	}
}
