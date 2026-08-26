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

package egress

import (
	"os"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

// The contract between the egress gateway and a cluster-local origin. The
// gateway tells the origin which actor a request belongs to in actorHeader,
// and the origin may believe it only because the gateway proved who it was in
// the handshake that carried it. Both halves live in the Envoy bootstrap, so
// the compiler sees neither; this file is what holds them together.
const (
	actorHeader = "x-ate-actor"

	// The filter state the CONNECT listener populates from the verified actor
	// client certificate. Written on the outer leg and shared upstream, it is
	// the only actor identity on the MITM leg the actor did not author.
	actorIdentityFilterState = "ate.actor.identity"

	// The gateway's own credential, which authenticates it to the origin, and
	// the trust bundle that signs in-cluster service DNS names.
	podIdentityBundle   = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"
	serviceDNSTrustFile = "/run/servicedns.podcert.ate.dev/trust-bundle.pem"
)

// internalVirtualHost is the one virtual host allowed to state an actor
// identity to an origin, and internalClusters are the upstreams it may use.
const internalVirtualHost = "internal"

var (
	internalClusters = []string{"egress_forward_proxy_internal", "egress_forward_proxy_internal_grpc"}
	// Only what the servicedns signer certifies. auto_san_validation on the
	// internal clusters checks the dialed name against the origin's SANs, and
	// the signer issues <service>.<namespace>.svc alone, so a longer suffix
	// here would fail the handshake rather than extend the feature to it.
	internalDomains = []string{"*.svc"}
)

// TestEgressManifestReservesTheActorHeader checks that x-ate-actor means the
// same thing everywhere it can reach an origin: an actor identity this gateway
// authenticated and then vouched for over mTLS.
//
// The failure this guards is quiet. A tunneled request is the actor's to
// compose, so an actor can send x-ate-actor itself, naming any actor it likes.
// Every route out of the MITM listener therefore has to either overwrite the
// header with the authenticated value or drop it — a virtual host that does
// neither forwards the actor's own claim to an origin that has been told to
// trust the header, and nothing about the request looks wrong.
func TestEgressManifestReservesTheActorHeader(t *testing.T) {
	for _, rc := range mitmRouteConfigs(t, egressBootstrap(t)) {
		for _, vh := range rc.VirtualHosts {
			where := rc.Name + "/" + vh.Name
			added := vh.added(actorHeader)
			removed := slices.Contains(vh.RequestHeadersToRemove, actorHeader)

			switch {
			case vh.Name == internalVirtualHost:
				if added == nil {
					t.Errorf("virtual host %s adds no %s; it is the one host that routes to an origin authenticated to read it, and without the header that origin cannot attribute the request at all", where, actorHeader)
					continue
				}
				// OVERWRITE_IF_EXISTS_OR_ADD, not APPEND: appending would
				// leave the actor's own value in place alongside this one and
				// let the origin read either.
				if got := added.AppendAction; got != "OVERWRITE_IF_EXISTS_OR_ADD" {
					t.Errorf("virtual host %s adds %s with append_action %q, so an actor that sends the header itself keeps its own value; it must be OVERWRITE_IF_EXISTS_OR_ADD", where, actorHeader, got)
				}
				// An empty value is worse than none: the origin cannot tell
				// "no actor" from "an actor whose name did not render".
				if added.KeepEmptyValue {
					t.Errorf("virtual host %s sets keep_empty_value on %s; an unresolved identity must be sent as no header rather than as an empty one", where, actorHeader)
				}
				if want := "%FILTER_STATE(" + actorIdentityFilterState + ":PLAIN)%"; added.Header.Value != want {
					t.Errorf("virtual host %s sets %s to %q, which is not the identity the CONNECT listener authenticated; want %q", where, actorHeader, added.Header.Value, want)
				}
				// Cluster-local suffixes only. A "*" or a bare-hostname
				// pattern here would silently widen the set of origins that
				// are told an actor identity, and widen it to origins this
				// gateway holds no credential for.
				if want := internalDomains; !slices.Equal(vh.Domains, want) {
					t.Errorf("virtual host %s matches %v; it decides which origins are treated as in-cluster and must match exactly %v", where, vh.Domains, want)
				}

			case added != nil:
				t.Errorf("virtual host %s adds %s, but only %q reaches origins that authenticate this gateway; anywhere else the header is a claim the origin cannot check", where, actorHeader, internalVirtualHost)

			case !removed:
				t.Errorf("virtual host %s neither adds nor removes %s, so an actor's own %s is forwarded to the origin looking platform-issued; list it in request_headers_to_remove", where, actorHeader, actorHeader)
			}
		}
	}
}

// TestEgressManifestBacksTheActorHeaderWithMTLS checks the other half: the
// internal virtual host routes only to clusters that present the gateway's
// podidentity credential. Without a client certificate the header is a string
// anything on the pod network could have sent, and an origin configured to
// trust it would be trusting exactly that.
func TestEgressManifestBacksTheActorHeaderWithMTLS(t *testing.T) {
	bootstrap := egressBootstrap(t)

	for _, rc := range mitmRouteConfigs(t, bootstrap) {
		for _, vh := range rc.VirtualHosts {
			for _, r := range vh.Routes {
				internal := slices.Contains(internalClusters, r.Route.Cluster)
				if internal != (vh.Name == internalVirtualHost) {
					t.Errorf("virtual host %s/%s routes to cluster %q; the mTLS clusters %v and the virtual host that adds %s have to be the same set, or a request reaches an origin over a channel that does not match what the header claims", rc.Name, vh.Name, r.Route.Cluster, internalClusters, actorHeader)
				}
			}
		}
	}

	for _, name := range internalClusters {
		tls := findCluster(t, bootstrap, name).TransportSocket.TypedConfig.CommonTLSContext
		if len(tls.TLSCertificates) == 0 {
			t.Errorf("cluster %s presents no client certificate, so the origin cannot tell this gateway from any other client and %s becomes unverifiable", name, actorHeader)
			continue
		}
		for _, cert := range tls.TLSCertificates {
			if cert.CertificateChain.Filename != podIdentityBundle || cert.PrivateKey.Filename != podIdentityBundle {
				t.Errorf("cluster %s presents %s/%s; the origin verifies this gateway against the podidentity trust bundle, so it must present %s", name, cert.CertificateChain.Filename, cert.PrivateKey.Filename, podIdentityBundle)
			}
		}
		// The public roots do not sign cluster-local service DNS names, so
		// pointing this elsewhere does not weaken the handshake, it breaks it.
		if got := tls.ValidationContext.TrustedCA.Filename; got != serviceDNSTrustFile {
			t.Errorf("cluster %s validates the origin against %q; in-cluster service DNS names are signed by the servicedns signer, so it must be %s", name, got, serviceDNSTrustFile)
		}
	}
}

// The slice of the Envoy bootstrap these tests read. Deliberately partial:
// unmarshalling into the full xDS types would need the manifest to be valid
// discovery config, and what is under test is a handful of fields.

type bootstrap struct {
	StaticResources struct {
		Listeners []listener `json:"listeners"`
		Clusters  []cluster  `json:"clusters"`
	} `json:"static_resources"`
}

type listener struct {
	Name         string `json:"name"`
	FilterChains []struct {
		Filters []struct {
			TypedConfig struct {
				RouteConfig routeConfig `json:"route_config"`
			} `json:"typed_config"`
		} `json:"filters"`
	} `json:"filter_chains"`
}

type routeConfig struct {
	Name         string        `json:"name"`
	VirtualHosts []virtualHost `json:"virtual_hosts"`
}

type virtualHost struct {
	Name                   string       `json:"name"`
	Domains                []string     `json:"domains"`
	RequestHeadersToAdd    []headerAdd  `json:"request_headers_to_add"`
	RequestHeadersToRemove []string     `json:"request_headers_to_remove"`
	Routes                 []routeEntry `json:"routes"`
}

type headerAdd struct {
	Header struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"header"`
	AppendAction   string `json:"append_action"`
	KeepEmptyValue bool   `json:"keep_empty_value"`
}

type routeEntry struct {
	Route struct {
		Cluster string `json:"cluster"`
	} `json:"route"`
}

type cluster struct {
	Name            string `json:"name"`
	TransportSocket struct {
		TypedConfig struct {
			CommonTLSContext struct {
				TLSCertificates []struct {
					CertificateChain dataSource `json:"certificate_chain"`
					PrivateKey       dataSource `json:"private_key"`
				} `json:"tls_certificates"`
				ValidationContext struct {
					TrustedCA dataSource `json:"trusted_ca"`
				} `json:"validation_context"`
			} `json:"common_tls_context"`
		} `json:"typed_config"`
	} `json:"transport_socket"`
}

type dataSource struct {
	Filename string `json:"filename"`
}

// added returns the request_headers_to_add entry for name, or nil.
func (vh virtualHost) added(name string) *headerAdd {
	for i, h := range vh.RequestHeadersToAdd {
		if strings.EqualFold(h.Header.Key, name) {
			return &vh.RequestHeadersToAdd[i]
		}
	}
	return nil
}

// mitmRouteConfigs is every route config on the MITM listener: the leg that
// terminates the actor's tunneled TLS and re-originates it, and so the only
// place a header this gateway writes reaches an origin. The CONNECT listener's
// own route config is excluded — its headers set up the tunnel and never
// become headers of the request inside it.
func mitmRouteConfigs(t *testing.T, b *bootstrap) []routeConfig {
	t.Helper()
	const mitmListener = "mitm_listener"

	var configs []routeConfig
	for _, l := range b.StaticResources.Listeners {
		if l.Name != mitmListener {
			continue
		}
		for _, fc := range l.FilterChains {
			for _, f := range fc.Filters {
				if rc := f.TypedConfig.RouteConfig; rc.Name != "" {
					configs = append(configs, rc)
				}
			}
		}
	}
	if len(configs) == 0 {
		t.Fatalf("the egress bootstrap has no route configs on listener %s; either it was renamed or these tests are checking nothing", mitmListener)
	}
	return configs
}

func findCluster(t *testing.T, b *bootstrap, name string) cluster {
	t.Helper()
	for _, c := range b.StaticResources.Clusters {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("the egress bootstrap has no cluster named %q", name)
	return cluster{}
}

// egressBootstrap is the Envoy config the egress gateway runs.
func egressBootstrap(t *testing.T) *bootstrap {
	t.Helper()
	const (
		manifestPath  = "../../../../../manifests/ate-install/atenet-egress-with-sdsmint.yaml"
		configMapName = "atenet-egress"
		configKey     = "envoy.yaml"
	)

	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("reading %s: %v", manifestPath, err)
	}
	for doc := range strings.SplitSeq(string(raw), "\n---\n") {
		var cm corev1.ConfigMap
		if err := yaml.Unmarshal([]byte(doc), &cm); err != nil {
			// Documents of other kinds need not fit a ConfigMap.
			continue
		}
		if cm.Kind != "ConfigMap" || cm.Name != configMapName {
			continue
		}
		envoyYAML, ok := cm.Data[configKey]
		if !ok {
			t.Fatalf("ConfigMap %s in %s has no %s key", configMapName, manifestPath, configKey)
		}
		var b bootstrap
		if err := yaml.Unmarshal([]byte(envoyYAML), &b); err != nil {
			t.Fatalf("parsing %s from ConfigMap %s: %v", configKey, configMapName, err)
		}
		return &b
	}
	t.Fatalf("%s has no ConfigMap named %s", manifestPath, configMapName)
	return nil
}
