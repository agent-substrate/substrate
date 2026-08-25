//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"fmt"
	"slices"
	"strings"

	"github.com/agent-substrate/substrate/internal/pemutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	certlisters "k8s.io/client-go/listers/certificates/v1beta1"
)

// EgressTrustBundleName is the well-known name of the egress gateway CA
// bundle (#823): the trust anchors for the per-SNI leaves the egress gateway
// mints, maintained by atecontroller from the egress-mitm-ca-pool.
const EgressTrustBundleName = "egress-mitm.ate.dev"

// supportedTrustBundles maps the bundle names the trustBundle data source
// may reference to their backing ClusterTrustBundle objects. Enforced here
// rather than in the CRD schema so a configurable backend registry (#932)
// can widen it without a template API change.
var supportedTrustBundles = map[string]string{
	EgressTrustBundleName: "egress-mitm.ate.dev:mitm:primary-bundle",
}

// supportedTrustBundleNames returns the allowlist, sorted, for error text.
func supportedTrustBundleNames() string {
	names := make([]string, 0, len(supportedTrustBundles))
	for name := range supportedTrustBundles {
		names = append(names, name)
	}
	slices.Sort(names)
	return strings.Join(names, ", ")
}

// resolveTrustBundle returns the sanitized PEM of the named trust bundle,
// resolving the name against the allowlist and reading the backing
// ClusterTrustBundle through atelet's informer-backed lister. Every error
// fails the actor start: an actor that declared a trust bundle must not
// start without one.
func resolveTrustBundle(lister certlisters.ClusterTrustBundleLister, name string) ([]byte, error) {
	objectName, supported := supportedTrustBundles[name]
	if !supported {
		return nil, fmt.Errorf("trust bundle %q is not supported by this deployment (supported: %s)", name, supportedTrustBundleNames())
	}
	if lister == nil {
		return nil, fmt.Errorf("trust bundle %q: no ClusterTrustBundle lister configured", name)
	}
	bundle, err := lister.Get(objectName)
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("trust bundle %q: ClusterTrustBundle %q not found", name, objectName)
	} else if err != nil {
		return nil, fmt.Errorf("trust bundle %q: while reading ClusterTrustBundle %q: %w", name, objectName, err)
	}
	pemBundle, err := pemutil.SanitizeCertificateBundle([]byte(bundle.Spec.TrustBundle))
	if err != nil {
		return nil, fmt.Errorf("trust bundle %q: unusable ClusterTrustBundle %q: %w", name, objectName, err)
	}
	return pemBundle, nil
}
