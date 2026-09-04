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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	certlisters "k8s.io/client-go/listers/certificates/v1beta1"
)

// EgressTrustBundleName is the well-known name of the egress gateway CA
// bundle (#823): the trust anchors for the per-SNI leaves the egress gateway
// mints, maintained by atecontroller from the egress-mitm-ca-pool.
const EgressTrustBundleName = "egress-mitm.ate.dev"

// supportedTrustBundles maps the bundle names the trustBundle data source
// may reference to their backing ClusterTrustBundles. Enforced here so a
// configurable backend registry (#932) can widen it without an API change.
//
// TODO(#932): select bundles by signer name + label selector (merging the
// matches) instead of one direct object, so a new root can be trialed on a
// fraction of workloads before it reaches all of them.
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

// bundleNamesFor returns the allowlisted bundle names backed by the
// ClusterTrustBundle objectName.
func bundleNamesFor(objectName string) []string {
	var names []string
	for name, object := range supportedTrustBundles {
		if object == objectName {
			names = append(names, name)
		}
	}
	return names
}

// rawTrustBundle returns the unsanitized contents of the named trust
// bundle's backing ClusterTrustBundle, resolving the name against the
// allowlist and reading through atelet's informer-backed lister.
func rawTrustBundle(lister certlisters.ClusterTrustBundleLister, name string) (objectName, raw string, err error) {
	objectName, supported := supportedTrustBundles[name]
	if !supported {
		return "", "", fmt.Errorf("trust bundle %q is not supported by this deployment (supported: %s)", name, supportedTrustBundleNames())
	}
	bundle, err := lister.Get(objectName)
	if apierrors.IsNotFound(err) {
		return "", "", fmt.Errorf("trust bundle %q: ClusterTrustBundle %q not found", name, objectName)
	} else if err != nil {
		return "", "", fmt.Errorf("trust bundle %q: while reading ClusterTrustBundle %q: %w", name, objectName, err)
	}
	return objectName, bundle.Spec.TrustBundle, nil
}

// trustBundleHash fingerprints raw backing contents. Refreshes compare this,
// never the projected bytes: sanitization shuffles the anchors, so two
// projections of identical contents differ byte-wise.
func trustBundleHash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
