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

package imagecache

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/google/go-containerregistry/pkg/authn"
)

// ecrHost matches private Amazon ECR registry hosts and captures the region:
// <account>.dkr.ecr[-fips].<region>.amazonaws.com[.cn].
var ecrHost = regexp.MustCompile(`^\d{12}\.dkr\.ecr(?:-fips)?\.([a-z0-9-]+)\.amazonaws\.com(?:\.cn)?$`)

// ecrTokenRefreshMargin renews a token this long before ECR says it expires, so
// a pull never starts with a token that could lapse mid-transfer.
const ecrTokenRefreshMargin = 30 * time.Minute

type ecrKeychain struct {
	cfg aws.Config

	mu     sync.Mutex
	tokens map[string]ecrToken // keyed by registry host
}

type ecrToken struct {
	auth    authn.Authenticator
	expires time.Time
}

// NewECRKeychain returns a keychain for private Amazon ECR registries. It trades
// the ambient AWS credentials (on EKS, the pod's IRSA role) for a registry token
// through ecr:GetAuthorizationToken, caches it per registry and renews it before
// it expires. Any other host resolves to anonymous, so the keychain never lends
// ECR credentials to another registry.
func NewECRKeychain(cfg aws.Config) authn.Keychain {
	return &ecrKeychain{cfg: cfg, tokens: map[string]ecrToken{}}
}

func (k *ecrKeychain) Resolve(target authn.Resource) (authn.Authenticator, error) {
	host := target.RegistryStr()
	m := ecrHost.FindStringSubmatch(host)
	if m == nil {
		return authn.Anonymous, nil
	}
	region := m[1]

	k.mu.Lock()
	defer k.mu.Unlock()
	if t, ok := k.tokens[host]; ok && time.Now().Before(t.expires.Add(-ecrTokenRefreshMargin)) {
		return t.auth, nil
	}

	// Pin the region to the registry's, and drop any process-wide endpoint
	// override: AWS_ENDPOINT_URL may point the object store client at an
	// S3-compatible service, and must not redirect ECR calls there too.
	client := ecr.NewFromConfig(k.cfg, func(o *ecr.Options) {
		o.Region = region
		o.BaseEndpoint = nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := client.GetAuthorizationToken(ctx, &ecr.GetAuthorizationTokenInput{})
	if err != nil {
		return nil, fmt.Errorf("ecr GetAuthorizationToken for %s: %w", host, err)
	}
	if len(out.AuthorizationData) == 0 || out.AuthorizationData[0].AuthorizationToken == nil {
		return nil, fmt.Errorf("ecr GetAuthorizationToken for %s returned no token", host)
	}
	data := out.AuthorizationData[0]
	decoded, err := base64.StdEncoding.DecodeString(aws.ToString(data.AuthorizationToken))
	if err != nil {
		return nil, fmt.Errorf("decoding the ECR token for %s: %w", host, err)
	}
	user, pass, ok := strings.Cut(string(decoded), ":")
	if !ok {
		return nil, fmt.Errorf("malformed ECR token for %s", host)
	}

	auth := authn.FromConfig(authn.AuthConfig{Username: user, Password: pass})
	expires := time.Now().Add(time.Hour)
	if data.ExpiresAt != nil {
		expires = *data.ExpiresAt
	}
	k.tokens[host] = ecrToken{auth: auth, expires: expires}
	return auth, nil
}
