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

package gcsissuer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/option"
)

const (
	// transferTokenLifetime is what the source token is requested with, the
	// longest the IAM Credentials API grants without an org policy change.
	transferTokenLifetime = time.Hour
	// sourceRefreshMargin is how much life a source token has left when the
	// next one is fetched. Downscoped tokens expire with their source, so each
	// starts with at least this long.
	sourceRefreshMargin = 40 * time.Minute
	// iamTimeout bounds one IAM Credentials call, as long as the metadata
	// server client waits for a token.
	iamTimeout = 5 * time.Second
)

// transferAccountSource returns a token for the transfer service account on
// every call, from the IAM Credentials API. The caller is the pod's own
// identity, which needs roles/iam.serviceAccountTokenCreator on the account.
type transferAccountSource struct {
	ctx  context.Context
	svc  *iamcredentials.Service
	name string
}

func (s transferAccountSource) Token() (*oauth2.Token, error) {
	ctx, cancel := context.WithTimeout(s.ctx, iamTimeout)
	defer cancel()
	resp, err := s.svc.Projects.ServiceAccounts.GenerateAccessToken(s.name, &iamcredentials.GenerateAccessTokenRequest{
		Scope:    []string{cloudPlatformScope},
		Lifetime: fmt.Sprintf("%.0fs", transferTokenLifetime.Seconds()),
	}).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("while getting a token for %s: %w", s.name, err)
	}
	if resp.AccessToken == "" {
		return nil, fmt.Errorf("the IAM Credentials API returned no token for %s", s.name)
	}
	expiry, err := time.Parse(time.RFC3339, resp.ExpireTime)
	if err != nil {
		return nil, fmt.Errorf("the IAM Credentials API returned expiry %q for %s: %w", resp.ExpireTime, s.name, err)
	}
	return &oauth2.Token{AccessToken: resp.AccessToken, TokenType: "Bearer", Expiry: expiry}, nil
}

// fallbackSource returns the last token src gave while src fails and that
// token has not expired, so a brief IAM Credentials outage does not fail
// exchanges.
type fallbackSource struct {
	src  oauth2.TokenSource
	last atomic.Pointer[oauth2.Token]
}

func (f *fallbackSource) Token() (*oauth2.Token, error) {
	tok, err := f.src.Token()
	if err != nil {
		// Valid would apply the 40 minute margin the reuse source sets on its
		// tokens, so check the expiry itself. The token must outlast the STS
		// call it is downscoped with.
		if last := f.last.Load(); last != nil && time.Until(last.Expiry) > stsTimeout {
			return last, nil
		}
		return nil, err
	}
	f.last.Store(tok)
	return tok, nil
}

// newTransferAccountSource returns the source credential: tokens for account,
// fetched again once less than sourceRefreshMargin is left. It calls the IAM
// Credentials API itself rather than through a library that caches until
// shortly before expiry, since that cache would hand back the same token.
func newTransferAccountSource(ctx context.Context, account string, opts ...option.ClientOption) (oauth2.TokenSource, error) {
	if account == "" {
		return nil, errors.New("no transfer service account to impersonate")
	}
	svc, err := iamcredentials.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("while creating the IAM Credentials client: %w", err)
	}
	src := transferAccountSource{ctx: ctx, svc: svc, name: "projects/-/serviceAccounts/" + account}
	return &fallbackSource{src: oauth2.ReuseTokenSourceWithExpiry(nil, src, sourceRefreshMargin)}, nil
}
