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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/tink"
	"golang.org/x/oauth2"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/internal/objectstoreplugin/gcsboundary"
	pb "go.ate.dev/substrate/internal/proto/clientsideboundarypb"
)

// Client-side Credential Access Boundaries, as google-auth-library-java's
// ClientSideCredentialAccessBoundaryFactory implements them, see
// https://docs.cloud.google.com/iam/docs/create-downscoped-short-lived-credentials#client-side-token-exchange
// and
// https://github.com/googleapis/google-auth-library-java/blob/9ac2d4340ebc6a8582b898e97f65aeed3c1776d6/cab-token-generator/java/com/google/auth/credentialaccessboundary/ClientSideCredentialAccessBoundaryFactory.java.
// One STS exchange per source token returns an intermediary token and a
// session key. Each downscoped token is then the intermediary token, a dot,
// and the boundary encrypted with the session key. GCS accepts the
// intermediary token only with a boundary appended.
const (
	stsTokenURL           = "https://sts.googleapis.com/v1/token"
	tokenExchangeGrant    = "urn:ietf:params:oauth:grant-type:token-exchange"
	accessTokenType       = "urn:ietf:params:oauth:token-type:access_token"
	intermediaryTokenType = "urn:ietf:params:oauth:token-type:access_boundary_intermediary_token"
	// maxSTSResponse bounds what is read of an STS response, which is under
	// a few KiB when valid. An error quotes at most the first KiB.
	maxSTSResponse = 64 << 10
)

// intermediary is what one STS exchange returns for a source token.
type intermediary struct {
	token  string
	expiry time.Time
	aead   tink.AEAD
}

// currentIntermediary returns the intermediary for the current source token,
// exchanging at STS when none is stored for it. Intermediaries are stored by
// source token until they expire, so a caller still holding a token that was
// just replaced finds its intermediary too. Concurrent mints share one
// exchange, which outlives a caller that gives up, so that caller does not
// fail the mints waiting on it.
func (i *Issuer) currentIntermediary(ctx context.Context) (*intermediary, error) {
	src, err := i.source.Token()
	if err != nil {
		return nil, fmt.Errorf("while getting the source token: %w", err)
	}
	if in := i.stored(src); in != nil {
		return in, nil
	}
	ch := i.flight.DoChan(src.AccessToken, func() (any, error) {
		if in := i.stored(src); in != nil {
			return in, nil
		}
		in, err := i.exchangeIntermediary(context.WithoutCancel(ctx), src)
		if err != nil {
			return nil, err
		}
		i.intermediaries.Set(src.AccessToken, in, in.expiry.Sub(i.now()))
		return in, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*intermediary), nil
	}
}

// stored returns the intermediary exchanged for src, if it has not expired.
// The cache expires entries on the wall clock, so expiry is checked again on
// the issuer's clock, which tests set with WithClock.
func (i *Issuer) stored(src *oauth2.Token) *intermediary {
	if v, ok := i.intermediaries.Get(src.AccessToken); ok {
		if in := v.(*intermediary); i.now().Before(in.expiry) {
			return in
		}
	}
	return nil
}

func (i *Issuer) exchangeIntermediary(ctx context.Context, src *oauth2.Token) (*intermediary, error) {
	form := url.Values{
		"grant_type":           {tokenExchangeGrant},
		"subject_token_type":   {accessTokenType},
		"subject_token":        {src.AccessToken},
		"requested_token_type": {intermediaryTokenType},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, stsTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := i.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("while exchanging at STS: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSTSResponse))
	if err != nil {
		return nil, fmt.Errorf("while reading the STS response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("while exchanging at STS: status %d: %.1024s", resp.StatusCode, body)
	}
	var r struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		SessionKey  string `json:"access_boundary_session_key"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("while decoding the STS response: %w", err)
	}
	if r.AccessToken == "" {
		return nil, errors.New("STS returned no access token")
	}
	if r.SessionKey == "" {
		return nil, errors.New("STS returned no session key")
	}
	// For a service account's source token, STS reports the token's remaining
	// life, less a second. Google's Java library says it reports nothing for
	// other tokens. The intermediary is not trusted past the source token
	// either way.
	expiry := src.Expiry
	if r.ExpiresIn > 0 {
		if sts := i.now().Add(time.Duration(r.ExpiresIn) * time.Second); expiry.IsZero() || sts.Before(expiry) {
			expiry = sts
		}
	}
	if expiry.IsZero() {
		// Without an expiry the node plugin could not know when to stop using
		// the token.
		return nil, errors.New("intermediary token has no expiry")
	}
	rawKey, err := base64.StdEncoding.DecodeString(r.SessionKey)
	if err != nil {
		return nil, fmt.Errorf("while decoding the session key: %w", err)
	}
	handle, err := insecurecleartextkeyset.Read(keyset.NewBinaryReader(bytes.NewReader(rawKey)))
	if err != nil {
		return nil, fmt.Errorf("while reading the session key: %w", err)
	}
	a, err := aead.New(handle)
	if err != nil {
		return nil, fmt.Errorf("while loading the session key: %w", err)
	}
	return &intermediary{token: r.AccessToken, expiry: expiry, aead: a}, nil
}

// encode serializes rules as the ClientSideAccessBoundary STS decrypts. A
// condition is parsed, not type checked, as the Java factory does.
func encode(env *cel.Env, rules []gcsboundary.Rule) ([]byte, error) {
	var b pb.ClientSideAccessBoundary
	for _, r := range rules {
		ast, iss := env.Parse(r.Condition)
		if iss.Err() != nil {
			return nil, fmt.Errorf("while parsing condition %q: %w", r.Condition, iss.Err())
		}
		parsed, err := cel.AstToParsedExpr(ast)
		if err != nil {
			return nil, fmt.Errorf("while converting condition %q: %w", r.Condition, err)
		}
		cond, err := proto.Marshal(parsed.GetExpr())
		if err != nil {
			return nil, fmt.Errorf("while encoding condition %q: %w", r.Condition, err)
		}
		b.AccessBoundaryRules = append(b.AccessBoundaryRules, &pb.ClientSideAccessBoundaryRule{
			AvailableResource:             r.Resource,
			AvailablePermissions:          r.Permissions,
			CompiledAvailabilityCondition: cond,
		})
	}
	return proto.Marshal(&b)
}

// seal encrypts the boundary with the session key and appends it to the
// intermediary token, unpadded as the Java library writes it.
func (in *intermediary) seal(plain []byte) (string, error) {
	sealed, err := in.aead.Encrypt(plain, []byte{})
	if err != nil {
		return "", fmt.Errorf("while encrypting the access boundary: %w", err)
	}
	return in.token + "." + base64.RawURLEncoding.EncodeToString(sealed), nil
}
