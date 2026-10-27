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

// Package tokenexchange exchanges actor JWTs for access tokens at OAuth 2.0
// token exchange endpoints (RFC 8693).
package tokenexchange

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	grantTypeTokenExchange = "urn:ietf:params:oauth:grant-type:token-exchange"
	tokenTypeJWT           = "urn:ietf:params:oauth:token-type:jwt"
	tokenTypeAccessToken   = "urn:ietf:params:oauth:token-type:access_token"

	maxResponseBytes = 1 << 20
	// maxDetailBytes bounds the endpoint-supplied error fields an error carries.
	maxDetailBytes = 200
)

// deniedErrors are the OAuth error codes that mean the endpoint refused this
// actor's exchange.
var deniedErrors = map[string]bool{
	"invalid_grant":       true,
	"invalid_target":      true,
	"unauthorized_client": true,
	"invalid_scope":       true,
	"access_denied":       true,
}

// Token is an access token issued by a token endpoint.
type Token struct {
	AccessToken string
	// ExpiresAt is zero when the endpoint did not return expires_in.
	ExpiresAt time.Time
}

// Client sends token exchange requests.
type Client struct {
	httpClient *http.Client
}

// New returns a Client that trusts the system roots plus the PEM certificates
// in caFile, when set, which must hold at least one. The client ignores proxy
// environment variables and does not follow redirects.
func New(caFile string) (*Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if caFile != "" {
		certs, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("reading token exchange CA file: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("loading system roots: %w", err)
		}
		if !roots.AppendCertsFromPEM(certs) {
			return nil, fmt.Errorf("token exchange CA file %q contains no certificates", caFile)
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots}
	}
	return &Client{httpClient: &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}, nil
}

// Exchange trades subjectToken, a JWT, for an access token at tokenURL. ctx
// bounds the whole exchange. Errors carry a gRPC status code:
// PermissionDenied when the endpoint refuses the exchange, FailedPrecondition
// when the request or endpoint is misconfigured, and Unavailable or
// DeadlineExceeded when a retry may succeed.
func (c *Client) Exchange(ctx context.Context, tokenURL, subjectToken, audience string, scopes []string) (Token, error) {
	form := url.Values{
		"grant_type":           {grantTypeTokenExchange},
		"subject_token":        {subjectToken},
		"subject_token_type":   {tokenTypeJWT},
		"audience":             {audience},
		"requested_token_type": {tokenTypeAccessToken},
	}
	if len(scopes) > 0 {
		form.Set("scope", strings.Join(scopes, " "))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return Token{}, status.Errorf(codes.FailedPrecondition, "building token exchange request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		code := codes.Unavailable
		if errors.Is(err, context.DeadlineExceeded) {
			code = codes.DeadlineExceeded
		}
		return Token{}, status.Errorf(code, "token exchange request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return Token{}, status.Errorf(codes.Unavailable, "reading token exchange response: %v", err)
	}
	if len(body) > maxResponseBytes {
		return Token{}, status.Errorf(codes.Unavailable, "token exchange response exceeds %d bytes", maxResponseBytes)
	}

	if resp.StatusCode == http.StatusOK {
		return parseToken(body)
	}
	var oauthErr oauthError
	// A body that is not an OAuth error response leaves both fields empty.
	_ = json.Unmarshal(body, &oauthErr)
	code := codes.FailedPrecondition
	switch {
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		code = codes.Unavailable
	case (resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized) && deniedErrors[oauthErr.Error]:
		code = codes.PermissionDenied
	}
	return Token{}, status.Errorf(code, "token endpoint returned %d%s", resp.StatusCode, oauthErr.detail())
}

// parseToken reads a successful token exchange response.
func parseToken(body []byte) (Token, error) {
	var r struct {
		AccessToken     string `json:"access_token"`
		IssuedTokenType string `json:"issued_token_type"`
		ExpiresIn       *int64 `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return Token{}, status.Errorf(codes.Unavailable, "parsing token exchange response: %v", err)
	}
	if r.IssuedTokenType != "" && r.IssuedTokenType != tokenTypeAccessToken {
		return Token{}, status.Errorf(codes.Unavailable, "token endpoint issued token type %q, want %q", clip(r.IssuedTokenType), tokenTypeAccessToken)
	}
	if !isAccessToken(r.AccessToken) {
		return Token{}, status.Error(codes.Unavailable, "token endpoint returned an empty access_token or one with characters outside VSCHAR")
	}
	tok := Token{AccessToken: r.AccessToken}
	if r.ExpiresIn != nil {
		tok.ExpiresAt = time.Now().Add(time.Duration(*r.ExpiresIn) * time.Second)
	}
	return tok, nil
}

// isAccessToken reports whether s is 1*VSCHAR, the access-token syntax of
// RFC 6749 Appendix A.12.
func isAccessToken(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return s != ""
}

// oauthError is an OAuth 2.0 error response (RFC 6749 section 5.2).
type oauthError struct {
	Error       string `json:"error"`
	Description string `json:"error_description"`
}

// detail formats e for an error message, clipped and quoted, or "" when the
// response carried neither field.
func (e oauthError) detail() string {
	if e.Error == "" && e.Description == "" {
		return ""
	}
	return fmt.Sprintf(": error %q, error_description %q", clip(e.Error), clip(e.Description))
}

func clip(s string) string {
	if len(s) > maxDetailBytes {
		return s[:maxDetailBytes]
	}
	return s
}
