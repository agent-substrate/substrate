// Copyright 2026 The Agent Substrate Authors
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

// Package gcsissuer mints GCS tokens downscoped to an actor's grants. It is the
// GCS half of the control plugin's credential exchange: actorscope decides what
// an actor may touch, this package turns that into a credential GCS enforces.
//
// The source credential is a token for the snapshot transfer service account,
// which the pod's own identity impersonates through the IAM Credentials API.
// GCS audit logs then name that account rather than ate-api-server. The
// downscope package exchanges the token at STS with a Credential Access
// Boundary and returns an access token GCS limits to the boundary. STS reports
// no lifetime for that token, so the package falls back to the source token's
// expiry, and the two expire together.
package gcsissuer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google/downscope"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/lru"

	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/objectstoreplugin/credential"
	"go.ate.dev/substrate/internal/objectstoreplugin/gcsboundary"
)

const (
	// cloudPlatformScope is the scope the source token is requested with. The
	// boundary narrows it to the actor's prefixes.
	cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

	// minServeLifetime is the least remaining lifetime a token is cached and
	// served with, the same on the node plugin's side.
	minServeLifetime = credential.MinServeLifetime
	// defaultCacheSize bounds the per-actor cache. A miss costs an exchange,
	// never correctness.
	defaultCacheSize = 100_000
	// stsTimeout bounds one exchange. The downscope package does not take a
	// per-call context, so the client's timeout is the only bound.
	stsTimeout = 30 * time.Second
	// mintFlightTimeout bounds one detached shared mint flight. It is shorter
	// than the worst-case retry chain (stsMaxAttempts x stsTimeout plus
	// backoff), so it also caps how long a flight keeps retrying.
	mintFlightTimeout = 2 * time.Minute

	stsMaxAttempts   = 6
	stsRetryBaseWait = 400 * time.Millisecond
	stsRetryMaxWait  = 6 * time.Second
)

// Issuer exchanges the transfer account's token for per-actor downscoped
// tokens and caches them by actor UID.
type Issuer struct {
	source     oauth2.TokenSource
	iamOptions []option.ClientOption
	client     *http.Client
	now        func() time.Time

	// mu coordinates flights and compound grant-coverage cache updates.
	mu      sync.Mutex
	cache   *lru.Cache
	flights map[string]*issueFlight
}

// entry is a cached token with the grants it was minted for, so a request can
// tell whether the token covers it.
type entry struct {
	token  credential.Token
	grants []actorscope.Grant
}

type issueFlight struct {
	waiters  int
	finished bool
	cancel   context.CancelFunc
	done     chan struct{}
	grants   []actorscope.Grant
	token    credential.Token
	err      error
}

func (i *Issuer) releaseWaiter(actorUID string, f *issueFlight) {
	var cancel context.CancelFunc
	i.mu.Lock()
	f.waiters--
	if f.waiters == 0 && !f.finished {
		if i.flights[actorUID] == f {
			delete(i.flights, actorUID)
		}
		cancel = f.cancel
	}
	i.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Option configures an Issuer.
type Option func(*Issuer)

// WithIAMCredentialsOptions configures the IAM Credentials client, for tests.
func WithIAMCredentialsOptions(opts ...option.ClientOption) Option {
	return func(i *Issuer) { i.iamOptions = opts }
}

// WithTokenSource replaces the transfer account's token, for tests.
func WithTokenSource(ts oauth2.TokenSource) Option { return func(i *Issuer) { i.source = ts } }

// WithHTTPClient overrides the client used for STS, for tests.
func WithHTTPClient(c *http.Client) Option { return func(i *Issuer) { i.client = c } }

// WithClock overrides the clock, for tests.
func WithClock(now func() time.Time) Option { return func(i *Issuer) { i.now = now } }

// WithCacheSize bounds the token cache. A size below one is treated as one,
// since lru.New(0) would mean no bound at all.
func WithCacheSize(n int) Option {
	return func(i *Issuer) { i.cache = lru.New(max(n, 1)) }
}

// New returns an Issuer whose source credential is a token for the transfer
// service account with email account. Tests that replace the source with
// WithTokenSource pass an empty account. The credential keeps refreshing for
// the Issuer's whole life, so ctx is detached from cancellation.
func New(ctx context.Context, account string, opts ...Option) (*Issuer, error) {
	i := &Issuer{
		client:  &http.Client{Timeout: stsTimeout},
		now:     time.Now,
		cache:   lru.New(defaultCacheSize),
		flights: make(map[string]*issueFlight),
	}
	for _, o := range opts {
		o(i)
	}
	if i.source == nil {
		src, err := newTransferAccountSource(context.WithoutCancel(ctx), account, i.iamOptions...)
		if err != nil {
			return nil, err
		}
		i.source = src
	}
	return i, nil
}

// Issue returns a token downscoped to grants for the actor. A cached token is
// returned while it covers the grants and has at least minServeLifetime left,
// unless force is set, which mints regardless and is what the node plugin
// sends after GCS refused a token. A minted token expires with the source
// token, which is fetched again once less than sourceRefreshMargin is left, so
// it starts with at least that long. If that fetch fails, the previous source
// token is used until it expires. A minted token with less than
// minServeLifetime left, which only that leaves behind, is handed out anyway,
// since a short token beats a failed transfer, but it is not cached.
//
// The cache is keyed by actor UID and each entry remembers its grants. Grants
// narrow when the tag an actor was created from drops out after its first
// completed suspend, and the cached token still covers that, a hit. They
// widen when a never-suspended actor is repointed to another storage
// location, and the cached token lacks it, a mint. So the common path costs
// nothing and a repoint never hands out a token GCS would refuse.
//
// A grant set the access boundary cannot express fails with codes.Internal.
// Every other failure is an STS or network error and carries no code.
func (i *Issuer) Issue(ctx context.Context, actorUID string, grants []actorscope.Grant, force bool) (credential.Token, error) {
	if len(grants) == 0 {
		// Checked before the cache: an empty set is covered by any cached
		// token, but an actor with no grants must get nothing.
		return credential.Token{}, status.Error(codes.Internal, "actor has no storage grants")
	}
	if err := ctx.Err(); err != nil {
		return credential.Token{}, err
	}
	if force {
		return i.mintAndStore(ctx, actorUID, grants, true)
	}

	i.mu.Lock()
	if tok, ok := i.lookupLocked(actorUID, grants); ok {
		i.mu.Unlock()
		return tok, nil
	}
	f, ok := i.flights[actorUID]
	if ok && covers(f.grants, grants) {
		f.waiters++
		i.mu.Unlock()
		select {
		case <-f.done:
			return f.token, f.err
		case <-ctx.Done():
			i.releaseWaiter(actorUID, f)
			return credential.Token{}, ctx.Err()
		}
	}
	if ok {
		// An in-flight mint has narrower grants than this caller needs; mint
		// directly for the wider grant set without blocking or overwriting the
		// flight slot.
		i.mu.Unlock()
		return i.mintAndStore(ctx, actorUID, grants, false)
	}

	mintCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mintFlightTimeout)
	clonedGrants := slices.Clone(grants)
	f = &issueFlight{
		waiters: 1,
		cancel:  cancel,
		done:    make(chan struct{}),
		grants:  clonedGrants,
	}
	i.flights[actorUID] = f
	i.mu.Unlock()

	go i.runFlight(mintCtx, actorUID, f)

	select {
	case <-f.done:
		return f.token, f.err
	case <-ctx.Done():
		i.releaseWaiter(actorUID, f)
		return credential.Token{}, ctx.Err()
	}
}

func (i *Issuer) lookupLocked(actorUID string, grants []actorscope.Grant) (credential.Token, bool) {
	v, ok := i.cache.Get(actorUID)
	if !ok {
		return credential.Token{}, false
	}
	e := v.(entry)
	if covers(e.grants, grants) && e.token.Expiry.Sub(i.now()) >= minServeLifetime {
		return e.token, true
	}
	return credential.Token{}, false
}

func (i *Issuer) storeLocked(actorUID string, tok credential.Token, grants []actorscope.Grant) {
	if tok.Expiry.Sub(i.now()) < minServeLifetime {
		return
	}
	if v, ok := i.cache.Get(actorUID); ok {
		// Keep an existing entry that strictly covers the new grants with a
		// longer or equal lifetime so a narrower concurrent mint cannot
		// downgrade a wider cached entry.
		prev := v.(entry)
		if covers(prev.grants, grants) && !covers(grants, prev.grants) &&
			prev.token.Expiry.Sub(i.now()) >= minServeLifetime &&
			!prev.token.Expiry.Before(tok.Expiry) {
			return
		}
	}
	i.cache.Add(actorUID, entry{token: tok, grants: slices.Clone(grants)})
}

func (i *Issuer) mintAndStore(ctx context.Context, actorUID string, grants []actorscope.Grant, force bool) (credential.Token, error) {
	tok, err := i.mint(ctx, grants)
	if err != nil {
		return credential.Token{}, err
	}
	i.mu.Lock()
	if force {
		if tok.Expiry.Sub(i.now()) >= minServeLifetime {
			i.cache.Add(actorUID, entry{token: tok, grants: slices.Clone(grants)})
		}
	} else {
		i.storeLocked(actorUID, tok, grants)
	}
	i.mu.Unlock()
	return tok, nil
}

func (i *Issuer) runFlight(ctx context.Context, actorUID string, f *issueFlight) {
	tok, err := i.mint(ctx, f.grants)
	f.token = tok
	f.err = err

	i.mu.Lock()
	if err == nil {
		i.storeLocked(actorUID, tok, f.grants)
	}
	if i.flights[actorUID] == f {
		delete(i.flights, actorUID)
	}
	f.finished = true
	cancel := f.cancel
	i.mu.Unlock()

	cancel()
	close(f.done)
}

// covers reports whether a token minted with the cached grants can serve a
// request for the requested grants: each requested prefix must appear in the
// cached grants with the same or higher access. Prefixes are compared as
// written, since both sides build them with the same helper.
func covers(cached, requested []actorscope.Grant) bool {
	for _, r := range requested {
		if !slices.ContainsFunc(cached, func(c actorscope.Grant) bool {
			return c.Prefix == r.Prefix && c.Access >= r.Access
		}) {
			return false
		}
	}
	return true
}

func (i *Issuer) mint(ctx context.Context, grants []actorscope.Grant) (credential.Token, error) {
	rules, err := gcsboundary.FromGrants(grants)
	if err != nil {
		return credential.Token{}, status.Errorf(codes.Internal, "while building the access boundary: %v", err)
	}

	var tok *oauth2.Token
	for attempt := range stsMaxAttempts {
		transport := &attemptTransport{ctx: ctx, base: i.client.Transport}
		rootSrc := &attemptTokenSource{base: i.source}
		// The downscope package reads its HTTP client from the context but sends
		// the STS request without one, so the caller's deadline is attached by the
		// transport instead.
		httpCtx := context.WithValue(ctx, oauth2.HTTPClient, &http.Client{
			Transport: transport,
			Timeout:   i.client.Timeout,
		})
		ts, buildErr := downscope.NewTokenSource(httpCtx, downscope.DownscopingConfig{RootSource: rootSrc, Rules: rules})
		if buildErr != nil {
			return credential.Token{}, status.Errorf(codes.Internal, "while building the access boundary: %v", buildErr)
		}
		tok, err = ts.Token()
		if err == nil {
			break
		}
		if ctx.Err() != nil || attempt == stsMaxAttempts-1 || !isRetryableMintAttempt(err, rootSrc.err, transport) {
			return credential.Token{}, fmt.Errorf("while exchanging at STS: %w", err)
		}
		base := min(stsRetryBaseWait<<attempt, stsRetryMaxWait)
		wait := base + time.Duration(rand.Int64N(int64(base/2)+1))
		slog.WarnContext(ctx, "Retrying transient STS token exchange error",
			slog.Int("attempt", attempt+1), slog.Duration("backoff", wait), slog.Any("err", err))
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return credential.Token{}, ctx.Err()
		case <-timer.C:
		}
	}
	if tok.AccessToken == "" {
		return credential.Token{}, errors.New("STS returned no access token")
	}
	if tok.Expiry.IsZero() {
		// Without an expiry the node plugin could not know when to stop using
		// the token.
		return credential.Token{}, errors.New("downscoped token has no expiry")
	}
	return credential.Token{AccessToken: tok.AccessToken, Expiry: tok.Expiry}, nil
}

func isRetryableMintAttempt(err, rootErr error, t *attemptTransport) bool {
	if t.bodyReadErr != nil {
		return !errors.Is(t.bodyReadErr, context.Canceled) && !errors.Is(t.bodyReadErr, context.DeadlineExceeded)
	}
	// If the STS HTTP round-trip ran, rely on the exact HTTP status or
	// transport error captured before downscope formatted the error string.
	if t.lastStatus != 0 {
		return t.lastStatus == http.StatusTooManyRequests || t.lastStatus >= http.StatusInternalServerError
	}
	if t.transportErr != nil {
		return !errors.Is(t.transportErr, context.Canceled) && !errors.Is(t.transportErr, context.DeadlineExceeded)
	}

	// Otherwise RootSource.Token() failed before STS was dialed (downscope
	// formats RootSource errors with %v rather than %w, so inspect rootErr).
	checkErr := err
	if rootErr != nil {
		checkErr = rootErr
	}
	if errors.Is(checkErr, context.Canceled) || errors.Is(checkErr, context.DeadlineExceeded) {
		return false
	}
	var ge *googleapi.Error
	if errors.As(checkErr, &ge) {
		return ge.Code == http.StatusTooManyRequests || ge.Code >= http.StatusInternalServerError
	}
	var re *oauth2.RetrieveError
	if errors.As(checkErr, &re) && re.Response != nil {
		return re.Response.StatusCode == http.StatusTooManyRequests || re.Response.StatusCode >= http.StatusInternalServerError
	}
	switch status.Code(checkErr) {
	case codes.ResourceExhausted, codes.Unavailable:
		return true
	}
	msg := strings.ToLower(checkErr.Error())
	return strings.Contains(msg, "resource_exhausted") ||
		strings.Contains(msg, "quota_exceeded") ||
		strings.Contains(msg, "quota exceeded") ||
		strings.Contains(msg, "too many requests") ||
		strings.Contains(msg, "service unavailable")
}

type attemptTokenSource struct {
	base oauth2.TokenSource
	err  error
}

func (s *attemptTokenSource) Token() (*oauth2.Token, error) {
	tok, err := s.base.Token()
	s.err = err
	return tok, err
}

// attemptTransport sends every request with ctx and records the HTTP status,
// transport error, or response-body read error observed on the STS leg.
type attemptTransport struct {
	ctx          context.Context
	base         http.RoundTripper
	lastStatus   int
	transportErr error
	bodyReadErr  error
}

func (t *attemptTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req.WithContext(t.ctx))
	if err != nil {
		t.transportErr = err
		return nil, err
	}
	t.lastStatus = resp.StatusCode
	if resp.Body != nil {
		resp.Body = &trackingReadCloser{rc: resp.Body, onErr: func(rerr error) { t.bodyReadErr = rerr }}
	}
	return resp, nil
}

type trackingReadCloser struct {
	rc    io.ReadCloser
	onErr func(error)
}

func (r *trackingReadCloser) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		r.onErr(err)
	}
	return n, err
}

func (r *trackingReadCloser) Close() error { return r.rc.Close() }
