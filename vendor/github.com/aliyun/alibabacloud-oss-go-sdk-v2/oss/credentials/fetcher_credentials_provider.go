package credentials

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// Default expiration time adjustment factor
	defaultExpiredFactor = 0.8

	// backoff of refresh time
	defaultRefreshDuration = 120 * time.Second
)

// CredentialsFetcherOptions are the options
type CredentialsFetcherOptions struct {
	ExpiredFactor   float64
	RefreshDuration time.Duration
}

type CredentialsFetcher interface {
	Fetch(ctx context.Context) (Credentials, error)
}

// CredentialsFetcherFunc provides a helper wrapping a function value to
// satisfy the CredentialsFetcher interface.
type CredentialsFetcherFunc func(context.Context) (Credentials, error)

// Fetch delegates to the function value the CredentialsFetcherFunc wraps.
func (fn CredentialsFetcherFunc) Fetch(ctx context.Context) (Credentials, error) {
	return fn(ctx)
}

type CredentialsFetcherProvider struct {
	m sync.Mutex

	//credentials *fetcherCredentials
	credentials atomic.Value

	fetcher CredentialsFetcher

	expiredFactor   float64
	refreshDuration time.Duration
}

type fetcherCredentials struct {
	Creds        Credentials
	ExpiryWindow time.Duration
}

func NewCredentialsFetcherProvider(fetcher CredentialsFetcher, optFns ...func(*CredentialsFetcherOptions)) CredentialsProvider {
	options := CredentialsFetcherOptions{
		ExpiredFactor:   defaultExpiredFactor,
		RefreshDuration: defaultRefreshDuration,
	}

	for _, fn := range optFns {
		fn(&options)
	}

	return &CredentialsFetcherProvider{
		fetcher:         fetcher,
		expiredFactor:   options.ExpiredFactor,
		refreshDuration: options.RefreshDuration,
	}
}

func (c *CredentialsFetcherProvider) GetCredentials(ctx context.Context) (Credentials, error) {
	fcreds := c.getCreds()
	if c.isExpired(fcreds) {
		c.m.Lock()
		defer c.m.Unlock()
		creds, err := c.fetch(ctx)
		if err == nil {
			c.updateCreds(&creds)
		}
		return creds, err
	} else {
		if c.isSoonExpire(fcreds) && c.m.TryLock() {
			defer c.m.Unlock()
			fcreds1 := c.getCreds()
			if fcreds1 == fcreds {
				creds, err := c.fetch(ctx)
				if err == nil {
					c.updateCreds(&creds)
					return creds, nil
				} else {
					c.updateExpiryWindow(fcreds1)
					err = nil
				}
			}
			fcreds = fcreds1
		}
		return fcreds.Creds, nil
	}
}

type asyncFetchResult struct {
	val Credentials
	err error
}

func (c *CredentialsFetcherProvider) asyncFetch(ctx context.Context) <-chan asyncFetchResult {
	doChan := func() <-chan asyncFetchResult {
		ch := make(chan asyncFetchResult, 1)

		go func() {
			cred, err := c.fetcher.Fetch(ctx)
			ch <- asyncFetchResult{cred, err}
		}()

		return ch
	}

	return doChan()
}

func (c *CredentialsFetcherProvider) fetch(ctx context.Context) (Credentials, error) {
	if c.fetcher == nil {
		return Credentials{}, fmt.Errorf("fetcher is null.")
	}

	select {
	case result, _ := <-c.asyncFetch(ctx):
		return result.val, result.err
	case <-ctx.Done():
		return Credentials{}, fmt.Errorf("FetchCredentialsCanceled")
	}
}

func (p *CredentialsFetcherProvider) getCreds() *fetcherCredentials {
	v := p.credentials.Load()
	if v == nil {
		return nil
	}
	creds, _ := v.(*fetcherCredentials)
	return creds
}

func (c *CredentialsFetcherProvider) updateCreds(cred *Credentials) {
	fcred := fetcherCredentials{
		Creds: *cred,
	}
	if cred.Expires != nil {
		curr := time.Now().Round(0)
		durationS := c.expiredFactor * float64(cred.Expires.Sub(curr).Seconds())
		duration := time.Duration(durationS * float64(time.Second))
		if duration > c.refreshDuration {
			fcred.ExpiryWindow = duration
		}
	}
	c.credentials.Store(&fcred)
}

func (c *CredentialsFetcherProvider) updateExpiryWindow(fcreds *fetcherCredentials) {
	if fcreds.ExpiryWindow > 0 {
		fcreds1 := *fcreds
		fcreds1.ExpiryWindow -= c.refreshDuration
		c.credentials.Store(&fcreds1)
	}
}

func (c *CredentialsFetcherProvider) isExpired(fcreds *fetcherCredentials) bool {
	return fcreds == nil || fcreds.Creds.Expired()
}

func (c *CredentialsFetcherProvider) isSoonExpire(fcreds *fetcherCredentials) bool {
	if fcreds == nil || fcreds.Creds.Expired() {
		return true
	}

	if fcreds.ExpiryWindow > 0 && fcreds.Creds.Expires != nil {
		if !fcreds.Creds.Expires.After(time.Now().Round(0).Add(fcreds.ExpiryWindow)) {
			return true
		}
	}

	return false
}
