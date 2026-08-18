package credentials

import (
	"context"
	"time"
)

type Credentials struct {
	AccessKeyID     string     // Access key ID
	AccessKeySecret string     // Access Key Secret
	SecurityToken   string     // Security Token
	Expires         *time.Time // The time the credentials will expire at.
}

func (v Credentials) Expired() bool {
	if v.Expires != nil {
		return !v.Expires.After(time.Now().Round(0))
	}
	return false
}

func (v Credentials) HasKeys() bool {
	return len(v.AccessKeyID) > 0 && len(v.AccessKeySecret) > 0
}

type CredentialsProvider interface {
	GetCredentials(ctx context.Context) (Credentials, error)
}

// CredentialsProviderFunc provides a helper wrapping a function value to
// satisfy the CredentialsProvider interface.
type CredentialsProviderFunc func(context.Context) (Credentials, error)

// GetCredentials delegates to the function value the CredentialsProviderFunc wraps.
func (fn CredentialsProviderFunc) GetCredentials(ctx context.Context) (Credentials, error) {
	return fn(ctx)
}

type AnonymousCredentialsProvider struct{}

func NewAnonymousCredentialsProvider() CredentialsProvider {
	return &AnonymousCredentialsProvider{}
}

func (*AnonymousCredentialsProvider) GetCredentials(_ context.Context) (Credentials, error) {
	return Credentials{AccessKeyID: "", AccessKeySecret: ""}, nil
}
