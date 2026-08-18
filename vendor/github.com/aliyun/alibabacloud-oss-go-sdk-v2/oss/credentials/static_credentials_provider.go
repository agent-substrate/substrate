package credentials

import (
	"context"
)

type StaticCredentialsProvider struct {
	credentials Credentials
}

func NewStaticCredentialsProvider(id, secret string, tokens ...string) CredentialsProvider {
	token := ""
	if len(tokens) > 0 {
		token = tokens[0]
	}
	return StaticCredentialsProvider{
		credentials: Credentials{
			AccessKeyID:     id,
			AccessKeySecret: secret,
			SecurityToken:   token,
		}}
}

func (s StaticCredentialsProvider) GetCredentials(_ context.Context) (Credentials, error) {
	return s.credentials, nil
}
