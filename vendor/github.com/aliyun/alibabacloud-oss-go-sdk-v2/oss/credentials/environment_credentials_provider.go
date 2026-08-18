package credentials

import (
	"context"
	"fmt"
	"os"
)

type EnvironmentVariableCredentialsProvider struct {
}

func (s *EnvironmentVariableCredentialsProvider) GetCredentials(ctx context.Context) (Credentials, error) {
	id := os.Getenv("OSS_ACCESS_KEY_ID")
	secret := os.Getenv("OSS_ACCESS_KEY_SECRET")
	if id == "" || secret == "" {
		return Credentials{}, fmt.Errorf("access key id or access key secret is empty!")
	}
	return Credentials{
		AccessKeyID:     id,
		AccessKeySecret: secret,
		SecurityToken:   os.Getenv("OSS_SESSION_TOKEN"),
	}, nil
}

func NewEnvironmentVariableCredentialsProvider() CredentialsProvider {
	return &EnvironmentVariableCredentialsProvider{}
}
