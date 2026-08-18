package credentials

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

const ecs_ram_cred_url = "http://100.100.100.200/latest/meta-data/ram/security-credentials/"

type ecsRoleCredentialsProvider struct {
	ramCredUrl string
	ramRole    string
	timeout    time.Duration
	retries    int
}

type ecsRoleCredentials struct {
	AccessKeyId     string    `json:"AccessKeyId,omitempty"`
	AccessKeySecret string    `json:"AccessKeySecret,omitempty"`
	SecurityToken   string    `json:"SecurityToken,omitempty"`
	Expiration      time.Time `json:"Expiration,omitempty"`
	LastUpDated     time.Time `json:"LastUpDated,omitempty"`
	Code            string    `json:"Code,omitempty"`
}

func (p *ecsRoleCredentialsProvider) httpGet(ctx context.Context, url string) (*http.Response, error) {
	c := &http.Client{
		Timeout: p.timeout,
	}
	var resp *http.Response
	var err error
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	for i := 0; i < p.retries; i++ {
		resp, err = c.Do(req)
		if err != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return resp, nil
	}
	return nil, err
}

func (p *ecsRoleCredentialsProvider) getRoleFromMetaData(ctx context.Context) (string, error) {
	resp, err := p.httpGet(ctx, p.ramCredUrl)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to fetch ecs role name, resp.StatusCode:%v", resp.StatusCode)
	}
	defer resp.Body.Close()
	roleName, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if len(roleName) == 0 {
		return "", errors.New("ecs role name is empty")
	}

	return string(roleName), nil
}

func (p *ecsRoleCredentialsProvider) getCredentialsFromMetaData(ctx context.Context) (ecsRoleCredentials, error) {
	var ecsCred ecsRoleCredentials
	u, err := url.Parse(p.ramCredUrl)
	if err != nil {
		return ecsCred, err
	}
	u.Path = path.Join(u.Path, p.ramRole)
	resp, err := p.httpGet(ctx, u.String())
	if err != nil {
		return ecsCred, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ecsCred, err
	}
	err = json.Unmarshal(body, &ecsCred)
	if err != nil {
		return ecsCred, err
	}

	if ecsCred.Code != "" && strings.ToUpper(ecsCred.Code) != "SUCCESS" {
		return ecsCred, fmt.Errorf("failed to fetch credentials, return code:%s", ecsCred.Code)
	}

	if ecsCred.AccessKeyId == "" || ecsCred.AccessKeySecret == "" {
		return ecsCred, fmt.Errorf("AccessKeyId or AccessKeySecret is empty, response body is '%s'", string(body))
	}

	return ecsCred, nil
}

func (p *ecsRoleCredentialsProvider) GetCredentials(ctx context.Context) (cred Credentials, err error) {
	if len(p.ramRole) == 0 {
		name, err := p.getRoleFromMetaData(ctx)
		if err != nil {
			return cred, err
		}
		p.ramRole = name
	}
	ecsCred, err := p.getCredentialsFromMetaData(ctx)
	if err != nil {
		return cred, err
	}
	cred.AccessKeyID = ecsCred.AccessKeyId
	cred.AccessKeySecret = ecsCred.AccessKeySecret
	cred.SecurityToken = ecsCred.SecurityToken
	if !ecsCred.Expiration.IsZero() {
		cred.Expires = &ecsCred.Expiration
	}
	return cred, nil
}

type EcsRoleCredentialsProviderOptions struct {
	RamRole string
	Timeout time.Duration
	Retries int
}

func NewEcsRoleCredentialsProviderWithoutRefresh(optFns ...func(*EcsRoleCredentialsProviderOptions)) CredentialsProvider {
	options := EcsRoleCredentialsProviderOptions{
		RamRole: "",
		Timeout: time.Second * 10,
		Retries: 3,
	}
	for _, fn := range optFns {
		fn(&options)
	}
	return &ecsRoleCredentialsProvider{
		ramCredUrl: ecs_ram_cred_url,
		ramRole:    options.RamRole,
		timeout:    options.Timeout,
		retries:    options.Retries,
	}
}

func EcsRamRole(ramRole string) func(*EcsRoleCredentialsProviderOptions) {
	return func(options *EcsRoleCredentialsProviderOptions) {
		options.RamRole = ramRole
	}
}

func NewEcsRoleCredentialsProvider(optFns ...func(*EcsRoleCredentialsProviderOptions)) CredentialsProvider {
	p := NewEcsRoleCredentialsProviderWithoutRefresh(optFns...)
	provider := NewCredentialsFetcherProvider(CredentialsFetcherFunc(func(ctx context.Context) (Credentials, error) {
		return p.GetCredentials(ctx)
	}))
	return provider
}
