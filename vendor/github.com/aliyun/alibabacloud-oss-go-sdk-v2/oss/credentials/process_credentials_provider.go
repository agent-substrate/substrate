package credentials

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"time"
)

/*
temporary access credentials format
{
	"AccessKeyId" : "ak",
	"AccessKeySecret" : "sk",
	"Expiration" : "2023-12-29T07:45:02Z",
	"SecurityToken" : "token",
}

long-term access credentials
{
	"AccessKeyId" : "ak",
	"AccessKeySecret" : "sk",
}
*/

type processCredentialsResult struct {
	AccessKeyId string `json:"AccessKeyId"`

	AccessKeySecret string `json:"AccessKeySecret"`

	SecurityToken string `json:"SecurityToken"`

	Expiration *time.Time `json:"Expiration"`
}

type ProcessCredentialsProviderOptions struct {
	Timeout time.Duration
}

type ProcessCredentialsProvider struct {
	timeout time.Duration
	args    []string
}

func NewProcessCredentialsProvider(command string, optFns ...func(*ProcessCredentialsProviderOptions)) CredentialsProvider {
	options := ProcessCredentialsProviderOptions{
		Timeout: 15 * time.Second,
	}

	for _, fn := range optFns {
		fn(&options)
	}

	var args []string
	if len(command) > 0 {
		args = []string{command}
	}

	return &ProcessCredentialsProvider{
		timeout: options.Timeout,
		args:    args,
	}
}

func (p *ProcessCredentialsProvider) GetCredentials(ctx context.Context) (Credentials, error) {
	return p.fetchCredentials(ctx)
}

func (p *ProcessCredentialsProvider) buildCommand(ctx context.Context) (*exec.Cmd, error) {
	if len(p.args) == 0 {
		return nil, fmt.Errorf("command must not be empty")
	}

	var cmdArgs []string
	if runtime.GOOS == "windows" {
		cmdArgs = []string{"cmd.exe", "/C"}
	} else {
		cmdArgs = []string{"sh", "-c"}
	}

	cmdArgs = append(cmdArgs, p.args...)
	cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
	cmd.Env = os.Environ()

	return cmd, nil
}

func (p *ProcessCredentialsProvider) fetchCredentials(ctx context.Context) (Credentials, error) {
	data, err := p.executeProcess(ctx)
	if err != nil {
		return Credentials{}, err
	}

	//json to Credentials
	result := &processCredentialsResult{}
	if err = json.Unmarshal(data, result); err != nil {
		return Credentials{}, err

	}

	creds := Credentials{
		AccessKeyID:     result.AccessKeyId,
		AccessKeySecret: result.AccessKeySecret,
		SecurityToken:   result.SecurityToken,
		Expires:         result.Expiration,
	}

	if !creds.HasKeys() {
		return creds, fmt.Errorf("missing AccessKeyId or AccessKeySecret in process output")
	}

	return creds, nil
}

func (p *ProcessCredentialsProvider) executeProcess(ctx context.Context) ([]byte, error) {
	if p.timeout >= 0 {
		var cancelFunc func()
		ctx, cancelFunc = context.WithTimeout(ctx, p.timeout)
		defer cancelFunc()
	}

	cmd, err := p.buildCommand(ctx)
	if err != nil {
		return nil, err
	}

	// get creds from process's stdout
	output := bytes.NewBuffer(make([]byte, 0, int(8*1024)))
	cmd.Stdout = output

	// Start the command
	executeFn := func(cmd *exec.Cmd, exec chan error) {
		err := cmd.Start()
		if err == nil {
			err = cmd.Wait()
		}
		exec <- err
	}

	execCh := make(chan error, 1)
	go executeFn(cmd, execCh)

	// Wait commnd done
	select {
	case execError := <-execCh:
		if execError == nil {
			break
		}
		select {
		case <-ctx.Done():
			return output.Bytes(), fmt.Errorf("credential process timed out: %w", execError)
		default:
			return output.Bytes(), fmt.Errorf("error in credential_process: %w", execError)
		}
	}

	out := output.Bytes()
	if runtime.GOOS == "windows" {
		// windows adds slashes to quotes
		out = bytes.ReplaceAll(out, []byte(`\"`), []byte(`"`))
	}

	return out, nil
}
