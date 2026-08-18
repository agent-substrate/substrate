package signer

import (
	"context"
	"net/http"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
)

const (
	SubResource = "SubResource"
	SignTime    = "SignTime"
)

type SigningContext struct {
	//input
	Product *string
	Region  *string
	Bucket  *string
	Key     *string
	Request *http.Request

	SubResource       []string
	AdditionalHeaders []string

	Credentials *credentials.Credentials

	AuthMethodQuery bool

	// input and output
	Time        time.Time
	ClockOffset time.Duration

	// output
	SignedHeaders map[string]string
	StringToSign  string

	// for test
	signTime *time.Time
}

type Signer interface {
	Sign(ctx context.Context, signingCtx *SigningContext) error
}

type NopSigner struct{}

func (*NopSigner) Sign(ctx context.Context, signingCtx *SigningContext) error {
	return nil
}
