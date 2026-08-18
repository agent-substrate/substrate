package oss

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type PresignOptions struct {
	// Expires sets the expiration duration for the generated presign url.
	Expires time.Duration

	// Expiration sets the expiration time for the generated presign url.
	Expiration time.Time
}

type PresignResult struct {
	Method        string
	URL           string
	Expiration    time.Time
	SignedHeaders map[string]string
}

type nopHttpClient struct {
}

func (c *nopHttpClient) Do(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: 200,
		Header:     http.Header{},
		Body:       http.NoBody,
	}, nil
}

var (
	defaultNopHttpClient  = &nopHttpClient{}
	defaultPresignOptions = []func(*Options){
		func(o *Options) {
			o.HttpClient = defaultNopHttpClient
			o.AuthMethod = Ptr(AuthMethodQuery)
		},
	}
)

func (c *Client) Presign(ctx context.Context, request any, optFns ...func(*PresignOptions)) (*PresignResult, error) {
	options := PresignOptions{}

	if request == nil {
		return nil, NewErrParamNull("request")
	}

	for _, fn := range optFns {
		fn(&options)
	}

	input := OperationInput{}
	if err := c.marshalPresignInput(request, &input); err != nil {
		return nil, err
	}

	// expiration
	if !options.Expiration.IsZero() {
		input.OpMetadata.Set(signer.SignTime, options.Expiration)
	} else if options.Expires > 0 {
		input.OpMetadata.Set(signer.SignTime, time.Now().Add(options.Expires))
	}
	output, err := c.invokeOperation(ctx, &input, defaultPresignOptions)
	if err != nil {
		return nil, err
	}

	result := &PresignResult{}
	err = c.unmarshalPresignOutput(result, output)
	return result, err
}

func PresignExpires(value time.Duration) func(*PresignOptions) {
	return func(o *PresignOptions) {
		o.Expires = value
	}
}

func PresignExpiration(value time.Time) func(*PresignOptions) {
	return func(o *PresignOptions) {
		o.Expiration = value
	}
}

func (c *Client) marshalPresignInput(request any, input *OperationInput) error {
	switch t := request.(type) {
	case *GetObjectRequest:
		input.OpName = "GetObject"
		input.Method = "GET"
		input.Bucket = t.Bucket
		input.Key = t.Key
	case *PutObjectRequest:
		input.OpName = "PutObject"
		input.Method = "PUT"
		input.Bucket = t.Bucket
		input.Key = t.Key
	case *HeadObjectRequest:
		input.OpName = "HeadObject"
		input.Method = "HEAD"
		input.Bucket = t.Bucket
		input.Key = t.Key
	case *InitiateMultipartUploadRequest:
		input.OpName = "InitiateMultipartUpload"
		input.Method = "POST"
		input.Bucket = t.Bucket
		input.Key = t.Key
		input.Parameters = map[string]string{
			"uploads": "",
		}
	case *UploadPartRequest:
		input.OpName = "UploadPart"
		input.Method = "PUT"
		input.Bucket = t.Bucket
		input.Key = t.Key
	case *CompleteMultipartUploadRequest:
		input.OpName = "CompleteMultipartUpload"
		input.Method = "POST"
		input.Bucket = t.Bucket
		input.Key = t.Key
	case *AbortMultipartUploadRequest:
		input.OpName = "AbortMultipartUpload"
		input.Method = "DELETE"
		input.Bucket = t.Bucket
		input.Key = t.Key
	default:
		return NewErrParamInvalid(fmt.Sprintf("request %v", reflect.ValueOf(request).Type().String()))
	}

	return c.marshalInput(request, input)
}

func (c *Client) unmarshalPresignOutput(result *PresignResult, output *OperationOutput) error {
	if chk, ok := c.options.Signer.(interface{ IsSignedHeader([]string, string) bool }); ok {
		header := map[string]string{}
		for k, v := range output.httpRequest.Header {
			if chk.IsSignedHeader(c.options.AdditionalHeaders, k) {
				header[k] = v[0]
			}
		}
		if len(header) > 0 {
			result.SignedHeaders = header
		}
	}
	result.Method = output.httpRequest.Method
	result.URL = output.httpRequest.URL.String()
	if signTime, ok := output.OpMetadata.Get(signer.SignTime).(time.Time); ok {
		result.Expiration = signTime
	}
	_, ok := c.options.Signer.(*signer.SignerV4)
	if ok {
		if !result.Expiration.IsZero() && (result.Expiration.After(time.Now().Add(7 * 24 * time.Hour))) {
			return fmt.Errorf("expires should be not greater than 604800(seven days)")
		}
	}
	return nil
}
