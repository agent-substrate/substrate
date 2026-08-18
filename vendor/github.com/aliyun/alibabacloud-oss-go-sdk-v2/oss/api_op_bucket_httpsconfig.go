package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type TLS struct {
	// Specifies whether to enable TLS version management for the bucket.Valid values:*   true            *   false
	Enable *bool `xml:"Enable"`

	// The TLS versions.
	TLSVersions []string `xml:"TLSVersion"`
}

type HttpsConfiguration struct {
	// The container that stores TLS version configurations.
	TLS *TLS `xml:"TLS"`

	CipherSuite *CipherSuite `xml:"CipherSuite"`
}

type CipherSuite struct {
	Enable *bool `xml:"Enable"`

	StrongCipherSuite *bool `xml:"StrongCipherSuite"`

	CustomCipherSuites []string `xml:"CustomCipherSuite"`

	TLS13CustomCipherSuites []string `xml:"TLS13CustomCipherSuite"`
}

type GetBucketHttpsConfigRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketHttpsConfigResult struct {
	// The container that stores HTTPS configurations.
	HttpsConfiguration *HttpsConfiguration `output:"body,HttpsConfiguration,xml"`

	ResultCommon
}

// GetBucketHttpsConfig Queries the Transport Layer Security (TLS) version configurations of a bucket.
func (c *Client) GetBucketHttpsConfig(ctx context.Context, request *GetBucketHttpsConfigRequest, optFns ...func(*Options)) (*GetBucketHttpsConfigResult, error) {
	var err error
	if request == nil {
		request = &GetBucketHttpsConfigRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketHttpsConfig",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"httpsConfig": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"httpsConfig"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &GetBucketHttpsConfigResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type PutBucketHttpsConfigRequest struct {
	// This name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	HttpsConfiguration *HttpsConfiguration `input:"body,HttpsConfiguration,xml,required"`

	RequestCommon
}

type PutBucketHttpsConfigResult struct {
	ResultCommon
}

// PutBucketHttpsConfig Enables or disables Transport Layer Security (TLS) version management for a bucket.
func (c *Client) PutBucketHttpsConfig(ctx context.Context, request *PutBucketHttpsConfigRequest, optFns ...func(*Options)) (*PutBucketHttpsConfigResult, error) {
	var err error
	if request == nil {
		request = &PutBucketHttpsConfigRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketHttpsConfig",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"httpsConfig": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"httpsConfig"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketHttpsConfigResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}
