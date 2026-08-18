package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type ApplyServerSideEncryptionByDefault struct {
	// The default server-side encryption method. Valid values: KMS, AES256, and SM4. You are charged when you call API operations to encrypt or decrypt data by using CMKs managed by KMS. For more information, see [Billing of KMS](~~52608~~). If the default server-side encryption method is configured for the destination bucket and ReplicaCMKID is configured in the CRR rule:*   If objects in the source bucket are not encrypted, they are encrypted by using the default encryption method of the destination bucket after they are replicated.*   If objects in the source bucket are encrypted by using SSE-KMS or SSE-OSS, they are encrypted by using the same method after they are replicated.For more information, see [Use data replication with server-side encryption](~~177216~~).
	SSEAlgorithm *string `xml:"SSEAlgorithm"`

	// The CMK ID that is specified when SSEAlgorithm is set to KMS and a specified CMK is used for encryption. In other cases, leave this parameter empty.
	KMSMasterKeyID *string `xml:"KMSMasterKeyID"`

	// The algorithm that is used to encrypt objects. If this parameter is not specified, objects are encrypted by using AES256. This parameter is valid only when SSEAlgorithm is set to KMS. Valid value: SM4.
	KMSDataEncryption *string `xml:"KMSDataEncryption"`
}

type ServerSideEncryptionRule struct {
	// The container that stores the default server-side encryption method.
	ApplyServerSideEncryptionByDefault *ApplyServerSideEncryptionByDefault `xml:"ApplyServerSideEncryptionByDefault"`
}

type PutBucketEncryptionRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	ServerSideEncryptionRule *ServerSideEncryptionRule `input:"body,ServerSideEncryptionRule,xml,required"`

	RequestCommon
}

type PutBucketEncryptionResult struct {
	ResultCommon
}

// PutBucketEncryption Configures encryption rules for a bucket.
func (c *Client) PutBucketEncryption(ctx context.Context, request *PutBucketEncryptionRequest, optFns ...func(*Options)) (*PutBucketEncryptionResult, error) {
	var err error
	if request == nil {
		request = &PutBucketEncryptionRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketEncryption",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"encryption": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"encryption"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketEncryptionResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketEncryptionRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketEncryptionResult struct {
	// The container that stores server-side encryption rules.
	ServerSideEncryptionRule *ServerSideEncryptionRule `output:"body,ServerSideEncryptionRule,xml"`

	ResultCommon
}

// GetBucketEncryption Queries the encryption rules configured for a bucket.
func (c *Client) GetBucketEncryption(ctx context.Context, request *GetBucketEncryptionRequest, optFns ...func(*Options)) (*GetBucketEncryptionResult, error) {
	var err error
	if request == nil {
		request = &GetBucketEncryptionRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketEncryption",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"encryption": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"encryption"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketEncryptionResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteBucketEncryptionRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type DeleteBucketEncryptionResult struct {
	ResultCommon
}

// DeleteBucketEncryption Deletes encryption rules for a bucket.
func (c *Client) DeleteBucketEncryption(ctx context.Context, request *DeleteBucketEncryptionRequest, optFns ...func(*Options)) (*DeleteBucketEncryptionResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketEncryptionRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucketEncryption",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"encryption": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"encryption"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteBucketEncryptionResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
