package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type GetBucketPublicAccessBlockRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketPublicAccessBlockResult struct {
	// The container in which the Block Public Access configurations are stored.
	PublicAccessBlockConfiguration *PublicAccessBlockConfiguration `output:"body,PublicAccessBlockConfiguration,xml"`

	ResultCommon
}

// GetBucketPublicAccessBlock Queries the Block Public Access configurations of a bucket.
func (c *Client) GetBucketPublicAccessBlock(ctx context.Context, request *GetBucketPublicAccessBlockRequest, optFns ...func(*Options)) (*GetBucketPublicAccessBlockResult, error) {
	var err error
	if request == nil {
		request = &GetBucketPublicAccessBlockRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketPublicAccessBlock",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"publicAccessBlock": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"publicAccessBlock"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketPublicAccessBlockResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutBucketPublicAccessBlockRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// Request body.
	PublicAccessBlockConfiguration *PublicAccessBlockConfiguration `input:"body,PublicAccessBlockConfiguration,xml,required"`

	RequestCommon
}

type PutBucketPublicAccessBlockResult struct {
	ResultCommon
}

// PutBucketPublicAccessBlock Enables or disables Block Public Access for a bucket.
func (c *Client) PutBucketPublicAccessBlock(ctx context.Context, request *PutBucketPublicAccessBlockRequest, optFns ...func(*Options)) (*PutBucketPublicAccessBlockResult, error) {
	var err error
	if request == nil {
		request = &PutBucketPublicAccessBlockRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketPublicAccessBlock",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"publicAccessBlock": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"publicAccessBlock"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketPublicAccessBlockResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteBucketPublicAccessBlockRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type DeleteBucketPublicAccessBlockResult struct {
	ResultCommon
}

// DeleteBucketPublicAccessBlock Deletes the Block Public Access configurations of a bucket.
func (c *Client) DeleteBucketPublicAccessBlock(ctx context.Context, request *DeleteBucketPublicAccessBlockRequest, optFns ...func(*Options)) (*DeleteBucketPublicAccessBlockResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketPublicAccessBlockRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucketPublicAccessBlock",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"publicAccessBlock": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"publicAccessBlock"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteBucketPublicAccessBlockResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
