package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type GetAccessPointPublicAccessBlockRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the access point.
	AccessPointName *string `input:"query,x-oss-access-point-name,required"`

	RequestCommon
}

type GetAccessPointPublicAccessBlockResult struct {
	// The container in which the Block Public Access configurations are stored.
	PublicAccessBlockConfiguration *PublicAccessBlockConfiguration `output:"body,PublicAccessBlockConfiguration,xml"`

	ResultCommon
}

// GetAccessPointPublicAccessBlock Queries the Block Public Access configurations of an access point.
func (c *Client) GetAccessPointPublicAccessBlock(ctx context.Context, request *GetAccessPointPublicAccessBlockRequest, optFns ...func(*Options)) (*GetAccessPointPublicAccessBlockResult, error) {
	var err error
	if request == nil {
		request = &GetAccessPointPublicAccessBlockRequest{}
	}
	input := &OperationInput{
		OpName: "GetAccessPointPublicAccessBlock",
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

	result := &GetAccessPointPublicAccessBlockResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutAccessPointPublicAccessBlockRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the access point.
	AccessPointName *string `input:"query,x-oss-access-point-name,required"`

	// The request body.
	PublicAccessBlockConfiguration *PublicAccessBlockConfiguration `input:"body,PublicAccessBlockConfiguration,xml,required"`

	RequestCommon
}

type PutAccessPointPublicAccessBlockResult struct {
	ResultCommon
}

// PutAccessPointPublicAccessBlock Enables or disables Block Public Access for an access point.
func (c *Client) PutAccessPointPublicAccessBlock(ctx context.Context, request *PutAccessPointPublicAccessBlockRequest, optFns ...func(*Options)) (*PutAccessPointPublicAccessBlockResult, error) {
	var err error
	if request == nil {
		request = &PutAccessPointPublicAccessBlockRequest{}
	}
	input := &OperationInput{
		OpName: "PutAccessPointPublicAccessBlock",
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

	result := &PutAccessPointPublicAccessBlockResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteAccessPointPublicAccessBlockRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the access point.
	AccessPointName *string `input:"query,x-oss-access-point-name,required"`

	RequestCommon
}

type DeleteAccessPointPublicAccessBlockResult struct {
	ResultCommon
}

// DeleteAccessPointPublicAccessBlock Deletes the Block Public Access configurations of an access point.
func (c *Client) DeleteAccessPointPublicAccessBlock(ctx context.Context, request *DeleteAccessPointPublicAccessBlockRequest, optFns ...func(*Options)) (*DeleteAccessPointPublicAccessBlockResult, error) {
	var err error
	if request == nil {
		request = &DeleteAccessPointPublicAccessBlockRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteAccessPointPublicAccessBlock",
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

	result := &DeleteAccessPointPublicAccessBlockResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
