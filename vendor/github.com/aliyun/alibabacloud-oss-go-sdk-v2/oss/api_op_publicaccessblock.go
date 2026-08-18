package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type PublicAccessBlockConfiguration struct {
	// Specifies whether to enable Block Public Access.true: enables Block Public Access.false (default): disables Block Public Access.
	BlockPublicAccess *bool `xml:"BlockPublicAccess"`
}

type GetPublicAccessBlockRequest struct {
	RequestCommon
}

type GetPublicAccessBlockResult struct {
	// The container in which the Block Public Access configurations are stored.
	PublicAccessBlockConfiguration *PublicAccessBlockConfiguration `output:"body,PublicAccessBlockConfiguration,xml"`

	ResultCommon
}

// GetPublicAccessBlock Queries the Block Public Access configurations of OSS resources.
func (c *Client) GetPublicAccessBlock(ctx context.Context, request *GetPublicAccessBlockRequest, optFns ...func(*Options)) (*GetPublicAccessBlockResult, error) {
	var err error
	if request == nil {
		request = &GetPublicAccessBlockRequest{}
	}
	input := &OperationInput{
		OpName: "GetPublicAccessBlock",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"publicAccessBlock": "",
		},
	}
	input.OpMetadata.Set(signer.SubResource, []string{"publicAccessBlock"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetPublicAccessBlockResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutPublicAccessBlockRequest struct {
	// Request body.
	PublicAccessBlockConfiguration *PublicAccessBlockConfiguration `input:"body,PublicAccessBlockConfiguration,xml,required"`

	RequestCommon
}

type PutPublicAccessBlockResult struct {
	ResultCommon
}

// PutPublicAccessBlock Enables or disables Block Public Access for Object Storage Service (OSS) resources.
func (c *Client) PutPublicAccessBlock(ctx context.Context, request *PutPublicAccessBlockRequest, optFns ...func(*Options)) (*PutPublicAccessBlockResult, error) {
	var err error
	if request == nil {
		request = &PutPublicAccessBlockRequest{}
	}
	input := &OperationInput{
		OpName: "PutPublicAccessBlock",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"publicAccessBlock": "",
		},
	}
	input.OpMetadata.Set(signer.SubResource, []string{"publicAccessBlock"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutPublicAccessBlockResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeletePublicAccessBlockRequest struct {
	RequestCommon
}

type DeletePublicAccessBlockResult struct {
	ResultCommon
}

// DeletePublicAccessBlock Deletes the Block Public Access configurations of OSS resources.
func (c *Client) DeletePublicAccessBlock(ctx context.Context, request *DeletePublicAccessBlockRequest, optFns ...func(*Options)) (*DeletePublicAccessBlockResult, error) {
	var err error
	if request == nil {
		request = &DeletePublicAccessBlockRequest{}
	}
	input := &OperationInput{
		OpName: "DeletePublicAccessBlock",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"publicAccessBlock": "",
		},
	}
	input.OpMetadata.Set(signer.SubResource, []string{"publicAccessBlock"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeletePublicAccessBlockResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
