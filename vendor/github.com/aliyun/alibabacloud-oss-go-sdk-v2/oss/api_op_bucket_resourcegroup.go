package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type BucketResourceGroupConfiguration struct {
	// The ID of the resource group to which the bucket belongs.
	ResourceGroupId *string `xml:"ResourceGroupId" json:"ResourceGroupId"`
}

type GetBucketResourceGroupRequest struct {
	// The name of the bucket that you want to query.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketResourceGroupResult struct {
	// The container that stores the ID of the resource group.
	BucketResourceGroupConfiguration *BucketResourceGroupConfiguration `output:"body,BucketResourceGroupConfiguration,xml"`

	ResultCommon
}

// GetBucketResourceGroup Queries the ID of the resource group to which a bucket belongs.
func (c *Client) GetBucketResourceGroup(ctx context.Context, request *GetBucketResourceGroupRequest, optFns ...func(*Options)) (*GetBucketResourceGroupResult, error) {
	var err error
	if request == nil {
		request = &GetBucketResourceGroupRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketResourceGroup",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"resourceGroup": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"resourceGroup"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &GetBucketResourceGroupResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type PutBucketResourceGroupRequest struct {
	// The bucket for which you want to modify the ID of the resource group.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	BucketResourceGroupConfiguration *BucketResourceGroupConfiguration `input:"body,BucketResourceGroupConfiguration,xml,required"`

	RequestCommon
}

type PutBucketResourceGroupResult struct {
	ResultCommon
}

// PutBucketResourceGroup Modifies the ID of the resource group to which a bucket belongs.
func (c *Client) PutBucketResourceGroup(ctx context.Context, request *PutBucketResourceGroupRequest, optFns ...func(*Options)) (*PutBucketResourceGroupResult, error) {
	var err error
	if request == nil {
		request = &PutBucketResourceGroupRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketResourceGroup",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"resourceGroup": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"resourceGroup"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &PutBucketResourceGroupResult{}
	if err = c.unmarshalOutput(result, output, discardBody); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}
