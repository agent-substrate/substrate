package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type OverwriteRule struct {
	// The unique identifier of the rule. If you do not specify this element, a UUID is randomly generated. If you specify this element, the value must be unique. Different rules cannot have the same ID.
	ID *string `xml:"ID"`

	// The operation type. Currently, only `forbid` (prohibit overwrites) is supported.
	Action *string `xml:"Action"`

	// The prefix of object names to filter the objects that you want to process. The maximum length is 1,023 characters. Each rule can have at most one prefix. Prefixes and suffixes do not support regular expressions.
	Prefix *string `xml:"Prefix"`

	// The suffix of object names to filter the objects that you want to process. The maximum length is 1,023 characters. Each rule can have at most one suffix. Prefixes and suffixes do not support regular expressions.
	Suffix *string `xml:"Suffix"`

	// A collection of authorized entities. The usage is similar to the `Principal` element in a bucket policy. You can specify an Alibaba Cloud account, a RAM user, or a RAM role. If this element is empty or not configured, overwrites are prohibited for all objects that match the prefix and suffix conditions.
	Principals *OverwritePrincipals `xml:"Principals"`
}

type OverwritePrincipals struct {
	// A collection of authorized entities. The usage is similar to the `Principal` element in a bucket policy. You can specify an Alibaba Cloud account, a RAM user, or a RAM role. If this element is empty or not configured, overwrites are prohibited for all objects that match the prefix and suffix conditions.
	Principals []string `xml:"Principal"`
}

type OverwriteConfiguration struct {
	// List of overwrite protection rules. A bucket can have a maximum of 100 rules.
	Rules []OverwriteRule `xml:"Rule"`
}

type PutBucketOverwriteConfigRequest struct {
	// Bucket Name
	Bucket *string `input:"host,bucket,required"`

	// Structure of the API Request Body
	OverwriteConfiguration *OverwriteConfiguration `input:"body,OverwriteConfiguration,xml,required"`

	RequestCommon
}

type PutBucketOverwriteConfigResult struct {
	ResultCommon
}

// PutBucketOverwriteConfig Call the PutBucketOverwriteConfig operation to configure overwrite protection for a bucket. This prevents specified objects from being overwritten.
func (c *Client) PutBucketOverwriteConfig(ctx context.Context, request *PutBucketOverwriteConfigRequest, optFns ...func(*Options)) (*PutBucketOverwriteConfigResult, error) {
	var err error
	if request == nil {
		request = &PutBucketOverwriteConfigRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketOverwriteConfig",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"overwriteConfig": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"overwriteConfig"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketOverwriteConfigResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketOverwriteConfigRequest struct {
	// Bucket Name
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketOverwriteConfigResult struct {
	// Container for Saving Bucket Overwrite Rules
	OverwriteConfiguration *OverwriteConfiguration `output:"body,OverwriteConfiguration,xml"`

	ResultCommon
}

// GetBucketOverwriteConfig Call the GetBucketOverwriteConfig operation to retrieve the overwrite configuration of a bucket.
func (c *Client) GetBucketOverwriteConfig(ctx context.Context, request *GetBucketOverwriteConfigRequest, optFns ...func(*Options)) (*GetBucketOverwriteConfigResult, error) {
	var err error
	if request == nil {
		request = &GetBucketOverwriteConfigRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketOverwriteConfig",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"overwriteConfig": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"overwriteConfig"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketOverwriteConfigResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteBucketOverwriteConfigRequest struct {
	// Bucket name
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type DeleteBucketOverwriteConfigResult struct {
	ResultCommon
}

// DeleteBucketOverwriteConfig Delete overwrite configuration rule for the bucket.
func (c *Client) DeleteBucketOverwriteConfig(ctx context.Context, request *DeleteBucketOverwriteConfigRequest, optFns ...func(*Options)) (*DeleteBucketOverwriteConfigResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketOverwriteConfigRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucketOverwriteConfig",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"overwriteConfig": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"overwriteConfig"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteBucketOverwriteConfigResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
