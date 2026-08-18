package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type PutBucketTagsRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	Tagging *Tagging `input:"body,Tagging,xml,required"`

	RequestCommon
}

type PutBucketTagsResult struct {
	ResultCommon
}

// PutBucketTags Adds tags to or modifies the existing tags of a bucket.
func (c *Client) PutBucketTags(ctx context.Context, request *PutBucketTagsRequest, optFns ...func(*Options)) (*PutBucketTagsResult, error) {
	var err error
	if request == nil {
		request = &PutBucketTagsRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketTags",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"tagging": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"tagging"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &PutBucketTagsResult{}
	if err = c.unmarshalOutput(result, output, discardBody); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketTagsRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketTagsResult struct {
	// The container that stores the returned tags of the bucket. If no tags are configured for the bucket, an XML message body is returned in which the Tagging element is empty.
	Tagging *Tagging `output:"body,Tagging,xml"`

	ResultCommon
}

// GetBucketTags Queries the tags of a bucket.
func (c *Client) GetBucketTags(ctx context.Context, request *GetBucketTagsRequest, optFns ...func(*Options)) (*GetBucketTagsResult, error) {
	var err error
	if request == nil {
		request = &GetBucketTagsRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketTags",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"tagging": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"tagging"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &GetBucketTagsResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type DeleteBucketTagsRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	Tagging *string `input:"query,tagging"`

	RequestCommon
}

type DeleteBucketTagsResult struct {
	ResultCommon
}

// DeleteBucketTags Deletes tags configured for a bucket.
func (c *Client) DeleteBucketTags(ctx context.Context, request *DeleteBucketTagsRequest, optFns ...func(*Options)) (*DeleteBucketTagsResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketTagsRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucketTags",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"tagging": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"tagging"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &DeleteBucketTagsResult{}
	if err = c.unmarshalOutput(result, output, discardBody); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}
