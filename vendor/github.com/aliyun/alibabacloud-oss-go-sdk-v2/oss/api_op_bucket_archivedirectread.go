package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type ArchiveDirectReadConfiguration struct {
	// Specifies whether to enable real-time access of Archive objects for a bucket. Valid values:- true- false
	Enabled *bool `xml:"Enabled"`
}

type GetBucketArchiveDirectReadRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketArchiveDirectReadResult struct {
	// The container that stores the configurations for real-time access of Archive objects.
	ArchiveDirectReadConfiguration *ArchiveDirectReadConfiguration `output:"body,ArchiveDirectReadConfiguration,xml"`

	ResultCommon
}

// GetBucketArchiveDirectRead Queries whether real-time access of Archive objects is enabled for a bucket.
func (c *Client) GetBucketArchiveDirectRead(ctx context.Context, request *GetBucketArchiveDirectReadRequest, optFns ...func(*Options)) (*GetBucketArchiveDirectReadResult, error) {
	var err error
	if request == nil {
		request = &GetBucketArchiveDirectReadRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketArchiveDirectRead",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"bucketArchiveDirectRead": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"bucketArchiveDirectRead"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &GetBucketArchiveDirectReadResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type PutBucketArchiveDirectReadRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body.
	ArchiveDirectReadConfiguration *ArchiveDirectReadConfiguration `input:"body,ArchiveDirectReadConfiguration,xml,required"`

	RequestCommon
}

type PutBucketArchiveDirectReadResult struct {
	ResultCommon
}

// PutBucketArchiveDirectRead Enables or disables real-time access of Archive objects for a bucket.
func (c *Client) PutBucketArchiveDirectRead(ctx context.Context, request *PutBucketArchiveDirectReadRequest, optFns ...func(*Options)) (*PutBucketArchiveDirectReadResult, error) {
	var err error
	if request == nil {
		request = &PutBucketArchiveDirectReadRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketArchiveDirectRead",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"bucketArchiveDirectRead": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"bucketArchiveDirectRead"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &PutBucketArchiveDirectReadResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}
