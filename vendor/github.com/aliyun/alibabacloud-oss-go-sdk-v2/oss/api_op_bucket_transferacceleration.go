package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type TransferAccelerationConfiguration struct {
	// Whether the transfer acceleration is enabled for this bucket.
	Enabled *bool `xml:"Enabled"`
}

type PutBucketTransferAccelerationRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The container of the request body.
	TransferAccelerationConfiguration *TransferAccelerationConfiguration `input:"body,TransferAccelerationConfiguration,xml,required"`

	RequestCommon
}

type PutBucketTransferAccelerationResult struct {
	ResultCommon
}

// PutBucketTransferAcceleration Configures transfer acceleration for a bucket. After you enable transfer acceleration for a bucket, the object access speed is accelerated for users worldwide. The transfer acceleration feature is applicable to scenarios where data needs to be transferred over long geographical distances. This feature can also be used to download or upload objects that are gigabytes or terabytes in size.
func (c *Client) PutBucketTransferAcceleration(ctx context.Context, request *PutBucketTransferAccelerationRequest, optFns ...func(*Options)) (*PutBucketTransferAccelerationResult, error) {
	var err error
	if request == nil {
		request = &PutBucketTransferAccelerationRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketTransferAcceleration",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"transferAcceleration": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"transferAcceleration"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketTransferAccelerationResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketTransferAccelerationRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketTransferAccelerationResult struct {

	// The container that stores the transfer acceleration configurations.
	TransferAccelerationConfiguration *TransferAccelerationConfiguration `output:"body,TransferAccelerationConfiguration,xml"`

	ResultCommon
}

// GetBucketTransferAcceleration Queries the transfer acceleration configurations of a bucket.
func (c *Client) GetBucketTransferAcceleration(ctx context.Context, request *GetBucketTransferAccelerationRequest, optFns ...func(*Options)) (*GetBucketTransferAccelerationResult, error) {
	var err error
	if request == nil {
		request = &GetBucketTransferAccelerationRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketTransferAcceleration",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"transferAcceleration": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"transferAcceleration"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketTransferAccelerationResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
