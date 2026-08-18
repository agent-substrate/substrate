package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type InitiateWormConfiguration struct {
	// The number of days for which objects can be retained.
	RetentionPeriodInDays *int32 `xml:"RetentionPeriodInDays"`
}

type ExtendWormConfiguration struct {
	// The number of days for which objects can be retained.
	RetentionPeriodInDays *int32 `xml:"RetentionPeriodInDays"`
}

type WormConfiguration struct {
	// The ID of the retention policy.&gt;Note If the specified retention policy ID that is used to query the retention policy configurations of the bucket does not exist, OSS returns the 404 error code.
	WormId *string `xml:"WormId"`

	// The status of the retention policy. Valid values:- InProgress: indicates that the retention policy is in the InProgress state. By default, a retention policy is in the InProgress state after it is created. The policy remains in this state for 24 hours.- Locked: indicates that the retention policy is in the Locked state.
	State BucketWormStateType `xml:"State"`

	// The number of days for which objects can be retained.
	RetentionPeriodInDays *int32 `xml:"RetentionPeriodInDays"`

	// The time at which the retention policy was created.
	CreationDate *string `xml:"CreationDate"`

	// The time at which the retention policy will be expired.
	//ExpirationDate *string `xml:"ExpirationDate"`
}

type InitiateBucketWormRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The container of the request body.
	InitiateWormConfiguration *InitiateWormConfiguration `input:"body,InitiateWormConfiguration,xml,required"`

	RequestCommon
}

type InitiateBucketWormResult struct {
	// The ID of the retention policy.
	WormId *string `output:"header,x-oss-worm-id"`

	ResultCommon
}

// InitiateBucketWorm Creates a retention policy.
func (c *Client) InitiateBucketWorm(ctx context.Context, request *InitiateBucketWormRequest, optFns ...func(*Options)) (*InitiateBucketWormResult, error) {
	var err error
	if request == nil {
		request = &InitiateBucketWormRequest{}
	}
	input := &OperationInput{
		OpName: "InitiateBucketWorm",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"worm": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"worm"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &InitiateBucketWormResult{}
	if err = c.unmarshalOutput(result, output, unmarshalHeader, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type AbortBucketWormRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type AbortBucketWormResult struct {
	ResultCommon
}

// AbortBucketWorm Deletes an unlocked retention policy for a bucket.
func (c *Client) AbortBucketWorm(ctx context.Context, request *AbortBucketWormRequest, optFns ...func(*Options)) (*AbortBucketWormResult, error) {
	var err error
	if request == nil {
		request = &AbortBucketWormRequest{}
	}
	input := &OperationInput{
		OpName: "AbortBucketWorm",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"worm": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"worm"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &AbortBucketWormResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type CompleteBucketWormRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The ID of the retention policy.
	WormId *string `input:"query,wormId,required"`

	RequestCommon
}

type CompleteBucketWormResult struct {
	ResultCommon
}

// CompleteBucketWorm Locks a retention policy.
func (c *Client) CompleteBucketWorm(ctx context.Context, request *CompleteBucketWormRequest, optFns ...func(*Options)) (*CompleteBucketWormResult, error) {
	var err error
	if request == nil {
		request = &CompleteBucketWormRequest{}
	}
	input := &OperationInput{
		OpName: "CompleteBucketWorm",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"wormId"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &CompleteBucketWormResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type ExtendBucketWormRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The ID of the retention policy.&gt;  If the ID of the retention policy that specifies the number of days for which objects can be retained does not exist, the HTTP status code 404 is returned.
	WormId *string `input:"query,wormId,required"`

	// The container of the request body.
	ExtendWormConfiguration *ExtendWormConfiguration `input:"body,ExtendWormConfiguration,xml,required"`

	RequestCommon
}

type ExtendBucketWormResult struct {
	ResultCommon
}

// ExtendBucketWorm Extends the retention period of objects in a bucket for which a retention policy is locked.
func (c *Client) ExtendBucketWorm(ctx context.Context, request *ExtendBucketWormRequest, optFns ...func(*Options)) (*ExtendBucketWormResult, error) {
	var err error
	if request == nil {
		request = &ExtendBucketWormRequest{}
	}
	input := &OperationInput{
		OpName: "ExtendBucketWorm",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"wormExtend": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"wormExtend", "wormId"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ExtendBucketWormResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type GetBucketWormRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketWormResult struct {
	// The container that stores the information about retention policies of the bucket.
	WormConfiguration *WormConfiguration `output:"body,WormConfiguration,xml"`

	ResultCommon
}

// GetBucketWorm Queries the retention policy configured for a bucket.
func (c *Client) GetBucketWorm(ctx context.Context, request *GetBucketWormRequest, optFns ...func(*Options)) (*GetBucketWormResult, error) {
	var err error
	if request == nil {
		request = &GetBucketWormRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketWorm",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"worm": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"worm"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketWormResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}
