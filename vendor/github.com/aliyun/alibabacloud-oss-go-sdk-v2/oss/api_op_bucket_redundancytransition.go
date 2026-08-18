package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type BucketDataRedundancyTransition struct {
	// The progress of the redundancy type change task in percentage. Valid values: 0 to 100. This element is available when the task is in the Processing or Finished state.
	ProcessPercentage *int32 `xml:"ProcessPercentage"`

	// The estimated period of time that is required for the redundancy type change task. Unit: hours. This element is available when the task is in the Processing or Finished state.
	EstimatedRemainingTime *int64 `xml:"EstimatedRemainingTime"`

	// The name of the bucket.
	Bucket *string `xml:"Bucket"`

	// The ID of the redundancy type change task.
	TaskId *string `xml:"TaskId"`

	// The state of the redundancy type change task. Valid values:QueueingProcessingFinished
	Status *string `xml:"Status"`

	// The time when the redundancy type change task was created.
	CreateTime *string `xml:"CreateTime"`

	// The time when the redundancy type change task was performed. This element is available when the task is in the Processing or Finished state.
	StartTime *string `xml:"StartTime"`

	// The time when the redundancy type change task was finished. This element is available when the task is in the Finished state.
	EndTime *string `xml:"EndTime"`
}

type ListBucketDataRedundancyTransition struct {
	// Indicates that this ListUserDataRedundancyTransition request contains subsequent results.
	// You must set NextContinuationToken to continuation-token to continue obtaining the results.
	NextContinuationToken *string `xml:"NextContinuationToken"`

	// The container in which the redundancy type conversion task is stored.
	BucketDataRedundancyTransitions []BucketDataRedundancyTransition `xml:"BucketDataRedundancyTransition"`

	// Indicates whether the returned results are truncated.
	// Valid values:true: indicates that not all results are returned for the request.
	// false: indicates that all results are returned for the request.
	IsTruncated *bool `xml:"IsTruncated"`
}

type ListBucketDataRedundancyTransitionRequest struct {
	// The name of the bucket
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type ListBucketDataRedundancyTransitionResult struct {
	// The container for listed redundancy type change tasks.
	ListBucketDataRedundancyTransition *ListBucketDataRedundancyTransition `output:"body,ListBucketDataRedundancyTransition,xml"`

	ResultCommon
}

// ListBucketDataRedundancyTransition Lists all redundancy type conversion tasks of a bucket.
func (c *Client) ListBucketDataRedundancyTransition(ctx context.Context, request *ListBucketDataRedundancyTransitionRequest, optFns ...func(*Options)) (*ListBucketDataRedundancyTransitionResult, error) {
	var err error
	if request == nil {
		request = &ListBucketDataRedundancyTransitionRequest{}
	}
	input := &OperationInput{
		OpName: "ListBucketDataRedundancyTransition",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"redundancyTransition": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"redundancyTransition"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListBucketDataRedundancyTransitionResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketDataRedundancyTransitionRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The ID of the redundancy change task.
	RedundancyTransitionTaskid *string `input:"query,x-oss-redundancy-transition-taskid,required"`

	RequestCommon
}

type GetBucketDataRedundancyTransitionResult struct {
	// The container for a specific redundancy type change task.
	BucketDataRedundancyTransition *BucketDataRedundancyTransition `output:"body,BucketDataRedundancyTransition,xml"`

	ResultCommon
}

// GetBucketDataRedundancyTransition Queries the redundancy type conversion tasks of a bucket.
func (c *Client) GetBucketDataRedundancyTransition(ctx context.Context, request *GetBucketDataRedundancyTransitionRequest, optFns ...func(*Options)) (*GetBucketDataRedundancyTransitionResult, error) {
	var err error
	if request == nil {
		request = &GetBucketDataRedundancyTransitionRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketDataRedundancyTransition",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"redundancyTransition": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"redundancyTransition"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketDataRedundancyTransitionResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type CreateBucketDataRedundancyTransitionRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The redundancy type to which you want to convert the bucket. You can only convert the redundancy type of a bucket from LRS to ZRS.
	TargetRedundancyType *string `input:"query,x-oss-target-redundancy-type,required"`

	RequestCommon
}

type CreateBucketDataRedundancyTransitionResult struct {
	// The container in which the redundancy type conversion task is stored.
	BucketDataRedundancyTransition *BucketDataRedundancyTransition `output:"body,BucketDataRedundancyTransition,xml"`

	ResultCommon
}

// CreateBucketDataRedundancyTransition Creates a redundancy type conversion task for a bucket.
func (c *Client) CreateBucketDataRedundancyTransition(ctx context.Context, request *CreateBucketDataRedundancyTransitionRequest, optFns ...func(*Options)) (*CreateBucketDataRedundancyTransitionResult, error) {
	var err error
	if request == nil {
		request = &CreateBucketDataRedundancyTransitionRequest{}
	}
	input := &OperationInput{
		OpName: "CreateBucketDataRedundancyTransition",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"redundancyTransition": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"redundancyTransition"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &CreateBucketDataRedundancyTransitionResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteBucketDataRedundancyTransitionRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The ID of the redundancy type change task.
	RedundancyTransitionTaskid *string `input:"query,x-oss-redundancy-transition-taskid,required"`

	RequestCommon
}

type DeleteBucketDataRedundancyTransitionResult struct {
	ResultCommon
}

// DeleteBucketDataRedundancyTransition Deletes a redundancy type conversion task of a bucket.
func (c *Client) DeleteBucketDataRedundancyTransition(ctx context.Context, request *DeleteBucketDataRedundancyTransitionRequest, optFns ...func(*Options)) (*DeleteBucketDataRedundancyTransitionResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketDataRedundancyTransitionRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucketDataRedundancyTransition",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"redundancyTransition": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"redundancyTransition"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteBucketDataRedundancyTransitionResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type ListUserDataRedundancyTransitionRequest struct {
	// The token from which the list operation must start.
	ContinuationToken *string `input:"query,continuation-token"`

	// The maximum number of redundancy type conversion tasks that can be returned. Valid values: 1 to 100.
	MaxKeys int32 `input:"query,max-keys"`

	RequestCommon
}

type ListUserDataRedundancyTransitionResult struct {
	// The container in which the listed redundancy type conversion tasks are stored.
	ListBucketDataRedundancyTransition *ListBucketDataRedundancyTransition `output:"body,ListBucketDataRedundancyTransition,xml"`

	ResultCommon
}

// ListUserDataRedundancyTransition 列举请求者所有的存储冗余转换任务
func (c *Client) ListUserDataRedundancyTransition(ctx context.Context, request *ListUserDataRedundancyTransitionRequest, optFns ...func(*Options)) (*ListUserDataRedundancyTransitionResult, error) {
	var err error
	if request == nil {
		request = &ListUserDataRedundancyTransitionRequest{}
	}
	input := &OperationInput{
		OpName: "ListUserDataRedundancyTransition",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"redundancyTransition": "",
		},
	}

	input.OpMetadata.Set(signer.SubResource, []string{"redundancyTransition"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListUserDataRedundancyTransitionResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
