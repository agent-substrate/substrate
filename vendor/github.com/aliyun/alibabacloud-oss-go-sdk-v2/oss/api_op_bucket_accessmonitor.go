package oss

import (
	"context"
)

type AccessMonitorConfiguration struct {
	// The access tracking status of the bucket. Valid values:- Enabled: Access tracking is enabled.- Disabled: Access tracking is disabled.
	Status AccessMonitorStatusType `xml:"Status"`
}

type PutBucketAccessMonitorRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	AccessMonitorConfiguration *AccessMonitorConfiguration `input:"body,AccessMonitorConfiguration,xml,required"`

	RequestCommon
}

type PutBucketAccessMonitorResult struct {
	ResultCommon
}

// PutBucketAccessMonitor Modifies the access tracking status of a bucket.
func (c *Client) PutBucketAccessMonitor(ctx context.Context, request *PutBucketAccessMonitorRequest, optFns ...func(*Options)) (*PutBucketAccessMonitorResult, error) {
	var err error
	if request == nil {
		request = &PutBucketAccessMonitorRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketAccessMonitor",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessmonitor": "",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketAccessMonitorResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketAccessMonitorRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketAccessMonitorResult struct {
	// The container that stores access monitor configuration.
	AccessMonitorConfiguration *AccessMonitorConfiguration `output:"body,AccessMonitorConfiguration,xml"`

	ResultCommon
}

// GetBucketAccessMonitor Queries the access tracking status of a bucket.
func (c *Client) GetBucketAccessMonitor(ctx context.Context, request *GetBucketAccessMonitorRequest, optFns ...func(*Options)) (*GetBucketAccessMonitorResult, error) {
	var err error
	if request == nil {
		request = &GetBucketAccessMonitorRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketAccessMonitor",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessmonitor": "",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketAccessMonitorResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
