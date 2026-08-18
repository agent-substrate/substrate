package oss

import (
	"context"
	"io"
)

type DoMetaQueryActionRequest struct {
	Bucket *string `input:"host,bucket,required"`
	Action *string `input:"query,action,required"`

	Body io.Reader `input:"body,nop"`

	RequestCommon
}

type DoMetaQueryActionResult struct {
	Body io.ReadCloser

	ResultCommon
}

// DoMetaQueryAction meta query related api.
func (c *Client) DoMetaQueryAction(ctx context.Context, request *DoMetaQueryActionRequest, optFns ...func(*Options)) (*DoMetaQueryActionResult, error) {
	var err error
	if request == nil {
		request = &DoMetaQueryActionRequest{}
	}

	input := &OperationInput{
		OpName: "DoMetaQueryAction",
		Method: "POST",
		Headers: map[string]string{
			"Content-Type": "application/xml",
		},
		Parameters: map[string]string{
			"metaQuery": "",
		},
		Bucket: request.Bucket,
	}

	if err = c.marshalInput(request, input, MarshalUpdateContentMd5); err != nil {
		return nil, err
	}

	output, err := c.InvokeOperation(ctx, input, optFns...)
	if err != nil {
		return nil, err
	}

	result := &DoMetaQueryActionResult{
		Body: output.Body,
	}

	if err = c.unmarshalOutput(result, output); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, nil
}
