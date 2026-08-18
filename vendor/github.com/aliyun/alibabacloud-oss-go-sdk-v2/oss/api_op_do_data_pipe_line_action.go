package oss

import (
	"context"
	"io"
)

type DoDataPipeLineActionRequest struct {
	Action *string `input:"query,action,required"`

	Body io.Reader `input:"body,nop"`

	RequestCommon
}

type DoDataPipeLineActionResult struct {
	Body io.ReadCloser

	ResultCommon
}

// DoDataPipeLineAction data pipe line related api.
func (c *Client) DoDataPipeLineAction(ctx context.Context, request *DoDataPipeLineActionRequest, optFns ...func(*Options)) (*DoDataPipeLineActionResult, error) {
	var err error
	if request == nil {
		request = &DoDataPipeLineActionRequest{}
	}

	input := &OperationInput{
		OpName: "DoDataPipeLineAction",
		Method: "POST",
		Headers: map[string]string{
			"Content-Type": "application/xml",
		},
		Parameters: map[string]string{
			"dataPipeline": "",
		},
	}

	if err = c.marshalInput(request, input, MarshalUpdateContentMd5); err != nil {
		return nil, err
	}

	output, err := c.InvokeOperation(ctx, input, optFns...)
	if err != nil {
		return nil, err
	}

	result := &DoDataPipeLineActionResult{
		Body: output.Body,
	}

	if err = c.unmarshalOutput(result, output); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, nil
}
