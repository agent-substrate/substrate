package oss

import (
	"context"
	"io"
	"io/ioutil"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type PolicyStatus struct {
	// Indicates whether the current bucket policy allows public access.true false
	IsPublic *bool `xml:"IsPublic"`
}

type PutBucketPolicyRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request parameters.
	Body io.Reader `input:"body,nop,required"`

	RequestCommon
}

type PutBucketPolicyResult struct {
	ResultCommon
}

// PutBucketPolicy Configures a policy for a bucket.
func (c *Client) PutBucketPolicy(ctx context.Context, request *PutBucketPolicyRequest, optFns ...func(*Options)) (*PutBucketPolicyResult, error) {
	var err error
	if request == nil {
		request = &PutBucketPolicyRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketPolicy",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeJSON,
		},
		Parameters: map[string]string{
			"policy": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"policy"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketPolicyResult{}
	if err = c.unmarshalOutput(result, output, discardBody); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type GetBucketPolicyRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketPolicyResult struct {
	// The configurations of the bucket policy.
	Body string

	ResultCommon
}

// GetBucketPolicy Queries the policies configured for a bucket.
func (c *Client) GetBucketPolicy(ctx context.Context, request *GetBucketPolicyRequest, optFns ...func(*Options)) (*GetBucketPolicyResult, error) {
	var err error
	if request == nil {
		request = &GetBucketPolicyRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketPolicy",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"policy": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"policy"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(output.Body)
	defer output.Body.Close()
	if err != nil {
		return nil, err
	}
	result := &GetBucketPolicyResult{
		Body: string(body),
	}
	if err = c.unmarshalOutput(result, output); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type DeleteBucketPolicyRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type DeleteBucketPolicyResult struct {
	ResultCommon
}

// DeleteBucketPolicy Deletes a policy for a bucket.
func (c *Client) DeleteBucketPolicy(ctx context.Context, request *DeleteBucketPolicyRequest, optFns ...func(*Options)) (*DeleteBucketPolicyResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketPolicyRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucketPolicy",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"policy": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"policy"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteBucketPolicyResult{}
	if err = c.unmarshalOutput(result, output, discardBody); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type GetBucketPolicyStatusRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketPolicyStatusResult struct {
	// The container that stores public access information.
	PolicyStatus *PolicyStatus `output:"body,PolicyStatus,xml"`

	ResultCommon
}

// GetBucketPolicyStatus Checks whether the current bucket policy allows public access.
func (c *Client) GetBucketPolicyStatus(ctx context.Context, request *GetBucketPolicyStatusRequest, optFns ...func(*Options)) (*GetBucketPolicyStatusResult, error) {
	var err error
	if request == nil {
		request = &GetBucketPolicyStatusRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketPolicyStatus",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"policyStatus": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"policyStatus"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketPolicyStatusResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}
