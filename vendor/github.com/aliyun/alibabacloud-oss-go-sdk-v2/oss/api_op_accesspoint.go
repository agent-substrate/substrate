package oss

import (
	"context"
	"io"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type AccessPointVpcConfiguration struct {
	// The ID of the VPC that is required only when the NetworkOrigin parameter is set to vpc.
	VpcId *string `xml:"VpcId"`
}

type CreateAccessPointConfiguration struct {
	// The name of the access point. The name of the access point must meet the following naming rules:*   The name must be unique in a region of your Alibaba Cloud account.*   The name cannot end with -ossalias.*   The name can contain only lowercase letters, digits, and hyphens (-). It cannot start or end with a hyphen (-).*   The name must be 3 to 19 characters in length.
	AccessPointName *string `xml:"AccessPointName"`

	// The network origin of the access point.
	NetworkOrigin *string `xml:"NetworkOrigin"`

	// The container that stores the information about the VPC.
	VpcConfiguration *AccessPointVpcConfiguration `xml:"VpcConfiguration"`
}

type ListAccessPointsRequest struct {
	// The maximum number of access points that can be returned. Valid values:*   For user-level access points: (0,1000].*   For bucket-level access points: (0,100].
	MaxKeys int64 `input:"query,max-keys"`

	// The token from which the listing operation starts. You must specify the value of NextContinuationToken that is obtained from the previous query as the value of continuation-token.
	ContinuationToken *string `input:"query,continuation-token"`

	// The name of the bucket.
	Bucket *string `input:"host,bucket"`

	RequestCommon
}

type AccessPoint struct {
	// The network origin of the access point.
	NetworkOrigin *string `xml:"NetworkOrigin"`

	// The container that stores the information about the VPC.
	VpcConfiguration *AccessPointVpcConfiguration `xml:"VpcConfiguration"`

	// The status of the access point.
	Status *string `xml:"Status"`

	// The name of the bucket for which the access point is configured.
	Bucket *string `xml:"Bucket"`

	// The name of the access point.
	AccessPointName *string `xml:"AccessPointName"`

	// The alias of the access point.
	Alias *string `xml:"Alias"`
}

type ListAccessPointsResult struct {
	// The maximum number of results set for this enumeration operation.
	MaxKeys *int32 `xml:"MaxKeys"`

	// Indicates whether the returned list is truncated. Valid values: * true: indicates that not all results are returned. * false: indicates that all results are returned.
	IsTruncated *bool `xml:"IsTruncated"`

	// Indicates that this ListAccessPoints request does not return all results that can be listed. You can use NextContinuationToken to continue obtaining list results.
	NextContinuationToken *string `xml:"NextContinuationToken"`

	// The ID of the Alibaba Cloud account to which the access point belongs.
	AccountId *string `xml:"AccountId"`

	// The container that stores the information about all access point.
	AccessPoints []AccessPoint `xml:"AccessPoints>AccessPoint"`

	ResultCommon
}

// ListAccessPoints Queries the information about user-level or bucket-level access points.
func (c *Client) ListAccessPoints(ctx context.Context, request *ListAccessPointsRequest, optFns ...func(*Options)) (*ListAccessPointsResult, error) {
	var err error
	if request == nil {
		request = &ListAccessPointsRequest{}
	}
	input := &OperationInput{
		OpName: "ListAccessPoints",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPoint": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"accessPoint"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListAccessPointsResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetAccessPointRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the access point.
	AccessPointName *string `input:"header,x-oss-access-point-name,required"`

	RequestCommon
}

type GetAccessPointResult struct {
	// The ARN of the access point.
	AccessPointArn *string `xml:"AccessPointArn"`

	// The alias of the access point.
	Alias *string `xml:"Alias"`

	// The public endpoint of the access point.
	PublicEndpoint *string `xml:"Endpoints>PublicEndpoint"`

	// The internal endpoint of the access point.
	InternalEndpoint *string `xml:"Endpoints>InternalEndpoint"`

	// The time when the access point was created.
	CreationDate *string `xml:"CreationDate"`

	// The name of the access point.
	AccessPointName *string `xml:"AccessPointName"`

	// The name of the bucket for which the access point is configured.
	Bucket *string `xml:"Bucket"`

	// The ID of the Alibaba Cloud account for which the access point is configured.
	AccountId *string `xml:"AccountId"`

	// The network origin of the access point. Valid values: vpc and internet. vpc: You can only use the specified VPC ID to access the access point. internet: You can use public endpoints and internal endpoints to access the access point.
	NetworkOrigin *string `xml:"NetworkOrigin"`

	// The container that stores the information about the VPC.
	VpcConfiguration *AccessPointVpcConfiguration `xml:"VpcConfiguration"`

	// The status of the access point.
	AccessPointStatus *string `xml:"Status"`

	// The container that stores the Block Public Access configurations.
	PublicAccessBlockConfiguration *PublicAccessBlockConfiguration `xml:"PublicAccessBlockConfiguration"`

	ResultCommon
}

// GetAccessPoint Queries the information about an access point.
func (c *Client) GetAccessPoint(ctx context.Context, request *GetAccessPointRequest, optFns ...func(*Options)) (*GetAccessPointResult, error) {
	var err error
	if request == nil {
		request = &GetAccessPointRequest{}
	}
	input := &OperationInput{
		OpName: "GetAccessPoint",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPoint": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"accessPoint"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetAccessPointResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetAccessPointPolicyRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the access point.
	AccessPointName *string `input:"header,x-oss-access-point-name,required"`

	RequestCommon
}

type GetAccessPointPolicyResult struct {
	// The configurations of the access point policy.
	Body string

	ResultCommon
}

// GetAccessPointPolicy Queries the configurations of an access point policy.
func (c *Client) GetAccessPointPolicy(ctx context.Context, request *GetAccessPointPolicyRequest, optFns ...func(*Options)) (*GetAccessPointPolicyResult, error) {
	var err error
	if request == nil {
		request = &GetAccessPointPolicyRequest{}
	}
	input := &OperationInput{
		OpName: "GetAccessPointPolicy",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPointPolicy": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"accessPointPolicy"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(output.Body)
	defer output.Body.Close()
	if err != nil {
		return nil, err
	}
	result := &GetAccessPointPolicyResult{
		Body: string(body),
	}

	if err = c.unmarshalOutput(result, output); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteAccessPointPolicyRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the access point.
	AccessPointName *string `input:"header,x-oss-access-point-name,required"`

	RequestCommon
}

type DeleteAccessPointPolicyResult struct {
	ResultCommon
}

// DeleteAccessPointPolicy Deletes an access point policy.
func (c *Client) DeleteAccessPointPolicy(ctx context.Context, request *DeleteAccessPointPolicyRequest, optFns ...func(*Options)) (*DeleteAccessPointPolicyResult, error) {
	var err error
	if request == nil {
		request = &DeleteAccessPointPolicyRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteAccessPointPolicy",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPointPolicy": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"accessPointPolicy"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteAccessPointPolicyResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutAccessPointPolicyRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the access point.
	AccessPointName *string `input:"header,x-oss-access-point-name,required"`

	// The configurations of the access point policy.
	Body io.Reader `input:"body,nop,required"`

	RequestCommon
}

type PutAccessPointPolicyResult struct {
	ResultCommon
}

// PutAccessPointPolicy Configures an access point policy.
func (c *Client) PutAccessPointPolicy(ctx context.Context, request *PutAccessPointPolicyRequest, optFns ...func(*Options)) (*PutAccessPointPolicyResult, error) {
	var err error
	if request == nil {
		request = &PutAccessPointPolicyRequest{}
	}
	input := &OperationInput{
		OpName: "PutAccessPointPolicy",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPointPolicy": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"accessPointPolicy"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutAccessPointPolicyResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteAccessPointRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the access point.
	AccessPointName *string `input:"header,x-oss-access-point-name,required"`

	RequestCommon
}

type DeleteAccessPointResult struct {
	ResultCommon
}

// DeleteAccessPoint Deletes an access point.
func (c *Client) DeleteAccessPoint(ctx context.Context, request *DeleteAccessPointRequest, optFns ...func(*Options)) (*DeleteAccessPointResult, error) {
	var err error
	if request == nil {
		request = &DeleteAccessPointRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteAccessPoint",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPoint": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"accessPoint"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteAccessPointResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type CreateAccessPointRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The container of the request body.
	CreateAccessPointConfiguration *CreateAccessPointConfiguration `input:"body,CreateAccessPointConfiguration,xml,required"`

	RequestCommon
}

type CreateAccessPointResult struct {
	// The Alibaba Cloud Resource Name (ARN) of the access point.
	AccessPointArn *string `xml:"AccessPointArn"`

	// The alias of the access point.
	Alias *string `xml:"Alias"`

	ResultCommon
}

// CreateAccessPoint Creates an access point.
func (c *Client) CreateAccessPoint(ctx context.Context, request *CreateAccessPointRequest, optFns ...func(*Options)) (*CreateAccessPointResult, error) {
	var err error
	if request == nil {
		request = &CreateAccessPointRequest{}
	}
	input := &OperationInput{
		OpName: "CreateAccessPoint",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPoint": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"accessPoint"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &CreateAccessPointResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
