package oss

import (
	"context"
	"io"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type AccessPointsForObjectProcess struct {
	// The container that stores information about a single Object FC Access Point.
	AccessPointForObjectProcesses []AccessPointForObjectProcess `xml:"AccessPointForObjectProcess"`
}

type TransformationConfiguration struct {
	// The container that stores the operations.
	Actions *AccessPointActions `xml:"Actions"`

	// The container that stores the content of the transformation configurations.
	ContentTransformation *ContentTransformation `xml:"ContentTransformation"`
}

type ObjectProcessConfiguration struct {
	// The container that stores the transformation configurations.
	TransformationConfigurations *TransformationConfigurations `xml:"TransformationConfigurations"`

	// The container that stores allowed features.
	AllowedFeatures *ObjectProcessAllowedFeatures `xml:"AllowedFeatures"`
}

type ObjectProcessAllowedFeatures struct {
	// Specifies that Function Compute supports Range GetObject requests.
	AllowedFeatures []string `xml:"AllowedFeature"`
}

type TransformationConfigurations struct {
	// The container that stores the transformation configurations.
	TransformationConfigurations []TransformationConfiguration `xml:"TransformationConfiguration"`
}

type CreateAccessPointForObjectProcessConfiguration struct {
	// Whether allow anonymous user to access this FC Access Point.
	AllowAnonymousAccessForObjectProcess *string `xml:"AllowAnonymousAccessForObjectProcess"`

	// The name of the access point.
	AccessPointName *string `xml:"AccessPointName"`

	// The container that stores the processing information about the Object FC Access Point.
	ObjectProcessConfiguration *ObjectProcessConfiguration `xml:"ObjectProcessConfiguration"`
}

type AccessPointEndpoints struct {
	// The internal endpoint of the Object FC Access Point.
	InternalEndpoint *string `xml:"InternalEndpoint"`

	// The public endpoint of the Object FC Access Point.
	PublicEndpoint *string `xml:"PublicEndpoint"`
}

type AccessPointForObjectProcess struct {
	// The status of the Object FC Access Point. Valid values:enable: The Object FC Access Point is created.disable: The Object FC Access Point is disabled.creating: The Object FC Access Point is being created.deleting: The Object FC Access Point is deleted.
	Status *string `xml:"Status"`

	// Whether allow anonymous user access this FC Access Point.
	AllowAnonymousAccessForObjectProcess *string `xml:"AllowAnonymousAccessForObjectProcess"`

	// The name of the Object FC Access Point.
	AccessPointNameForObjectProcess *string `xml:"AccessPointNameForObjectProcess"`

	// The alias of the Object FC Access Point.
	AccessPointForObjectProcessAlias *string `xml:"AccessPointForObjectProcessAlias"`

	// The name of the access point.
	AccessPointName *string `xml:"AccessPointName"`
}

type AccessPointActions struct {
	// The supported OSS API operations. Only the GetObject operation is supported.
	Actions []string `xml:"Action"`
}

type ObjectProcessCustomForwardHeaders struct {
	CustomForwardHeaders []string `xml:"CustomForwardHeader"`
}

type ContentTransformation struct {
	// The container that stores the information about Function Compute.
	FunctionCompute *ObjectProcessFunctionCompute `xml:"FunctionCompute"`

	// The container that stores the information about Additional Features.
	AdditionalFeatures *ObjectProcessAdditionalFeatures `xml:"AdditionalFeatures"`
}

type ObjectProcessFunctionCompute struct {
	// The ARN of the function. For more information,
	FunctionArn *string `xml:"FunctionArn"`

	// The Alibaba Cloud Resource Name (ARN) of the role that Function Compute uses to access your resources in other cloud services. The default role is AliyunFCDefaultRole.
	FunctionAssumeRoleArn *string `xml:"FunctionAssumeRoleArn"`
}

type ObjectProcessAdditionalFeatures struct {
	CustomForwardHeaders *ObjectProcessCustomForwardHeaders `xml:"CustomForwardHeaders"`
}

type PutAccessPointConfigForObjectProcessConfiguration struct {
	// Whether allow anonymous user to access this FC Access Point.
	AllowAnonymousAccessForObjectProcess *string `xml:"AllowAnonymousAccessForObjectProcess"`

	// The container in which the Block Public Access configurations are stored.
	PublicAccessBlockConfiguration *PublicAccessBlockConfiguration `xml:"PublicAccessBlockConfiguration"`

	// The container that stores the processing information about the Object FC Access Point.
	ObjectProcessConfiguration *ObjectProcessConfiguration `xml:"ObjectProcessConfiguration"`
}

type CreateAccessPointForObjectProcessRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the Object FC Access Point.
	AccessPointForObjectProcessName *string `input:"header,x-oss-access-point-for-object-process-name,required"`

	// The request body.
	CreateAccessPointForObjectProcessConfiguration *CreateAccessPointForObjectProcessConfiguration `input:"body,CreateAccessPointForObjectProcessConfiguration,xml,required"`

	RequestCommon
}

type CreateAccessPointForObjectProcessResult struct {
	// The ARN of the Object FC Access Point.
	AccessPointForObjectProcessArn *string `xml:"AccessPointForObjectProcessArn"`

	// The alias of the Object FC Access Point.
	AccessPointForObjectProcessAlias *string `xml:"AccessPointForObjectProcessAlias"`

	ResultCommon
}

// CreateAccessPointForObjectProcess Creates an Object FC Access Point.
func (c *Client) CreateAccessPointForObjectProcess(ctx context.Context, request *CreateAccessPointForObjectProcessRequest, optFns ...func(*Options)) (*CreateAccessPointForObjectProcessResult, error) {
	var err error
	if request == nil {
		request = &CreateAccessPointForObjectProcessRequest{}
	}
	input := &OperationInput{
		OpName: "CreateAccessPointForObjectProcess",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPointForObjectProcess": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"accessPointForObjectProcess"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &CreateAccessPointForObjectProcessResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetAccessPointForObjectProcessRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the Object FC Access Point. The name of an Object FC Access Point must meet the following requirements:The name cannot exceed 63 characters in length.The name can contain only lowercase letters, digits, and hyphens (-) and cannot start or end with a hyphen (-).The name must be unique in the current region.
	AccessPointForObjectProcessName *string `input:"header,x-oss-access-point-for-object-process-name,required"`

	RequestCommon
}

type GetAccessPointForObjectProcessResult struct {
	// The public endpoint of the Object FC Access Point.
	AllowAnonymousAccessForObjectProcess *string `xml:"AllowAnonymousAccessForObjectProcess"`

	// The container in which the Block Public Access configurations are stored.
	PublicAccessBlockConfiguration *PublicAccessBlockConfiguration `xml:"PublicAccessBlockConfiguration"`

	// The internal endpoint of the Object FC Access Point.
	AccessPointNameForObjectProcess *string `xml:"AccessPointNameForObjectProcess"`

	// The ARN of the Object FC Access Point.
	AccessPointForObjectProcessArn *string `xml:"AccessPointForObjectProcessArn"`

	// The time when the Object FC Access Point was created. The value is a timestamp.
	CreationDate *string `xml:"CreationDate"`

	// The status of the Object FC Access Point. Valid values:enable: The Object FC Access Point is created.disable: The Object FC Access Point is disabled.creating: The Object FC Access Point is being created.deleting: The Object FC Access Point is deleted.
	AccessPointForObjectProcessStatus *string `xml:"Status"`

	//  The container that stores the endpoints of the Object FC Access Point.
	Endpoints *AccessPointEndpoints `xml:"Endpoints"`

	// The alias of the Object FC Access Point.
	AccessPointForObjectProcessAlias *string `xml:"AccessPointForObjectProcessAlias"`

	// The public endpoint of the Object FC Access Point.
	AccessPointName *string `xml:"AccessPointName"`

	// The public endpoint of the Object FC Access Point.
	AccountId *string `xml:"AccountId"`

	ResultCommon
}

// GetAccessPointForObjectProcess Queries basic information about an Object FC Access Point.
func (c *Client) GetAccessPointForObjectProcess(ctx context.Context, request *GetAccessPointForObjectProcessRequest, optFns ...func(*Options)) (*GetAccessPointForObjectProcessResult, error) {
	var err error
	if request == nil {
		request = &GetAccessPointForObjectProcessRequest{}
	}
	input := &OperationInput{
		OpName: "GetAccessPointForObjectProcess",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPointForObjectProcess": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"accessPointForObjectProcess"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetAccessPointForObjectProcessResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type ListAccessPointsForObjectProcessRequest struct {
	// The maximum number of Object FC Access Points to return.Valid values: 1 to 1000 If the list cannot be complete at a time due to the configurations of the max-keys element, the NextContinuationToken element is included in the response as the token for the next list.
	MaxKeys int64 `input:"query,max-keys"`

	// The token from which the list operation must start. You can obtain this token from the NextContinuationToken element in the returned result.
	ContinuationToken *string `input:"query,continuation-token"`

	RequestCommon
}

type ListAccessPointsForObjectProcessResult struct {
	// The container that stores information about all Object FC Access Points.
	AccessPointsForObjectProcess *AccessPointsForObjectProcess `xml:"AccessPointsForObjectProcess"`

	// Indicates whether the returned results are truncated. Valid values:true: indicates that not all results are returned for the request.false: indicates that all results are returned for the request.
	IsTruncated *bool `xml:"IsTruncated"`

	// The container that stores information about a single Object FC Access Point.
	NextContinuationToken *string `xml:"NextContinuationToken"`

	// The UID of the Alibaba Cloud account to which the Object FC Access Points belong.
	AccountId *string `xml:"AccountId"`

	ResultCommon
}

// ListAccessPointsForObjectProcess Lists information about Object FC Access Points in an Alibaba Cloud account.
func (c *Client) ListAccessPointsForObjectProcess(ctx context.Context, request *ListAccessPointsForObjectProcessRequest, optFns ...func(*Options)) (*ListAccessPointsForObjectProcessResult, error) {
	var err error
	if request == nil {
		request = &ListAccessPointsForObjectProcessRequest{}
	}
	input := &OperationInput{
		OpName: "ListAccessPointsForObjectProcess",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPointForObjectProcess": "",
		},
	}

	input.OpMetadata.Set(signer.SubResource, []string{"accessPointForObjectProcess"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListAccessPointsForObjectProcessResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteAccessPointForObjectProcessRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the Object FC Access Point.
	AccessPointForObjectProcessName *string `input:"header,x-oss-access-point-for-object-process-name,required"`

	RequestCommon
}

type DeleteAccessPointForObjectProcessResult struct {
	ResultCommon
}

// DeleteAccessPointForObjectProcess Deletes an Object FC Access Point.
func (c *Client) DeleteAccessPointForObjectProcess(ctx context.Context, request *DeleteAccessPointForObjectProcessRequest, optFns ...func(*Options)) (*DeleteAccessPointForObjectProcessResult, error) {
	var err error
	if request == nil {
		request = &DeleteAccessPointForObjectProcessRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteAccessPointForObjectProcess",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPointForObjectProcess": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"accessPointForObjectProcess"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteAccessPointForObjectProcessResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetAccessPointConfigForObjectProcessRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the Object FC Access Point.
	AccessPointForObjectProcessName *string `input:"header,x-oss-access-point-for-object-process-name,required"`

	RequestCommon
}

type GetAccessPointConfigForObjectProcessResult struct {
	// The container that stores the processing information about the Object FC Access Point.
	ObjectProcessConfiguration *ObjectProcessConfiguration `xml:"ObjectProcessConfiguration"`

	// Whether allow anonymous user to access this FC Access Points.
	AllowAnonymousAccessForObjectProcess *string `xml:"AllowAnonymousAccessForObjectProcess"`

	// The container in which the Block Public Access configurations are stored.
	PublicAccessBlockConfiguration *PublicAccessBlockConfiguration `xml:"PublicAccessBlockConfiguration"`

	ResultCommon
}

// GetAccessPointConfigForObjectProcess Queries the configurations of an Object FC Access Point.
func (c *Client) GetAccessPointConfigForObjectProcess(ctx context.Context, request *GetAccessPointConfigForObjectProcessRequest, optFns ...func(*Options)) (*GetAccessPointConfigForObjectProcessResult, error) {
	var err error
	if request == nil {
		request = &GetAccessPointConfigForObjectProcessRequest{}
	}
	input := &OperationInput{
		OpName: "GetAccessPointConfigForObjectProcess",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPointConfigForObjectProcess": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"accessPointConfigForObjectProcess"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetAccessPointConfigForObjectProcessResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutAccessPointConfigForObjectProcessRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the Object FC Access Point. The name of an Object FC Access Point must meet the following requirements:The name cannot exceed 63 characters in length.The name can contain only lowercase letters, digits, and hyphens (-) and cannot start or end with a hyphen (-).The name must be unique in the current region.
	AccessPointForObjectProcessName *string `input:"header,x-oss-access-point-for-object-process-name,required"`

	// The request body.
	PutAccessPointConfigForObjectProcessConfiguration *PutAccessPointConfigForObjectProcessConfiguration `input:"body,PutAccessPointConfigForObjectProcessConfiguration,xml,required"`

	RequestCommon
}

type PutAccessPointConfigForObjectProcessResult struct {
	ResultCommon
}

// PutAccessPointConfigForObjectProcess Changes the configurations of an Object FC Access Point.
func (c *Client) PutAccessPointConfigForObjectProcess(ctx context.Context, request *PutAccessPointConfigForObjectProcessRequest, optFns ...func(*Options)) (*PutAccessPointConfigForObjectProcessResult, error) {
	var err error
	if request == nil {
		request = &PutAccessPointConfigForObjectProcessRequest{}
	}
	input := &OperationInput{
		OpName: "PutAccessPointConfigForObjectProcess",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPointConfigForObjectProcess": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"accessPointConfigForObjectProcess"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutAccessPointConfigForObjectProcessResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutAccessPointPolicyForObjectProcessRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the Object FC Access Point.
	AccessPointForObjectProcessName *string `input:"header,x-oss-access-point-for-object-process-name,required"`

	// The json format permission policies for an Object FC Access Point.
	Body io.Reader `input:"body,nop,required"`

	RequestCommon
}

type PutAccessPointPolicyForObjectProcessResult struct {
	ResultCommon
}

// PutAccessPointPolicyForObjectProcess Configures policies for an Object FC Access Point.
func (c *Client) PutAccessPointPolicyForObjectProcess(ctx context.Context, request *PutAccessPointPolicyForObjectProcessRequest, optFns ...func(*Options)) (*PutAccessPointPolicyForObjectProcessResult, error) {
	var err error
	if request == nil {
		request = &PutAccessPointPolicyForObjectProcessRequest{}
	}
	input := &OperationInput{
		OpName: "PutAccessPointPolicyForObjectProcess",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPointPolicyForObjectProcess": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"accessPointPolicyForObjectProcess"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutAccessPointPolicyForObjectProcessResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetAccessPointPolicyForObjectProcessRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the Object FC Access Point.
	AccessPointForObjectProcessName *string `input:"header,x-oss-access-point-for-object-process-name,required"`

	RequestCommon
}

type GetAccessPointPolicyForObjectProcessResult struct {
	// The configurations of the access point policy for object process.
	Body string

	ResultCommon
}

// GetAccessPointPolicyForObjectProcess Queries the policies of an Object FC Access Point.
func (c *Client) GetAccessPointPolicyForObjectProcess(ctx context.Context, request *GetAccessPointPolicyForObjectProcessRequest, optFns ...func(*Options)) (*GetAccessPointPolicyForObjectProcessResult, error) {
	var err error
	if request == nil {
		request = &GetAccessPointPolicyForObjectProcessRequest{}
	}
	input := &OperationInput{
		OpName: "GetAccessPointPolicyForObjectProcess",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPointPolicyForObjectProcess": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"accessPointPolicyForObjectProcess"})

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
	result := &GetAccessPointPolicyForObjectProcessResult{
		Body: string(body),
	}

	if err = c.unmarshalOutput(result, output); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteAccessPointPolicyForObjectProcessRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the Object FC Access Point.
	AccessPointForObjectProcessName *string `input:"header,x-oss-access-point-for-object-process-name,required"`

	RequestCommon
}

type DeleteAccessPointPolicyForObjectProcessResult struct {
	ResultCommon
}

// DeleteAccessPointPolicyForObjectProcess Deletes the policies of an Object FC Access Point.
func (c *Client) DeleteAccessPointPolicyForObjectProcess(ctx context.Context, request *DeleteAccessPointPolicyForObjectProcessRequest, optFns ...func(*Options)) (*DeleteAccessPointPolicyForObjectProcessResult, error) {
	var err error
	if request == nil {
		request = &DeleteAccessPointPolicyForObjectProcessRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteAccessPointPolicyForObjectProcess",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"accessPointPolicyForObjectProcess": "",
		},
		Bucket: request.Bucket,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"accessPointPolicyForObjectProcess"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteAccessPointPolicyForObjectProcessResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type WriteGetObjectResponseRequest struct {
	// The router forwarding address obtained from the event parameter of Function Compute.
	RequestRoute *string `input:"header,x-oss-request-route,required"`

	// The unique forwarding token obtained from the event parameter of Function Compute.
	RequestToken *string `input:"header,x-oss-request-token,required"`

	// The HTTP status code returned by the backend server.
	FwdStatus *string `input:"header,x-oss-fwd-status,required"`

	// The HTTP response header returned by the backend server. It is used to specify the scope of the resources that you want to query.
	FwdHeaderAcceptRanges *string `input:"header,x-oss-fwd-header-Accept-Ranges"`

	// The HTTP response header returned by the backend server. It is used to specify the resource cache method that the client uses. Valid values: no-cache, no-store, public, private, max-age
	FwdHeaderCacheControl *string `input:"header,x-oss-fwd-header-Cache-Control"`

	FwdHeaderContentDisposition *string `input:"header,x-oss-fwd-header-Content-Disposition"`

	FwdHeaderContentEncoding *string `input:"header,x-oss-fwd-header-Content-Encoding"`

	FwdHeaderContentLanguage *string `input:"header,x-oss-fwd-header-Content-Language"`

	FwdHeaderContentRange *string `input:"header,x-oss-fwd-header-Content-Range"`

	// The HTTP response header returned by the backend server. It is used to specify the type of the received or sent data.
	FwdHeaderContentType *string `input:"header,x-oss-fwd-header-Content-Type"`

	// The HTTP response header returned by the backend server. It uniquely identifies the object.
	FwdHeaderEtag *string `input:"header,x-oss-fwd-header-ETag"`

	// The HTTP response header returned by the backend server. It specifies the absolute expiration time of the cache.
	FwdHeaderExpires *string `input:"header,x-oss-fwd-header-Expires"`

	// The HTTP response header returned by the backend server. It specifies the time when the requested resource was last modified.
	FwdHeaderLastModified *string `input:"header,x-oss-fwd-header-Last-Modified"`

	Body io.Reader `input:"body,nop"`

	RequestCommon
}

type WriteGetObjectResponseResult struct {
	ResultCommon
}

// WriteGetObjectResponse Customize return data and response headers
func (c *Client) WriteGetObjectResponse(ctx context.Context, request *WriteGetObjectResponseRequest, optFns ...func(*Options)) (*WriteGetObjectResponseResult, error) {
	var err error
	if request == nil {
		request = &WriteGetObjectResponseRequest{}
	}
	input := &OperationInput{
		OpName: "WriteGetObjectResponse",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"x-oss-write-get-object-response": "",
		},
	}

	input.OpMetadata.Set(signer.SubResource, []string{"x-oss-write-get-object-response"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &WriteGetObjectResponseResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
