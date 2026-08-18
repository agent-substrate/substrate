package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type CORSConfiguration struct {
	// The container that stores CORS rules. Up to 10 rules can be configured for a bucket.
	CORSRules []CORSRule `xml:"CORSRule"`

	// Indicates whether the Vary: Origin header was returned. Default value: false.- true: The Vary: Origin header is returned regardless whether the request is a cross-origin request or whether the cross-origin request succeeds.- false: The Vary: Origin header is not returned.
	ResponseVary *bool `xml:"ResponseVary"`
}

type CORSRule struct {
	// The origins from which cross-origin requests are allowed.
	AllowedOrigins []string `xml:"AllowedOrigin"`

	// The methods that you can use in cross-origin requests.
	AllowedMethods []string `xml:"AllowedMethod"`

	// Specifies whether the headers specified by Access-Control-Request-Headers in the OPTIONS preflight request are allowed. Each header specified by Access-Control-Request-Headers must match the value of an AllowedHeader element.  You can use only one asterisk (\*) as the wildcard character.
	AllowedHeaders []string `xml:"AllowedHeader"`

	// The response headers for allowed access requests from applications, such as an XMLHttpRequest object in JavaScript.  The asterisk (\*) wildcard character is not supported.
	ExposeHeaders []string `xml:"ExposeHeader"`

	// The period of time within which the browser can cache the response to an OPTIONS preflight request for the specified resource. Unit: seconds.You can specify only one MaxAgeSeconds element in a CORS rule.
	MaxAgeSeconds *int64 `xml:"MaxAgeSeconds"`
}

type PutBucketCorsRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	CORSConfiguration *CORSConfiguration `input:"body,CORSConfiguration,xml,required"`

	RequestCommon
}

type PutBucketCorsResult struct {
	ResultCommon
}

// PutBucketCors Configures cross-origin resource sharing (CORS) rules for a bucket.
func (c *Client) PutBucketCors(ctx context.Context, request *PutBucketCorsRequest, optFns ...func(*Options)) (*PutBucketCorsResult, error) {
	var err error
	if request == nil {
		request = &PutBucketCorsRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketCors",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"cors": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"cors"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketCorsResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketCorsRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketCorsResult struct {
	// The container that stores CORS configuration.
	CORSConfiguration *CORSConfiguration `output:"body,CORSConfiguration,xml"`

	ResultCommon
}

// GetBucketCors Queries the cross-origin resource sharing (CORS) rules that are configured for a bucket.
func (c *Client) GetBucketCors(ctx context.Context, request *GetBucketCorsRequest, optFns ...func(*Options)) (*GetBucketCorsResult, error) {
	var err error
	if request == nil {
		request = &GetBucketCorsRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketCors",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"cors": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"cors"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketCorsResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteBucketCorsRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type DeleteBucketCorsResult struct {
	ResultCommon
}

// DeleteBucketCors Disables the cross-origin resource sharing (CORS) feature and deletes all CORS rules for a bucket.
func (c *Client) DeleteBucketCors(ctx context.Context, request *DeleteBucketCorsRequest, optFns ...func(*Options)) (*DeleteBucketCorsResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketCorsRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucketCors",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"cors": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"cors"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteBucketCorsResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type OptionObjectRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The full path of the object.
	Key *string `input:"path,key,required"`

	// The origin of the request. It is used to identify a cross-origin request. You can specify only one Origin header in a cross-origin request. By default, this header is left empty.
	Origin *string `input:"header,Origin,required"`

	// The method to be used in the actual cross-origin request. You can specify only one Access-Control-Request-Method header in a cross-origin request. By default, this header is left empty.
	AccessControlRequestMethod *string `input:"header,Access-Control-Request-Method,required"`

	// The custom headers to be sent in the actual cross-origin request. You can configure multiple custom headers in a cross-origin request. Custom headers are separated by commas (,). By default, this header is left empty.
	AccessControlRequestHeaders *string `input:"header,Access-Control-Request-Headers"`

	RequestCommon
}

type OptionObjectResult struct {
	// The HTTP method of the request. If the request is denied, the response does not contain the header.
	AccessControlAllowMethods *string `output:"header,Access-Control-Allow-Methods"`

	// The list of headers included in the request. If the request includes headers that are not allowed, the response does not contain the headers and the request is denied.
	AccessControlAllowHeaders *string `output:"header,Access-Control-Allow-Headers"`

	// The list of headers that can be accessed by JavaScript applications on a client.
	AccessControlExposeHeaders *string `output:"header,Access-Control-Expose-Headers"`

	// The maximum duration for the browser to cache preflight results. Unit: seconds.
	AccessControlMaxAge *int64 `output:"header,Access-Control-Max-Age"`

	// The origin that is included in the request. If the request is denied, the response does not contain the header.
	AccessControlAllowOrigin *string `output:"header,Access-Control-Allow-Origin"`

	ResultCommon
}

// OptionObject Determines whether to send a cross-origin request. Before a cross-origin request is sent, the browser sends a preflight OPTIONS request that includes a specific origin, HTTP method, and header information to Object Storage Service (OSS) to determine whether to send the cross-origin request.
func (c *Client) OptionObject(ctx context.Context, request *OptionObjectRequest, optFns ...func(*Options)) (*OptionObjectResult, error) {
	var err error
	if request == nil {
		request = &OptionObjectRequest{}
	}
	input := &OperationInput{
		OpName: "OptionObject",
		Method: "OPTIONS",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Bucket: request.Bucket,
		Key:    request.Key,
	}

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &OptionObjectResult{}

	if err = c.unmarshalOutput(result, output, unmarshalHeader, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
