package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type LoggingEnabled struct {
	// The bucket that stores access logs.
	TargetBucket *string `xml:"TargetBucket" json:"TargetBucket,omitempty"`

	// The prefix of the log objects. This parameter can be left empty.
	TargetPrefix *string `xml:"TargetPrefix" json:"TargetPrefix,omitempty"`

	// Log transfer authorization role.
	LoggingRole *string `xml:"LoggingRole" json:"LoggingRole,omitempty"`
}

type BucketLoggingStatus struct {
	// Indicates the container used to store access logging information. This element is returned if it is enabled and is not returned if it is disabled.
	LoggingEnabled *LoggingEnabled `xml:"LoggingEnabled" json:"LoggingEnabled,omitempty"`
}

type LoggingHeaderSet struct {
	// The list of the custom request headers.
	Headers []string `xml:"header"`
}

type LoggingParamSet struct {
	// The list of the custom URL parameters.
	Parameters []string `xml:"parameter"`
}

type UserDefinedLogFieldsConfiguration struct {
	// The container that stores the configurations of custom request headers.
	HeaderSet *LoggingHeaderSet `xml:"HeaderSet"`

	// The container that stores the configurations of custom URL parameters.
	ParamSet *LoggingParamSet `xml:"ParamSet"`
}

type PutBucketLoggingRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	BucketLoggingStatus *BucketLoggingStatus `input:"body,BucketLoggingStatus,xml,required"`

	RequestCommon
}

type PutBucketLoggingResult struct {
	ResultCommon
}

// PutBucketLogging Enables logging for a bucket. After you enable logging for a bucket, Object Storage Service (OSS) generates logs every hour based on the defined naming rule and stores the logs as objects in the specified destination bucket.
func (c *Client) PutBucketLogging(ctx context.Context, request *PutBucketLoggingRequest, optFns ...func(*Options)) (*PutBucketLoggingResult, error) {
	var err error
	if request == nil {
		request = &PutBucketLoggingRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketLogging",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"logging": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"logging"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketLoggingResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type GetBucketLoggingRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketLoggingResult struct {
	// Indicates the container used to store access logging configuration of a bucket.
	BucketLoggingStatus *BucketLoggingStatus `output:"body,BucketLoggingStatus,xml"`

	ResultCommon
}

// GetBucketLogging Queries the configurations of access log collection of a bucket. Only the owner of a bucket can query the configurations of access log collection of the bucket.
func (c *Client) GetBucketLogging(ctx context.Context, request *GetBucketLoggingRequest, optFns ...func(*Options)) (*GetBucketLoggingResult, error) {
	var err error
	if request == nil {
		request = &GetBucketLoggingRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketLogging",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"logging": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"logging"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketLoggingResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type DeleteBucketLoggingRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type DeleteBucketLoggingResult struct {
	ResultCommon
}

// DeleteBucketLogging Disables the logging feature for a bucket.
func (c *Client) DeleteBucketLogging(ctx context.Context, request *DeleteBucketLoggingRequest, optFns ...func(*Options)) (*DeleteBucketLoggingResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketLoggingRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucketLogging",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"logging": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"logging"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteBucketLoggingResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type PutUserDefinedLogFieldsConfigRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The container that stores the specified log configurations.
	UserDefinedLogFieldsConfiguration *UserDefinedLogFieldsConfiguration `input:"body,UserDefinedLogFieldsConfiguration,xml,required"`

	RequestCommon
}

type PutUserDefinedLogFieldsConfigResult struct {
	ResultCommon
}

// PutUserDefinedLogFieldsConfig Customizes the user_defined_log_fields field in real-time logs by adding custom request headers or query parameters to the field for subsequent analysis of requests.
func (c *Client) PutUserDefinedLogFieldsConfig(ctx context.Context, request *PutUserDefinedLogFieldsConfigRequest, optFns ...func(*Options)) (*PutUserDefinedLogFieldsConfigResult, error) {
	var err error
	if request == nil {
		request = &PutUserDefinedLogFieldsConfigRequest{}
	}
	input := &OperationInput{
		OpName: "PutUserDefinedLogFieldsConfig",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"userDefinedLogFieldsConfig": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"userDefinedLogFieldsConfig"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutUserDefinedLogFieldsConfigResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type GetUserDefinedLogFieldsConfigRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetUserDefinedLogFieldsConfigResult struct {
	// The container for the user-defined logging configuration.
	UserDefinedLogFieldsConfiguration *UserDefinedLogFieldsConfiguration `output:"body,UserDefinedLogFieldsConfiguration,xml"`

	ResultCommon
}

// GetUserDefinedLogFieldsConfig Queries the custom configurations of the user_defined_log_fields field in the real-time logs of a bucket.
func (c *Client) GetUserDefinedLogFieldsConfig(ctx context.Context, request *GetUserDefinedLogFieldsConfigRequest, optFns ...func(*Options)) (*GetUserDefinedLogFieldsConfigResult, error) {
	var err error
	if request == nil {
		request = &GetUserDefinedLogFieldsConfigRequest{}
	}
	input := &OperationInput{
		OpName: "GetUserDefinedLogFieldsConfig",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"userDefinedLogFieldsConfig": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"userDefinedLogFieldsConfig"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetUserDefinedLogFieldsConfigResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type DeleteUserDefinedLogFieldsConfigRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type DeleteUserDefinedLogFieldsConfigResult struct {
	ResultCommon
}

// DeleteUserDefinedLogFieldsConfig Deletes the custom configurations of the user_defined_log_fields field in the real-time logs of a bucket.
func (c *Client) DeleteUserDefinedLogFieldsConfig(ctx context.Context, request *DeleteUserDefinedLogFieldsConfigRequest, optFns ...func(*Options)) (*DeleteUserDefinedLogFieldsConfigResult, error) {
	var err error
	if request == nil {
		request = &DeleteUserDefinedLogFieldsConfigRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteUserDefinedLogFieldsConfig",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"userDefinedLogFieldsConfig": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"userDefinedLogFieldsConfig"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteUserDefinedLogFieldsConfigResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}
