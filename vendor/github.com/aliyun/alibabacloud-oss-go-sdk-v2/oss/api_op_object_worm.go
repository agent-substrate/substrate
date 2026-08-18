package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type PutObjectRetentionRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Version of the object.
	VersionId *string `input:"query,versionId"`

	// Bypass governance and retention.
	BypassGovernanceRetention *bool `input:"header,x-oss-bypass-governance-retention"`

	// The container that stores the retention policy.
	Retention *ObjectWormRetention `input:"body,Retention,xml,required"`
}

type ObjectWormRetention struct {
	// Object-level Retention Strategy Pattern.
	Mode *string `xml:"Mode"`

	// The absolute date and time for the Object-level retention policy.
	RetainUntilDate *string `xml:"RetainUntilDate"`
}

type PutObjectRetentionResult struct {
	ResultCommon
}

// PutObjectRetention Configure a retention policy on Object.
func (c *Client) PutObjectRetention(ctx context.Context, request *PutObjectRetentionRequest, optFns ...func(*Options)) (*PutObjectRetentionResult, error) {
	var err error
	if request == nil {
		request = &PutObjectRetentionRequest{}
	}
	input := &OperationInput{
		OpName: "PutObjectRetention",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"retention": "",
		},
		Bucket: request.Bucket,
		Key:    request.Key,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"retention"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutObjectRetentionResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetObjectRetentionRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Version of the object.
	VersionId *string `input:"query,versionId"`
}

type GetObjectRetentionResult struct {
	//  The container that stores the retention policy.
	Retention *ObjectWormRetention `output:"body,Retention,xml"`

	ResultCommon
}

// GetObjectRetention query the object-level retention policy of an object in a bucket.
func (c *Client) GetObjectRetention(ctx context.Context, request *GetObjectRetentionRequest, optFns ...func(*Options)) (*GetObjectRetentionResult, error) {
	var err error
	if request == nil {
		request = &GetObjectRetentionRequest{}
	}
	input := &OperationInput{
		OpName: "GetObjectRetention",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"retention": "",
		},
		Bucket: request.Bucket,
		Key:    request.Key,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"retention"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetObjectRetentionResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutObjectLegalHoldRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Version of the object.
	VersionId *string `input:"query,versionId"`

	// The container that stores the object-level legal retention.
	LegalHold *ObjectWormLegalHold `input:"body,LegalHold,xml,required"`
}

type ObjectWormLegalHold struct {
	// Object legal hold switch.
	Status *string `xml:"Status"`
}

type PutObjectLegalHoldResult struct {
	ResultCommon
}

// PutObjectLegalHold Configure legal retention on Object.
func (c *Client) PutObjectLegalHold(ctx context.Context, request *PutObjectLegalHoldRequest, optFns ...func(*Options)) (*PutObjectLegalHoldResult, error) {
	var err error
	if request == nil {
		request = &PutObjectLegalHoldRequest{}
	}
	input := &OperationInput{
		OpName: "PutObjectLegalHold",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"legalHold": "",
		},
		Bucket: request.Bucket,
		Key:    request.Key,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"legalHold"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutObjectLegalHoldResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetObjectLegalHoldRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Version of the object.
	VersionId *string `input:"query,versionId"`
}

type GetObjectLegalHoldResult struct {
	// The container that stores the object-level legal retention.
	LegalHold *ObjectWormLegalHold `output:"body,LegalHold,xml"`

	ResultCommon
}

// GetObjectLegalHold Queries the object-level legal retention of an object in a bucket.
func (c *Client) GetObjectLegalHold(ctx context.Context, request *GetObjectLegalHoldRequest, optFns ...func(*Options)) (*GetObjectLegalHoldResult, error) {
	var err error
	if request == nil {
		request = &GetObjectLegalHoldRequest{}
	}
	input := &OperationInput{
		OpName: "GetObjectLegalHold",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"legalHold": "",
		},
		Bucket: request.Bucket,
		Key:    request.Key,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"legalHold"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetObjectLegalHoldResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
