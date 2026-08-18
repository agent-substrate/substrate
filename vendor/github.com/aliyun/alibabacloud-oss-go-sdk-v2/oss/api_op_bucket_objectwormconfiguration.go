package oss

import (
	"context"
	"fmt"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type PutBucketObjectWormConfigurationRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	ObjectWormConfiguration *ObjectWormConfiguration `input:"body,ObjectWormConfiguration,xml,required"`

	RequestCommon
}

type ObjectWormConfiguration struct {
	// Whether to enable object-level retention policy.
	ObjectWormEnabled *string `xml:"ObjectWormEnabled"`

	// Container with object-level retention policy
	Rule *ObjectWormRule `xml:"Rule"`
}

type ObjectWormRule struct {
	DefaultRetention *ObjectWormDefaultRetention `xml:"DefaultRetention"`
}

type ObjectWormDefaultRetention struct {
	// Object-level retention strategy pattern. valid value:GOVERNANCE, COMPLIANCE
	Mode *string `xml:"Mode"`

	// Object-level retention policy days (max 36500)
	Days *int32 `xml:"Days"`

	// Bucket object level retention policy years (max 100)
	Years *int32 `xml:"Years"`
}

type PutBucketObjectWormConfigurationResult struct {
	ResultCommon
}

// PutBucketObjectWormConfiguration Enable object retention on the bucket and configure a retention policy.
func (c *Client) PutBucketObjectWormConfiguration(ctx context.Context, request *PutBucketObjectWormConfigurationRequest, optFns ...func(*Options)) (*PutBucketObjectWormConfigurationResult, error) {
	var err error
	if request == nil {
		request = &PutBucketObjectWormConfigurationRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketObjectWormConfiguration",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"objectWorm": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"objectWorm"})

	if err = checkObjectWormConfiguration(request.ObjectWormConfiguration); err != nil {
		return nil, err
	}

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketObjectWormConfigurationResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketObjectWormConfigurationRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketObjectWormConfigurationResult struct {
	// The container that stores object worm config.
	ObjectWormConfiguration *ObjectWormConfiguration `output:"body,ObjectWormConfiguration,xml"`

	ResultCommon
}

// GetBucketObjectWormConfiguration Queries the object-level retention policy of a bucket.
func (c *Client) GetBucketObjectWormConfiguration(ctx context.Context, request *GetBucketObjectWormConfigurationRequest, optFns ...func(*Options)) (*GetBucketObjectWormConfigurationResult, error) {
	var err error
	if request == nil {
		request = &GetBucketObjectWormConfigurationRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketObjectWormConfiguration",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"objectWorm": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"objectWorm"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketObjectWormConfigurationResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

func checkObjectWormConfiguration(configuration *ObjectWormConfiguration) error {
	if configuration == nil {
		return nil
	}

	if configuration.Rule == nil {
		return nil
	}

	if configuration.Rule.DefaultRetention == nil {
		return nil
	}

	if configuration.Rule.DefaultRetention.Days == nil && configuration.Rule.DefaultRetention.Years == nil {
		return fmt.Errorf("either DefaultRetention.Days or DefaultRetention.Years must be configured")
	}

	if configuration.Rule.DefaultRetention.Days != nil && *configuration.Rule.DefaultRetention.Days <= 0 {
		return fmt.Errorf("DefaultRetention.Days must be greater than 0")
	}

	if configuration.Rule.DefaultRetention.Years != nil && *configuration.Rule.DefaultRetention.Years <= 0 {
		return fmt.Errorf("DefaultRetention.Years must be greater than 0")
	}

	return nil
}
