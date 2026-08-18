package oss

import (
	"context"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type RefererList struct {
	// The addresses in the Referer whitelist.
	Referers []string `xml:"Referer"`
}

type RefererBlacklist struct {
	// The addresses in the Referer blacklist.
	Referers []string `xml:"Referer"`
}

type RefererConfiguration struct {
	// Specifies whether to allow a request whose Referer field is empty. Valid values:*   true (default)*   false
	AllowEmptyReferer *bool `xml:"AllowEmptyReferer"`

	// Specifies whether to truncate the query string in the URL when the Referer is matched. Valid values:*   true (default)*   false
	AllowTruncateQueryString *bool `xml:"AllowTruncateQueryString"`

	// Specifies whether to truncate the path and parts that follow the path in the URL when the Referer is matched. Valid values:*   true*   false
	TruncatePath *bool `xml:"TruncatePath"`

	// The container that stores the Referer whitelist.  ****The PutBucketReferer operation overwrites the existing Referer whitelist with the Referer whitelist specified in RefererList. If RefererList is not specified in the request, which specifies that no Referer elements are included, the operation clears the existing Referer whitelist.
	RefererList *RefererList `xml:"RefererList"`

	// The container that stores the Referer blacklist.
	RefererBlacklist *RefererBlacklist `xml:"RefererBlacklist"`
}

type PutBucketRefererRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	RefererConfiguration *RefererConfiguration `input:"body,RefererConfiguration,xml,required"`

	RequestCommon
}

type PutBucketRefererResult struct {
	ResultCommon
}

// PutBucketReferer Configures a Referer whitelist for an Object Storage Service (OSS) bucket. You can specify whether to allow the requests whose Referer field is empty or whose query strings are truncated.
func (c *Client) PutBucketReferer(ctx context.Context, request *PutBucketRefererRequest, optFns ...func(*Options)) (*PutBucketRefererResult, error) {
	var err error
	if request == nil {
		request = &PutBucketRefererRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketReferer",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"referer": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"referer"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketRefererResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketRefererRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketRefererResult struct {
	// The container that stores the hotlink protection configurations.
	RefererConfiguration *RefererConfiguration `output:"body,RefererConfiguration,xml"`

	ResultCommon
}

// GetBucketReferer Queries the hotlink protection configurations for a bucket.
func (c *Client) GetBucketReferer(ctx context.Context, request *GetBucketRefererRequest, optFns ...func(*Options)) (*GetBucketRefererResult, error) {
	var err error
	if request == nil {
		request = &GetBucketRefererRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketReferer",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"referer": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"referer"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketRefererResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
