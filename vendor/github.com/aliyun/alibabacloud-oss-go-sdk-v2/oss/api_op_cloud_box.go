package oss

import (
	"context"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type ListCloudBoxesRequest struct {
	// The name of the bucket from which the list operation begins.
	Marker *string `input:"query,marker"`

	// The maximum number of buckets that can be returned in the single query.
	// Valid values: 1 to 1000.
	MaxKeys int32 `input:"query,max-keys"`

	// The prefix that the names of returned buckets must contain.
	Prefix *string `input:"query,prefix"`

	RequestCommon
}

type ListCloudBoxesResult struct {
	// The prefix contained in the names of the returned bucket.
	Prefix *string `xml:"Prefix"`

	// The name of the bucket after which the ListBuckets  operation starts.
	Marker *string `xml:"Marker"` // The marker filter.

	// The maximum number of buckets that can be returned for the request.
	MaxKeys int32 `xml:"MaxKeys"`

	// Indicates whether all results are returned.
	// true: Only part of the results are returned for the request.
	// false: All results are returned for the request.
	IsTruncated bool `xml:"IsTruncated"`

	// The marker for the next ListBuckets request, which can be used to return the remaining results.
	NextMarker *string `xml:"NextMarker"`

	// The container that stores information about the bucket owner.
	Owner *Owner `xml:"Owner"`

	// The container that stores information about cloud box bucket.
	CloudBoxes []CloudBoxProperties `xml:"CloudBoxes>CloudBox"`

	ResultCommon
}

type CloudBoxProperties struct {
	ID              *string `xml:"ID"`
	Name            *string `xml:"Name"`
	Region          *string `xml:"Region"`
	ControlEndpoint *string `xml:"ControlEndpoint"`
	DataEndpoint    *string `xml:"DataEndpoint"`
}

// ListCloudBoxes Lists cloud box buckets that belong to the current account.
func (c *Client) ListCloudBoxes(ctx context.Context, request *ListCloudBoxesRequest, optFns ...func(*Options)) (*ListCloudBoxesResult, error) {
	var err error
	if request == nil {
		request = &ListCloudBoxesRequest{}
	}
	input := &OperationInput{
		OpName: "ListCloudBoxes",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"cloudboxes": "",
		},
	}

	input.OpMetadata.Set(signer.SubResource, []string{"cloudboxes"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListCloudBoxesResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}
