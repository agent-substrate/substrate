package oss

import (
	"context"
	"time"
)

type ListBucketsRequest struct {
	// The name of the bucket from which the list operation begins.
	Marker *string `input:"query,marker"`

	// The maximum number of buckets that can be returned in the single query.
	// Valid values: 1 to 1000.
	MaxKeys int32 `input:"query,max-keys"`

	// The prefix that the names of returned buckets must contain.
	Prefix *string `input:"query,prefix"` // Limits the response to keys that begin with the specified prefix

	// The ID of the resource group.
	ResourceGroupId *string `input:"header,x-oss-resource-group-id"`

	// A tag key of target buckets. The listing results will only include Buckets that have been tagged with this key.
	TagKey *string `input:"query,tag-key"`

	// A tag value for the target buckets. If this parameter is specified in the request, the tag-key must also be specified. The listing results will only include Buckets that have been tagged with this key-value pair.
	TagValue *string `input:"query,tag-value"`

	// Tag list of target buckets. Only Buckets that match all the key-value pairs in the list will added into the listing results. The tagging parameter cannot be used with the tag-key and tag-value parameters in a request.
	Tagging *string `input:"query,tagging"`

	RequestCommon
}

type ListBucketsResult struct {
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

	// The container that stores information about buckets.
	Buckets []BucketProperties `xml:"Buckets>Bucket"`

	ResultCommon
}

type BucketProperties struct {
	// The name of the bucket.
	Name *string `xml:"Name"`

	// The data center in which the bucket is located.
	Location *string `xml:"Location"`

	// The time when the bucket was created. Format: yyyy-mm-ddThh:mm:ss.timezone.
	CreationDate *time.Time `xml:"CreationDate"`

	// The storage class of the bucket. Valid values:
	// Standard, IA, Archive, ColdArchive and DeepColdArchive.
	StorageClass *string `xml:"StorageClass"`

	// The public endpoint used to access the bucket over the Internet.
	ExtranetEndpoint *string `xml:"ExtranetEndpoint"`

	// The internal endpoint that is used to access the bucket from ECS instances
	// that reside in the same region as the bucket.
	IntranetEndpoint *string `xml:"IntranetEndpoint"`

	// The region in which the bucket is located.
	Region *string `xml:"Region"`

	// The ID of the resource group to which the bucket belongs.
	ResourceGroupId *string `xml:"ResourceGroupId"`
}

// ListBuckets Lists buckets that belong to the current account.
func (c *Client) ListBuckets(ctx context.Context, request *ListBucketsRequest, optFns ...func(*Options)) (*ListBucketsResult, error) {
	var err error
	if request == nil {
		request = &ListBucketsRequest{}
	}
	input := &OperationInput{
		OpName: "ListBuckets",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeDefault,
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListBucketsResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}
