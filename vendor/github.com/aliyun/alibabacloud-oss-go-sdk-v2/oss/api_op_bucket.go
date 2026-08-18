package oss

import (
	"context"
	"encoding/xml"
	"net/url"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type PutBucketRequest struct {
	// The name of the bucket to create.
	Bucket *string `input:"host,bucket,required"`

	// The access control list (ACL) of the bucket.
	Acl BucketACLType `input:"header,x-oss-acl"`

	// The ID of the resource group.
	ResourceGroupId *string `input:"header,x-oss-resource-group-id"`

	// The configuration information for the bucket.
	CreateBucketConfiguration *CreateBucketConfiguration `input:"body,CreateBucketConfiguration,xml"`

	RequestCommon
}

type CreateBucketConfiguration struct {
	XMLName xml.Name `xml:"CreateBucketConfiguration"`

	// The storage class of the bucket.
	StorageClass StorageClassType `xml:"StorageClass,omitempty"`

	// The redundancy type of the bucket.
	DataRedundancyType DataRedundancyType `xml:"DataRedundancyType,omitempty"`
}

type PutBucketResult struct {
	ResultCommon
}

// PutBucket Creates a bucket.
func (c *Client) PutBucket(ctx context.Context, request *PutBucketRequest, optFns ...func(*Options)) (*PutBucketResult, error) {
	var err error
	if request == nil {
		request = &PutBucketRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucket",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketResult{}

	if err = c.unmarshalOutput(result, output, discardBody); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteBucketRequest struct {
	// The name of the bucket to delete.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type DeleteBucketResult struct {
	ResultCommon
}

// DeleteBucket Deletes a bucket.
func (c *Client) DeleteBucket(ctx context.Context, request *DeleteBucketRequest, optFns ...func(*Options)) (*DeleteBucketResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucket",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeDefault,
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteBucketResult{}
	if err = c.unmarshalOutput(result, output, discardBody); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type ListObjectsRequest struct {
	// The name of the bucket containing the objects
	Bucket *string `input:"host,bucket,required"`

	// The character that is used to group objects by name. If you specify the delimiter parameter in the request,
	// the response contains the CommonPrefixes parameter. The objects whose names contain the same string from
	// the prefix to the next occurrence of the delimiter are grouped as a single result element in CommonPrefixes.
	Delimiter *string `input:"query,delimiter"`

	// The encoding type of the content in the response. Valid value: url
	EncodingType *string `input:"query,encoding-type"`

	// The name of the object after which the ListObjects (GetBucket) operation starts.
	// If this parameter is specified, objects whose names are alphabetically greater than the marker value are returned.
	Marker *string `input:"query,marker"`

	// The maximum number of objects that you want to return. If the list operation cannot be complete at a time
	// because the max-keys parameter is specified, the NextMarker element is included in the response as the marker
	// for the next list operation.
	MaxKeys int32 `input:"query,max-keys"`

	// The prefix that the names of the returned objects must contain.
	Prefix *string `input:"query,prefix"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type ListObjectsResult struct {
	// The name of the bucket.
	Name *string `xml:"Name"`

	// The prefix contained in the returned object names.
	Prefix *string `xml:"Prefix"`

	// The name of the object after which the list operation begins.
	Marker *string `xml:"Marker"`

	// The maximum number of returned objects in the response.
	MaxKeys int32 `xml:"MaxKeys"`

	// The character that is used to group objects by name.
	Delimiter *string `xml:"Delimiter"`

	// Indicates whether the returned results are truncated.
	// true indicates that not all results are returned this time.
	// false indicates that all results are returned this time.
	IsTruncated bool `xml:"IsTruncated"`

	// The position from which the next list operation starts.
	NextMarker *string `xml:"NextMarker"`

	// The encoding type of the content in the response.
	EncodingType *string `xml:"EncodingType"`

	// The container that stores the metadata of the returned objects.
	Contents []ObjectProperties `xml:"Contents"`

	// If the Delimiter parameter is specified in the request, the response contains the CommonPrefixes element.
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes"`

	ResultCommon
}

type ObjectProperties struct {
	// The name of the object.
	Key *string `xml:"Key"`

	// The type of the object. Valid values: Normal, Multipart and Appendable
	Type *string `xml:"Type"`

	// The size of the returned object. Unit: bytes.
	Size int64 `xml:"Size"`

	// The entity tag (ETag). An ETag is created when an object is created to identify the content of the object.
	ETag *string `xml:"ETag"`

	// The time when the returned objects were last modified.
	LastModified *time.Time `xml:"LastModified"`

	// The storage class of the object.
	StorageClass *string `xml:"StorageClass"`

	// The container that stores information about the bucket owner.
	Owner *Owner `xml:"Owner"`

	// The restoration status of the object.
	RestoreInfo *string `xml:"RestoreInfo"`

	// The time when the storage class of the object is converted to Cold Archive or Deep Cold Archive based on lifecycle rules.
	TransitionTime *time.Time `xml:"TransitionTime"`
}

type Owner struct {
	// The ID of the bucket owner.
	ID *string `xml:"ID"`

	// The name of the object owner.
	DisplayName *string `xml:"DisplayName"`
}

type CommonPrefix struct {
	// The prefix contained in the returned object names.
	Prefix *string `xml:"Prefix"`
}

// ListObjects Lists the information about all objects in a bucket.
func (c *Client) ListObjects(ctx context.Context, request *ListObjectsRequest, optFns ...func(*Options)) (*ListObjectsResult, error) {
	var err error
	if request == nil {
		request = &ListObjectsRequest{}
	}
	input := &OperationInput{
		OpName: "ListObjects",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeDefault,
		},
		Parameters: map[string]string{
			"encoding-type": "url",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5, enableNonStream); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListObjectsResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml, unmarshalEncodeType); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

func unmarshalEncodeType(result any, output *OperationOutput) error {
	switch r := result.(type) {
	case *ListObjectsResult:
		if r.EncodingType != nil && strings.EqualFold(*r.EncodingType, "url") {
			fields := []**string{&r.Prefix, &r.Marker, &r.Delimiter, &r.NextMarker}
			var s string
			var err error
			for _, pp := range fields {
				if pp != nil && *pp != nil {
					if s, err = url.QueryUnescape(**pp); err != nil {
						return err
					}
					*pp = Ptr(s)
				}
			}
			for i := 0; i < len(r.Contents); i++ {
				if r.Contents[i].Key != nil {
					if *r.Contents[i].Key, err = url.QueryUnescape(*r.Contents[i].Key); err != nil {
						return err
					}
				}

			}
			for i := 0; i < len(r.CommonPrefixes); i++ {
				if r.CommonPrefixes[i].Prefix != nil {
					if *r.CommonPrefixes[i].Prefix, err = url.QueryUnescape(*r.CommonPrefixes[i].Prefix); err != nil {
						return err
					}
				}
			}
		}
	case *ListObjectsV2Result:
		if r.EncodingType != nil && strings.EqualFold(*r.EncodingType, "url") {
			fields := []**string{&r.Prefix, &r.StartAfter, &r.Delimiter, &r.ContinuationToken, &r.NextContinuationToken}
			var s string
			var err error
			for _, pp := range fields {
				if pp != nil && *pp != nil {
					if s, err = url.QueryUnescape(**pp); err != nil {
						return err
					}
					*pp = Ptr(s)
				}
			}
			for i := 0; i < len(r.Contents); i++ {
				if r.Contents[i].Key != nil {
					if *r.Contents[i].Key, err = url.QueryUnescape(*r.Contents[i].Key); err != nil {
						return err
					}
				}
			}
			for i := 0; i < len(r.CommonPrefixes); i++ {
				if r.CommonPrefixes[i].Prefix != nil {
					if *r.CommonPrefixes[i].Prefix, err = url.QueryUnescape(*r.CommonPrefixes[i].Prefix); err != nil {
						return err
					}
				}
			}
		}
	case *DeleteMultipleObjectsResult:
		if r.EncodingType != nil && strings.EqualFold(*r.EncodingType, "url") {
			var err error
			for i := 0; i < len(r.DeletedObjects); i++ {
				if r.DeletedObjects[i].Key != nil {
					if *r.DeletedObjects[i].Key, err = url.QueryUnescape(*r.DeletedObjects[i].Key); err != nil {
						return err
					}
				}
			}
		}
	case *InitiateMultipartUploadResult:
		if r.EncodingType != nil && strings.EqualFold(*r.EncodingType, "url") {
			var err error
			if r.Key != nil {
				if *r.Key, err = url.QueryUnescape(*r.Key); err != nil {
					return err
				}
			}
		}
	case *CompleteMultipartUploadResult:
		if r.EncodingType != nil && strings.EqualFold(*r.EncodingType, "url") {
			var err error
			if r.Key != nil {
				if *r.Key, err = url.QueryUnescape(*r.Key); err != nil {
					return err
				}
			}
		}
	case *ListMultipartUploadsResult:
		if r.EncodingType != nil && strings.EqualFold(*r.EncodingType, "url") {
			fields := []**string{&r.KeyMarker, &r.NextKeyMarker, &r.Prefix, &r.Delimiter}
			var s string
			var err error
			for _, pp := range fields {
				if pp != nil && *pp != nil {
					if s, err = url.QueryUnescape(**pp); err != nil {
						return err
					}
					*pp = Ptr(s)
				}
			}
			for i := 0; i < len(r.Uploads); i++ {
				if r.Uploads[i].Key != nil {
					if *r.Uploads[i].Key, err = url.QueryUnescape(*r.Uploads[i].Key); err != nil {
						return err
					}
				}
			}
		}
	case *ListPartsResult:
		if r.EncodingType != nil && strings.EqualFold(*r.EncodingType, "url") {
			fields := []**string{&r.Key}
			var s string
			var err error
			for _, pp := range fields {
				if pp != nil && *pp != nil {
					if s, err = url.QueryUnescape(**pp); err != nil {
						return err
					}
					*pp = Ptr(s)
				}
			}
		}
	case *ListObjectVersionsResult:
		if r.EncodingType != nil && strings.EqualFold(*r.EncodingType, "url") {
			fields := []**string{&r.Prefix, &r.KeyMarker, &r.Delimiter, &r.NextKeyMarker}
			var s string
			var err error
			for _, pp := range fields {
				if pp != nil && *pp != nil {
					if s, err = url.QueryUnescape(**pp); err != nil {
						return err
					}
					*pp = Ptr(s)
				}
			}
			for i := 0; i < len(r.ObjectVersions); i++ {
				if r.ObjectVersions[i].Key != nil {
					if *r.ObjectVersions[i].Key, err = url.QueryUnescape(*r.ObjectVersions[i].Key); err != nil {
						return err
					}
				}

			}
			for i := 0; i < len(r.ObjectDeleteMarkers); i++ {
				if r.ObjectDeleteMarkers[i].Key != nil {
					if *r.ObjectDeleteMarkers[i].Key, err = url.QueryUnescape(*r.ObjectDeleteMarkers[i].Key); err != nil {
						return err
					}
				}
			}
			for i := 0; i < len(r.ObjectVersionsDeleteMarkers); i++ {
				if r.ObjectVersionsDeleteMarkers[i].Key != nil {
					if *r.ObjectVersionsDeleteMarkers[i].Key, err = url.QueryUnescape(*r.ObjectVersionsDeleteMarkers[i].Key); err != nil {
						return err
					}
				}
			}
			for i := 0; i < len(r.CommonPrefixes); i++ {
				if r.CommonPrefixes[i].Prefix != nil {
					if *r.CommonPrefixes[i].Prefix, err = url.QueryUnescape(*r.CommonPrefixes[i].Prefix); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

type ListObjectsV2Request struct {
	// The name of the bucket containing the objects
	Bucket *string `input:"host,bucket,required"`

	// The character that is used to group objects by name. If you specify the delimiter parameter in the request,
	// the response contains the CommonPrefixes parameter. The objects whose names contain the same string from
	// the prefix to the next occurrence of the delimiter are grouped as a single result element in CommonPrefixes.
	Delimiter *string `input:"query,delimiter"`

	// The name of the object after which the ListObjectsV2 (GetBucketV2) operation starts.
	// The objects are returned in alphabetical order of their names. The start-after parameter
	// is used to list the returned objects by page.
	// The value of the parameter must be less than 1,024 bytes in length.
	// Even if the specified start-after value does not exist during a conditional query,
	// the ListObjectsV2 (GetBucketV2) operation starts from the object whose name is alphabetically greater than the start-after value.
	// By default, this parameter is left empty.
	StartAfter *string `input:"query,start-after"`

	// The token from which the ListObjectsV2 (GetBucketV2) operation must start.
	// You can obtain the token from the NextContinuationToken parameter in the ListObjectsV2 (GetBucketV2) response.
	ContinuationToken *string `input:"query,continuation-token"`

	// The maximum number of objects that you want to return. If the list operation cannot be complete at a time
	// because the max-keys parameter is specified, the NextMarker element is included in the response as the marker
	// for the next list operation.
	MaxKeys int32 `input:"query,max-keys"`

	// The prefix that the names of the returned objects must contain.
	Prefix *string `input:"query,prefix"`

	// The encoding type of the content in the response. Valid value: url
	EncodingType *string `input:"query,encoding-type"`

	// Specifies whether to include information about the object owner in the response.
	FetchOwner bool `input:"query,fetch-owner"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type ListObjectsV2Result struct {
	// The name of the bucket.
	Name *string `xml:"Name"`

	// The prefix contained in the returned object names.
	Prefix *string `xml:"Prefix"`

	// If the StartAfter parameter is specified in the request, the response contains the StartAfter parameter.
	StartAfter *string `xml:"StartAfter"`

	// The maximum number of returned objects in the response.
	MaxKeys int32 `xml:"MaxKeys"`

	// The character that is used to group objects by name.
	Delimiter *string `xml:"Delimiter"`

	// Indicates whether the returned results are truncated.
	// true indicates that not all results are returned this time.
	// false indicates that all results are returned this time.
	IsTruncated bool `xml:"IsTruncated"`

	// If the ContinuationToken parameter is specified in the request, the response contains the ContinuationToken parameter.
	ContinuationToken *string `xml:"ContinuationToken"`

	// The name of the object from which the next ListObjectsV2 (GetBucketV2) operation starts.
	// The NextContinuationToken value is used as the ContinuationToken value to query subsequent results.
	NextContinuationToken *string `xml:"NextContinuationToken"`

	// The encoding type of the content in the response.
	EncodingType *string `xml:"EncodingType"`

	// The container that stores the metadata of the returned objects.
	Contents []ObjectProperties `xml:"Contents"`

	// If the Delimiter parameter is specified in the request, the response contains the CommonPrefixes element.
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes"`

	// The number of objects returned for this request. If Delimiter is specified, KeyCount is the sum of the values of Key and CommonPrefixes.
	KeyCount int `xml:"KeyCount"`

	// The time when the storage class of the object is converted to Cold Archive or Deep Cold Archive based on lifecycle rules.
	TransitionTime *time.Time `xml:"TransitionTime"`

	ResultCommon
}

// ListObjectsV2 Queries information about all objects in a bucket.
func (c *Client) ListObjectsV2(ctx context.Context, request *ListObjectsV2Request, optFns ...func(*Options)) (*ListObjectsV2Result, error) {
	var err error
	if request == nil {
		request = &ListObjectsV2Request{}
	}
	input := &OperationInput{
		OpName: "ListObjectsV2",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeDefault,
		},
		Parameters: map[string]string{
			"list-type":     "2",
			"encoding-type": "url",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5, enableNonStream); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListObjectsV2Result{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml, unmarshalEncodeType); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketInfoRequest struct {
	// The name of the bucket containing the objects
	Bucket *string `input:"host,bucket,required"`
	RequestCommon
}

type GetBucketInfoResult struct {
	// The container that stores the bucket information.
	BucketInfo BucketInfo `xml:"Bucket"`
	ResultCommon
}

// BucketInfo defines Bucket information
type BucketInfo struct {
	// The name of the bucket.
	Name *string `xml:"Name"`

	// Indicates whether access tracking is enabled for the bucket.
	AccessMonitor *string `xml:"AccessMonitor"`

	// The region in which the bucket is located.
	Location *string `xml:"Location"`

	// The time when the bucket is created. The time is in UTC.
	CreationDate *time.Time `xml:"CreationDate"`

	// The public endpoint that is used to access the bucket over the Internet.
	ExtranetEndpoint *string `xml:"ExtranetEndpoint"`

	// The internal endpoint that is used to access the bucket from Elastic
	IntranetEndpoint *string `xml:"IntranetEndpoint"`

	// The container that stores the access control list (ACL) information about the bucket.
	ACL *string `xml:"AccessControlList>Grant"`

	// The disaster recovery type of the bucket.
	DataRedundancyType *string `xml:"DataRedundancyType"`

	// The container that stores the information about the bucket owner.
	Owner *Owner `xml:"Owner"`

	// The storage class of the bucket.
	StorageClass *string `xml:"StorageClass"`

	// The ID of the resource group to which the bucket belongs.
	ResourceGroupId *string `xml:"ResourceGroupId"`

	// The container that stores the server-side encryption method.
	SseRule SSERule `xml:"ServerSideEncryptionRule"`

	// Indicates whether versioning is enabled for the bucket.
	Versioning *string `xml:"Versioning"`

	// Indicates whether transfer acceleration is enabled for the bucket.
	TransferAcceleration *string `xml:"TransferAcceleration"`

	// Indicates whether cross-region replication (CRR) is enabled for the bucket.
	CrossRegionReplication *string `xml:"CrossRegionReplication"`

	// The container that stores the logs.
	BucketPolicy BucketPolicy `xml:"BucketPolicy"`

	// The description of the bucket.
	Comment *string `xml:"Comment"`

	// Indicates whether Block Public Access is enabled for the bucket.
	// true: Block Public Access is enabled. false: Block Public Access is disabled.
	BlockPublicAccess *bool `xml:"BlockPublicAccess"`
}

type SSERule struct {
	// The customer master key (CMK) ID in use. A valid value is returned only if you set SSEAlgorithm to KMS
	// and specify the CMK ID. In other cases, an empty value is returned.
	KMSMasterKeyID *string `xml:"KMSMasterKeyID"`

	// The server-side encryption method that is used by default.
	SSEAlgorithm *string `xml:"SSEAlgorithm"`

	// Object's encryption algorithm. If this element is not included in the response,
	// it indicates that the object is using the AES256 encryption algorithm.
	// This option is only valid if the SSEAlgorithm value is KMS.
	KMSDataEncryption *string `xml:"KMSDataEncryption"`
}

type BucketPolicy struct {
	// The name of the bucket that stores the logs.
	LogBucket *string `xml:"LogBucket"`

	// The directory in which logs are stored.
	LogPrefix *string `xml:"LogPrefix"`
}

// GetBucketInfo Queries information about a bucket.
func (c *Client) GetBucketInfo(ctx context.Context, request *GetBucketInfoRequest, optFns ...func(*Options)) (*GetBucketInfoResult, error) {
	var err error
	if request == nil {
		request = &GetBucketInfoRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketInfo",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeDefault,
		},
		Parameters: map[string]string{
			"bucketInfo": "",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketInfoResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml, unmarshalSseRule); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

func unmarshalSseRule(result any, output *OperationOutput) error {
	switch r := result.(type) {
	case *GetBucketInfoResult:
		fields := []*string{r.BucketInfo.SseRule.KMSMasterKeyID, r.BucketInfo.SseRule.SSEAlgorithm, r.BucketInfo.SseRule.KMSDataEncryption}
		for _, pp := range fields {
			if pp != nil && *pp == "None" {
				*pp = ""
			}
		}
	}
	return nil
}

type GetBucketLocationRequest struct {
	// The name of the bucket containing the objects
	Bucket *string `input:"host,bucket,required"`
	RequestCommon
}

type GetBucketLocationResult struct {
	// The region in which the bucket is located.
	LocationConstraint *string `xml:",chardata"`
	ResultCommon
}

// GetBucketLocation Queries the region of an Object Storage Service (OSS) bucket.
func (c *Client) GetBucketLocation(ctx context.Context, request *GetBucketLocationRequest, optFns ...func(*Options)) (*GetBucketLocationResult, error) {
	var err error
	if request == nil {
		request = &GetBucketLocationRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketLocation",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeDefault,
		},
		Parameters: map[string]string{
			"location": "",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &GetBucketLocationResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type GetBucketStatRequest struct {
	// The name of the bucket containing the objects
	Bucket *string `input:"host,bucket,required"`
	RequestCommon
}

type GetBucketStatResult struct {
	// The storage capacity of the bucket. Unit: bytes.
	Storage int64 `xml:"Storage"`

	// The total number of objects that are stored in the bucket.
	ObjectCount int64 `xml:"ObjectCount"`

	// The number of multipart upload tasks that have been initiated but are not completed or canceled.
	MultipartUploadCount int64 `xml:"MultipartUploadCount"`

	// The number of LiveChannels in the bucket.
	LiveChannelCount int64 `xml:"LiveChannelCount"`

	// The time when the obtained information is last modified. The value of this element is a UNIX timestamp. Unit: seconds.
	LastModifiedTime int64 `xml:"LastModifiedTime"`

	// The storage usage of Standard objects in the bucket. Unit: bytes.
	StandardStorage int64 `xml:"StandardStorage"`

	// The number of Standard objects in the bucket.
	StandardObjectCount int64 `xml:"StandardObjectCount"`

	// The billed storage usage of Infrequent Access (IA) objects in the bucket. Unit: bytes.
	InfrequentAccessStorage int64 `xml:"InfrequentAccessStorage"`

	// The actual storage usage of IA objects in the bucket. Unit: bytes.
	InfrequentAccessRealStorage int64 `xml:"InfrequentAccessRealStorage"`

	// The number of IA objects in the bucket.
	InfrequentAccessObjectCount int64 `xml:"InfrequentAccessObjectCount"`

	// The billed storage usage of Archive objects in the bucket. Unit: bytes.
	ArchiveStorage int64 `xml:"ArchiveStorage"`

	// The actual storage usage of Archive objects in the bucket. Unit: bytes.
	ArchiveRealStorage int64 `xml:"ArchiveRealStorage"`

	// The number of Archive objects in the bucket.
	ArchiveObjectCount int64 `xml:"ArchiveObjectCount"`

	// The billed storage usage of Cold Archive objects in the bucket. Unit: bytes.
	ColdArchiveStorage int64 `xml:"ColdArchiveStorage"`

	// The actual storage usage of Cold Archive objects in the bucket. Unit: bytes.
	ColdArchiveRealStorage int64 `xml:"ColdArchiveRealStorage"`

	// The number of Cold Archive objects in the bucket.
	ColdArchiveObjectCount int64 `xml:"ColdArchiveObjectCount"`

	// The number of Deep Cold Archive objects in the bucket.
	DeepColdArchiveObjectCount int64 `xml:"DeepColdArchiveObjectCount"`

	// The billed storage usage of Deep Cold Archive objects in the bucket. Unit: bytes.
	DeepColdArchiveStorage int64 `xml:"DeepColdArchiveStorage"`

	// The actual storage usage of Deep Cold Archive objects in the bucket. Unit: bytes.
	DeepColdArchiveRealStorage int64 `xml:"DeepColdArchiveRealStorage"`

	// The number of multipart parts in the bucket.
	MultipartPartCount int64 `xml:"MultipartPartCount"`

	// The number of delete marker in the bucket.
	DeleteMarkerCount int64 `xml:"DeleteMarkerCount"`

	ResultCommon
}

// GetBucketStat Queries the storage capacity of a specified bucket and the number of objects that are stored in the bucket.
func (c *Client) GetBucketStat(ctx context.Context, request *GetBucketStatRequest, optFns ...func(*Options)) (*GetBucketStatResult, error) {
	var err error
	if request == nil {
		request = &GetBucketStatRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketStat",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeDefault,
		},
		Parameters: map[string]string{
			"stat": "",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &GetBucketStatResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type PutBucketAclRequest struct {
	// The name of the bucket containing the objects
	Bucket *string `input:"host,bucket,required"`

	// The access control list (ACL) of the object.
	Acl BucketACLType `input:"header,x-oss-acl,required"`

	RequestCommon
}

type PutBucketAclResult struct {
	ResultCommon
}

// PutBucketAcl You can call this operation to configure or modify the ACL of a bucket.
func (c *Client) PutBucketAcl(ctx context.Context, request *PutBucketAclRequest, optFns ...func(*Options)) (*PutBucketAclResult, error) {
	var err error
	if request == nil {
		request = &PutBucketAclRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketAcl",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeDefault,
		},
		Parameters: map[string]string{
			"acl": "",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &PutBucketAclResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type GetBucketAclRequest struct {
	// The name of the bucket containing the objects
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketAclResult struct {
	// The container that stores the access control list (ACL) information about the bucket.
	ACL *string `xml:"AccessControlList>Grant"`

	// The container that stores information about the bucket owner.
	Owner *Owner `xml:"Owner"`

	ResultCommon
}

// GetBucketAcl You can call this operation to query the ACL of a bucket.
func (c *Client) GetBucketAcl(ctx context.Context, request *GetBucketAclRequest, optFns ...func(*Options)) (*GetBucketAclResult, error) {
	var err error
	if request == nil {
		request = &GetBucketAclRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketAcl",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeDefault,
		},
		Parameters: map[string]string{
			"acl": "",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &GetBucketAclResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type PutBucketVersioningRequest struct {
	// The name of the bucket containing the objects
	Bucket *string `input:"host,bucket,required"`

	VersioningConfiguration *VersioningConfiguration `input:"body,VersioningConfiguration,xml,required"`

	RequestCommon
}

type VersioningConfiguration struct {
	// The versioning state of the bucket. Valid values: Enabled,Suspended
	Status VersioningStatusType `xml:"Status"`
}

type PutBucketVersioningResult struct {
	ResultCommon
}

// PutBucketVersioning Configures the versioning state for a bucket.
func (c *Client) PutBucketVersioning(ctx context.Context, request *PutBucketVersioningRequest, optFns ...func(*Options)) (*PutBucketVersioningResult, error) {
	var err error
	if request == nil {
		request = &PutBucketVersioningRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketVersioning",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"versioning": "",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &PutBucketVersioningResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type GetBucketVersioningRequest struct {
	// The name of the bucket containing the objects
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketVersioningResult struct {
	// The versioning state of the bucket. Valid values: Enabled,Suspended
	VersionStatus *string `xml:"Status"`

	ResultCommon
}

// GetBucketVersioning You can call this operation to query the versioning state of a bucket.
func (c *Client) GetBucketVersioning(ctx context.Context, request *GetBucketVersioningRequest, optFns ...func(*Options)) (*GetBucketVersioningResult, error) {
	var err error
	if request == nil {
		request = &GetBucketVersioningRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketVersioning",
		Method: "GET",
		Parameters: map[string]string{
			"versioning": "",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &GetBucketVersioningResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type ListObjectVersionsRequest struct {
	// The name of the bucket containing the objects
	Bucket *string `input:"host,bucket,required"`

	// The character that is used to group objects by name. If you specify the delimiter parameter in the request,
	// the response contains the CommonPrefixes parameter. The objects whose names contain the same string from
	// the prefix to the next occurrence of the delimiter are grouped as a single result element in CommonPrefixes.
	Delimiter *string `input:"query,delimiter"`

	// Specifies that objects whose names are alphabetically after the value of the key-marker parameter are returned.
	// This parameter can be specified together with version-id-marker.
	// By default, this parameter is left empty.
	KeyMarker *string `input:"query,key-marker"`

	// Specifies that the versions created before the version specified by version-id-marker for the object
	// whose name is specified by key-marker are returned by creation time in descending order.
	// By default, if this parameter is not specified, the results are returned from the latest
	// version of the object whose name is alphabetically after the value of key-marker.
	VersionIdMarker *string `input:"query,version-id-marker"`

	// The maximum number of objects that you want to return. If the list operation cannot be complete at a time
	// because the max-keys parameter is specified, the NextMarker element is included in the response as the marker
	// for the next list operation.
	MaxKeys int32 `input:"query,max-keys"`

	// The prefix that the names of the returned objects must contain.
	Prefix *string `input:"query,prefix"`

	// The encoding type of the content in the response. Valid value: url
	EncodingType *string `input:"query,encoding-type"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	// To indicate that whether to stores the versions of objects and delete markers together in one container.
	// When false(default), stores the versions of objects into ListObjectVersionsResult.ObjectVersions,
	// When false(default), stores the delete markers into ListObjectVersionsResult.ObjectDeleteMarkers,
	// When true, stores the versions and delete markers into ListObjectVersionsResult.ObjectVersionsDeleteMarkers,
	IsMix bool

	RequestCommon
}

type ListObjectVersionsResult struct {
	// The name of the bucket.
	Name *string `xml:"Name"`

	// Indicates the object from which the ListObjectVersions (GetBucketVersions) operation starts.
	KeyMarker *string `xml:"KeyMarker"`

	// The version from which the ListObjectVersions (GetBucketVersions) operation starts.
	// This parameter is used together with KeyMarker.
	VersionIdMarker *string `xml:"VersionIdMarker"`

	// If not all results are returned for the request, the NextKeyMarker parameter is included
	// in the response to indicate the key-marker value of the next ListObjectVersions (GetBucketVersions) request.
	NextKeyMarker *string `xml:"NextKeyMarker"`

	// If not all results are returned for the request, the NextVersionIdMarker parameter is included in
	// the response to indicate the version-id-marker value of the next ListObjectVersions (GetBucketVersions) request.
	NextVersionIdMarker *string `xml:"NextVersionIdMarker"`

	// The container that stores delete markers.
	ObjectDeleteMarkers []ObjectDeleteMarkerProperties `xml:"DeleteMarker"`

	// The container that stores the versions of objects, excluding delete markers.
	ObjectVersions []ObjectVersionProperties `xml:"Version"`

	// The container that stores the versions of objects and delete markers together in the order they are returned.
	// Only valid when ListObjectVersionsRequest.IsMix is set to true
	ObjectVersionsDeleteMarkers []ObjectMixProperties `xml:"ObjectMix"`

	// The prefix contained in the returned object names.
	Prefix *string `xml:"Prefix"`

	// The maximum number of returned objects in the response.
	MaxKeys int32 `xml:"MaxKeys"`

	// The character that is used to group objects by name.
	Delimiter *string `xml:"Delimiter"`

	// Indicates whether the returned results are truncated.
	// true indicates that not all results are returned this time.
	// false indicates that all results are returned this time.
	IsTruncated bool `xml:"IsTruncated"`

	// The encoding type of the content in the response.
	EncodingType *string `xml:"EncodingType"`

	// If the Delimiter parameter is specified in the request, the response contains the CommonPrefixes element.
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes"`

	ResultCommon
}

type ObjectMixProperties ObjectVersionProperties

func (m ObjectMixProperties) IsDeleteMarker() bool {
	if m.VersionId != nil && m.Type == nil {
		return true
	}
	return false
}

type ObjectDeleteMarkerProperties struct {
	// The name of the object.
	Key *string `xml:"Key"`

	// The version ID of the object.
	VersionId *string `xml:"VersionId"`

	// Indicates whether the version is the current version.
	IsLatest bool `xml:"IsLatest"`

	// The time when the returned objects were last modified.
	LastModified *time.Time `xml:"LastModified"`

	// The container that stores information about the bucket owner.
	Owner *Owner `xml:"Owner"`
}

type ObjectVersionProperties struct {
	// The name of the object.
	Key *string `xml:"Key"`

	// The version ID of the object.
	VersionId *string `xml:"VersionId"`

	// Indicates whether the version is the current version.
	IsLatest bool `xml:"IsLatest"`

	// The time when the returned objects were last modified.
	LastModified *time.Time `xml:"LastModified"`

	// The type of the returned object.
	Type *string `xml:"Type"`

	// The size of the returned object. Unit: bytes.
	Size int64 `xml:"Size"`

	// The entity tag (ETag) that is generated when an object is created. ETags are used to identify the content of objects.
	ETag *string `xml:"ETag"`

	// The storage class of the object.
	StorageClass *string `xml:"StorageClass"`

	// The container that stores information about the bucket owner.
	Owner *Owner `xml:"Owner"`

	// The restoration status of the object.
	RestoreInfo *string `xml:"RestoreInfo"`

	// The time when the storage class of the object is converted to Cold Archive or Deep Cold Archive based on lifecycle rules.
	TransitionTime *time.Time `xml:"TransitionTime"`
}

// ListObjectVersions Lists the versions of all objects in a bucket, including delete markers.
func (c *Client) ListObjectVersions(ctx context.Context, request *ListObjectVersionsRequest, optFns ...func(*Options)) (*ListObjectVersionsResult, error) {
	var err error
	if request == nil {
		request = &ListObjectVersionsRequest{}
	}
	input := &OperationInput{
		OpName: "ListObjectVersions",
		Method: "GET",
		Parameters: map[string]string{
			"versions":      "",
			"encoding-type": "url",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, updateContentMd5, enableNonStream); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListObjectVersionsResult{}
	var unmarshalFns []func(result any, output *OperationOutput) error
	if request.IsMix {
		unmarshalFns = append(unmarshalFns, unmarshalBodyXmlVersions)
	} else {
		unmarshalFns = append(unmarshalFns, unmarshalBodyXml)
	}
	unmarshalFns = append(unmarshalFns, unmarshalEncodeType)
	if err = c.unmarshalOutput(result, output, unmarshalFns...); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutBucketRequestPaymentRequest struct {
	// The name of the bucket containing the objects
	Bucket *string `input:"host,bucket,required"`

	// The request payment configuration information for the bucket.
	PaymentConfiguration *RequestPaymentConfiguration `input:"body,RequestPaymentConfiguration,xml,required"`

	RequestCommon
}

type RequestPaymentConfiguration struct {
	XMLName xml.Name `xml:"RequestPaymentConfiguration"`

	// The payer of the request and traffic fees.
	Payer PayerType `xml:"Payer"`
}

type PutBucketRequestPaymentResult struct {
	ResultCommon
}

// PutBucketRequestPayment You can call this operation to enable pay-by-requester for a bucket.
func (c *Client) PutBucketRequestPayment(ctx context.Context, request *PutBucketRequestPaymentRequest, optFns ...func(*Options)) (*PutBucketRequestPaymentResult, error) {
	var err error
	if request == nil {
		request = &PutBucketRequestPaymentRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketRequestPayment",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeDefault,
		},
		Parameters: map[string]string{
			"requestPayment": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"requestPayment"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &PutBucketRequestPaymentResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type GetBucketRequestPaymentRequest struct {
	// The name of the bucket containing the objects
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketRequestPaymentResult struct {
	// Indicates who pays the download and request fees.
	Payer *string `xml:"Payer"`

	ResultCommon
}

// GetBucketRequestPayment You can call this operation to obtain pay-by-requester configurations for a bucket.
func (c *Client) GetBucketRequestPayment(ctx context.Context, request *GetBucketRequestPaymentRequest, optFns ...func(*Options)) (*GetBucketRequestPaymentResult, error) {
	var err error
	if request == nil {
		request = &GetBucketRequestPaymentRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketRequestPayment",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeDefault,
		},
		Parameters: map[string]string{
			"requestPayment": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"requestPayment"})
	if err = c.marshalInput(request, input); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &GetBucketRequestPaymentResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}
