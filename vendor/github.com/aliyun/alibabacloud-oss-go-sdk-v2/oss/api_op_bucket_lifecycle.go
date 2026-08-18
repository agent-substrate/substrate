package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type LifecycleRule struct {
	// Specifies whether to enable the rule. Valid values:*   Enabled: enables the rule. OSS periodically executes the rule.*   Disabled: does not enable the rule. OSS ignores the rule.
	Status *string `xml:"Status"`

	// The delete operation that you want OSS to perform on the parts that are uploaded in incomplete multipart upload tasks when the parts expire.
	AbortMultipartUpload *LifecycleRuleAbortMultipartUpload `xml:"AbortMultipartUpload"`

	// Timestamp for when access tracking was enabled.
	AtimeBase *int64 `xml:"AtimeBase"`

	// The conversion of the storage class of previous versions of the objects that match the lifecycle rule when the previous versions expire. The storage class of the previous versions can be converted to IA or Archive. The period of time from when the previous versions expire to when the storage class of the previous versions is converted to Archive must be longer than the period of time from when the previous versions expire to when the storage class of the previous versions is converted to IA.
	NoncurrentVersionTransitions []NoncurrentVersionTransition `xml:"NoncurrentVersionTransition"`

	// The container that stores the Not parameter that is used to filter objects.
	Filter *LifecycleRuleFilter `xml:"Filter"`

	// The ID of the lifecycle rule. The ID can contain up to 255 characters. If you do not specify the ID, OSS automatically generates a unique ID for the lifecycle rule.
	ID *string `xml:"ID"`

	// The prefix in the names of the objects to which the rule applies. The prefixes specified by different rules cannot overlap.*   If Prefix is specified, this rule applies only to objects whose names contain the specified prefix in the bucket.*   If Prefix is not specified, this rule applies to all objects in the bucket.
	Prefix *string `xml:"Prefix"`

	// The delete operation to perform on objects based on the lifecycle rule. For an object in a versioning-enabled bucket, the delete operation specified by this parameter is performed only on the current version of the object.The period of time from when the objects expire to when the objects are deleted must be longer than the period of time from when the objects expire to when the storage class of the objects is converted to IA or Archive.
	Expiration *LifecycleRuleExpiration `xml:"Expiration"`

	// The conversion of the storage class of objects that match the lifecycle rule when the objects expire. The storage class of the objects can be converted to IA, Archive, and ColdArchive. The storage class of Standard objects in a Standard bucket can be converted to IA, Archive, or Cold Archive. The period of time from when the objects expire to when the storage class of the objects is converted to Archive must be longer than the period of time from when the objects expire to when the storage class of the objects is converted to IA. For example, if the validity period is set to 30 for objects whose storage class is converted to IA after the validity period, the validity period must be set to a value greater than 30 for objects whose storage class is converted to Archive.  Either Days or CreatedBeforeDate is required.
	Transitions []LifecycleRuleTransition `xml:"Transition"`

	// The tag of the objects to which the lifecycle rule applies. You can specify multiple tags.
	Tags []Tag `xml:"Tag"`

	// The delete operation that you want OSS to perform on the previous versions of the objects that match the lifecycle rule when the previous versions expire.
	NoncurrentVersionExpiration *NoncurrentVersionExpiration `xml:"NoncurrentVersionExpiration"`
}

type LifecycleRuleAbortMultipartUpload struct {
	// The number of days from when the objects were last modified to when the lifecycle rule takes effect.

	Days *int32 `xml:"Days"`

	// The date based on which the lifecycle rule takes effect. OSS performs the specified operation on data whose last modified date is earlier than this date. Specify the time in the ISO 8601 standard. The time must be at 00:00:00 in UTC.
	CreatedBeforeDate *string `xml:"CreatedBeforeDate"`

	// Deprecated: please use Days or CreateDateBefore.
	// The date after which the lifecycle rule takes effect. If the specified time is earlier than the current moment, it'll takes effect immediately. (This fields is NOT RECOMMENDED, please use Days or CreateDateBefore)
	Date *string `xml:"Date"`
}

type LifecycleRuleNot struct {
	// The tag of the objects to which the lifecycle rule does not apply.
	Tag *Tag `xml:"Tag"`

	// The prefix in the names of the objects to which the lifecycle rule does not apply.
	Prefix *string `xml:"Prefix"`
}

type LifecycleRuleFilter struct {
	// The condition that is matched by objects to which the lifecycle rule does not apply.
	Nots []LifecycleRuleNot `xml:"Not"`

	// This lifecycle rule only applies to files larger than this size.
	ObjectSizeGreaterThan *int64 `xml:"ObjectSizeGreaterThan"`

	// This lifecycle rule only applies to files smaller than this size.
	ObjectSizeLessThan *int64 `xml:"ObjectSizeLessThan"`
}

type LifecycleRuleExpiration struct {
	// The date based on which the lifecycle rule takes effect. OSS performs the specified operation on data whose last modified date is earlier than this date. The value of this parameter is in the yyyy-MM-ddT00:00:00.000Z format.Specify the time in the ISO 8601 standard. The time must be at 00:00:00 in UTC.
	CreatedBeforeDate *string `xml:"CreatedBeforeDate"`

	// The number of days from when the objects were last modified to when the lifecycle rule takes effect.
	Days *int32 `xml:"Days"`

	// Specifies whether to automatically remove expired delete markers.*   true: Expired delete markers are automatically removed. If you set this parameter to true, you cannot specify the Days or CreatedBeforeDate parameter.*   false: Expired delete markers are not automatically removed. If you set this parameter to false, you must specify the Days or CreatedBeforeDate parameter.
	ExpiredObjectDeleteMarker *bool `xml:"ExpiredObjectDeleteMarker"`

	// Deprecated: please use Days or CreateDateBefore.
	// The date after which the lifecycle rule takes effect. If the specified time is earlier than the current moment, it'll takes effect immediately. (This fields is NOT RECOMMENDED, please use Days or CreateDateBefore)
	Date *string `xml:"Date"`
}

type NoncurrentVersionExpiration struct {
	// The number of days from when the objects became previous versions to when the lifecycle rule takes effect.
	NoncurrentDays *int32 `xml:"NoncurrentDays"`
}

type NoncurrentVersionTransition struct {
	// Specifies whether the lifecycle rule applies to objects based on their last access time. Valid values:*   true: The rule applies to objects based on their last access time.*   false: The rule applies to objects based on their last modified time.
	IsAccessTime *bool `xml:"IsAccessTime"`

	// Specifies whether to convert the storage class of non-Standard objects back to Standard after the objects are accessed. This parameter takes effect only when the IsAccessTime parameter is set to true. Valid values:*   true: converts the storage class of the objects to Standard.*   false: does not convert the storage class of the objects to Standard.
	ReturnToStdWhenVisit *bool `xml:"ReturnToStdWhenVisit"`

	// Specifies whether to convert the storage class of objects whose sizes are less than 64 KB to IA, Archive, or Cold Archive based on their last access time. Valid values:*   true: converts the storage class of objects that are smaller than 64 KB to IA, Archive, or Cold Archive. Objects that are smaller than 64 KB are charged as 64 KB. Objects that are greater than or equal to 64 KB are charged based on their actual sizes. If you set this parameter to true, the storage fees may increase.*   false: does not convert the storage class of an object that is smaller than 64 KB.
	AllowSmallFile *bool `xml:"AllowSmallFile"`

	// The number of days from when the objects became previous versions to when the lifecycle rule takes effect.
	NoncurrentDays *int32 `xml:"NoncurrentDays"`

	// The storage class to which objects are converted. Valid values:*   IA*   Archive*   ColdArchive  You can convert the storage class of objects in an IA bucket to only Archive or Cold Archive.
	StorageClass StorageClassType `xml:"StorageClass"`
}

type LifecycleRuleTransition struct {
	// The date based on which the lifecycle rule takes effect. OSS performs the specified operation on data whose last modified date is earlier than this date. Specify the time in the ISO 8601 standard. The time must be at 00:00:00 in UTC.
	CreatedBeforeDate *string `xml:"CreatedBeforeDate"`

	// The number of days from when the objects were last modified to when the lifecycle rule takes effect.
	Days *int32 `xml:"Days"`

	// The storage class to which objects are converted. Valid values:*   IA*   Archive*   ColdArchive  You can convert the storage class of objects in an IA bucket to only Archive or Cold Archive.
	StorageClass StorageClassType `xml:"StorageClass"`

	// Specifies whether the lifecycle rule applies to objects based on their last access time. Valid values:*   true: The rule applies to objects based on their last access time.*   false: The rule applies to objects based on their last modified time.
	IsAccessTime *bool `xml:"IsAccessTime"`

	// Specifies whether to convert the storage class of non-Standard objects back to Standard after the objects are accessed. This parameter takes effect only when the IsAccessTime parameter is set to true. Valid values:*   true: converts the storage class of the objects to Standard.*   false: does not convert the storage class of the objects to Standard.
	ReturnToStdWhenVisit *bool `xml:"ReturnToStdWhenVisit"`

	// Specifies whether to convert the storage class of objects whose sizes are less than 64 KB to IA, Archive, or Cold Archive based on their last access time. Valid values:*   true: converts the storage class of objects that are smaller than 64 KB to IA, Archive, or Cold Archive. Objects that are smaller than 64 KB are charged as 64 KB. Objects that are greater than or equal to 64 KB are charged based on their actual sizes. If you set this parameter to true, the storage fees may increase.*   false: does not convert the storage class of an object that is smaller than 64 KB.
	AllowSmallFile *bool `xml:"AllowSmallFile"`
}

type LifecycleConfiguration struct {
	// The container that stores the lifecycle rules.
	Rules []LifecycleRule `xml:"Rule"`
}

type PutBucketLifecycleRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// Specifies whether to allow overlapped prefixes. Valid values:true: Overlapped prefixes are allowed.false: Overlapped prefixes are not allowed.
	AllowSameActionOverlap *string `input:"header,x-oss-allow-same-action-overlap"`

	// The container of the request body.
	LifecycleConfiguration *LifecycleConfiguration `input:"body,LifecycleConfiguration,xml,required"`

	RequestCommon
}

type PutBucketLifecycleResult struct {
	ResultCommon
}

// PutBucketLifecycle Configures a lifecycle rule for a bucket. After you configure a lifecycle rule for a bucket, Object Storage Service (OSS) automatically deletes the objects that match the rule or converts the storage type of the objects based on the point in time that is specified in the lifecycle rule.
func (c *Client) PutBucketLifecycle(ctx context.Context, request *PutBucketLifecycleRequest, optFns ...func(*Options)) (*PutBucketLifecycleResult, error) {
	var err error
	if request == nil {
		request = &PutBucketLifecycleRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketLifecycle",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"lifecycle": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"lifecycle"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketLifecycleResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type GetBucketLifecycleRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketLifecycleResult struct {
	// The container that stores the lifecycle rules configured for the bucket.
	LifecycleConfiguration *LifecycleConfiguration `output:"body,LifecycleConfiguration,xml"`

	ResultCommon
}

// GetBucketLifecycle Queries the lifecycle rules configured for a bucket. Only the owner of a bucket has the permissions to query the lifecycle rules configured for the bucket.
func (c *Client) GetBucketLifecycle(ctx context.Context, request *GetBucketLifecycleRequest, optFns ...func(*Options)) (*GetBucketLifecycleResult, error) {
	var err error
	if request == nil {
		request = &GetBucketLifecycleRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketLifecycle",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"lifecycle": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"lifecycle"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketLifecycleResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type DeleteBucketLifecycleRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type DeleteBucketLifecycleResult struct {
	ResultCommon
}

// DeleteBucketLifecycle Deletes the lifecycle rules of a bucket.
func (c *Client) DeleteBucketLifecycle(ctx context.Context, request *DeleteBucketLifecycleRequest, optFns ...func(*Options)) (*DeleteBucketLifecycleResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketLifecycleRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucketLifecycle",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"lifecycle": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"lifecycle"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteBucketLifecycleResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}
