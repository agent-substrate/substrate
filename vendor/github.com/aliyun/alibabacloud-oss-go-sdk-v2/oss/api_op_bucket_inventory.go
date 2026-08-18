package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type InventoryOSSBucketDestination struct {
	// The format of exported inventory lists. The exported inventory lists are CSV objects compressed by using GZIP.
	Format InventoryFormatType `xml:"Format"`

	// The ID of the account to which permissions are granted by the bucket owner.
	AccountId *string `xml:"AccountId"`

	// The Alibaba Cloud Resource Name (ARN) of the role that has the permissions to read all objects from the source bucket and write objects to the destination bucket. Format: `acs:ram::uid:role/rolename`.
	RoleArn *string `xml:"RoleArn"`

	// The name of the bucket in which exported inventory lists are stored.
	Bucket *string `xml:"Bucket"`

	// The prefix of the path in which the exported inventory lists are stored.
	Prefix *string `xml:"Prefix"`

	// The container that stores the encryption method of the exported inventory lists.
	Encryption *InventoryEncryption `xml:"Encryption"`
}

type InventoryDestination struct {
	// The container that stores information about the bucket in which exported inventory lists are stored.
	OSSBucketDestination *InventoryOSSBucketDestination `xml:"OSSBucketDestination"`
}

type InventorySchedule struct {
	// The frequency at which the inventory list is exported. Valid values:- Daily: The inventory list is exported on a daily basis. - Weekly: The inventory list is exported on a weekly basis.
	Frequency InventoryFrequencyType `xml:"Frequency"`

	// The day of the month on which the inventory list is exported. This parameter is required if the Frequency is Monthly.
	DayOfMonth *int `xml:"DayOfMonth"`
}

type InventoryFilter struct {
	// The beginning of the time range during which the object was last modified. Unit: seconds.Valid values: [1262275200, 253402271999]
	LastModifyBeginTimeStamp *int64 `xml:"LastModifyBeginTimeStamp"`

	// The end of the time range during which the object was last modified. Unit: seconds.Valid values: [1262275200, 253402271999]
	LastModifyEndTimeStamp *int64 `xml:"LastModifyEndTimeStamp"`

	// The minimum size of the specified object. Unit: B.Valid values: [0 B, 48.8 TB]
	LowerSizeBound *int64 `xml:"LowerSizeBound"`

	// The maximum size of the specified object. Unit: B.Valid values: (0 B, 48.8 TB]
	UpperSizeBound *int64 `xml:"UpperSizeBound"`

	// The storage class of the object. You can specify multiple storage classes.Valid values:StandardIAArchiveColdArchiveAll
	StorageClass *string `xml:"StorageClass"`

	// The prefix that is specified in the inventory.
	Prefix *string `xml:"Prefix"`
}

type SSEKMS struct {
	// The ID of the key that is managed by Key Management Service (KMS).
	KeyId *string `xml:"KeyId"`
}

type InventoryEncryption struct {
	// The container that stores information about the SSE-OSS encryption method.
	SseOss *string `xml:"SSE-OSS"`

	// The container that stores the customer master key (CMK) used for SSE-KMS encryption.
	SseKms *SSEKMS `xml:"SSE-KMS"`
}

type InventoryConfiguration struct {
	// The name of the inventory. The name must be unique in the bucket.
	Id *string `xml:"Id"`

	// Specifies whether to enable the bucket inventory feature. Valid values:*   true*   false
	IsEnabled *bool `xml:"IsEnabled"`

	// The container that stores the exported inventory lists.
	Destination *InventoryDestination `xml:"Destination"`

	// The container that stores information about the frequency at which inventory lists are exported.
	Schedule *InventorySchedule `xml:"Schedule"`

	// The container that stores the prefix used to filter objects. Only objects whose names contain the specified prefix are included in the inventory.
	Filter *InventoryFilter `xml:"Filter"`

	// Specifies whether to include the version information about the objects in inventory lists. Valid values:*   All: The information about all versions of the objects is exported.*   Current: Only the information about the current versions of the objects is exported.
	IncludedObjectVersions *string `xml:"IncludedObjectVersions"`

	// The container that stores the configuration fields in inventory lists.
	OptionalFields *OptionalFields `xml:"OptionalFields"`

	// IncrementalInventory Configuration container for incremental inventory.
	IncrementalInventory *IncrementalInventory `xml:"IncrementalInventory"`
}

type ListInventoryConfigurationsResult struct {
	// The container that stores inventory configurations.
	InventoryConfigurations []InventoryConfiguration `xml:"InventoryConfiguration"`

	// Specifies whether to list all inventory tasks configured for the bucket.Valid values: true and false- The value of false indicates that all inventory tasks configured for the bucket are listed.- The value of true indicates that not all inventory tasks configured for the bucket are listed. To list the next page of inventory configurations, set the continuation-token parameter in the next request to the value of the NextContinuationToken header in the response to the current request.
	IsTruncated *bool `xml:"IsTruncated"`

	// If the value of IsTruncated in the response is true and value of this header is not null, set the continuation-token parameter in the next request to the value of this header.
	NextContinuationToken *string `xml:"NextContinuationToken"`
}

type OptionalFields struct {
	// The configuration fields that are included in inventory lists. Available configuration fields:*   Size: the size of the object.*   LastModifiedDate: the time when the object was last modified.*   ETag: the ETag of the object. It is used to identify the content of the object.*   StorageClass: the storage class of the object.*   IsMultipartUploaded: specifies whether the object is uploaded by using multipart upload.*   EncryptionStatus: the encryption status of the object.
	Fields []InventoryOptionalFieldType `xml:"Field"`
}

type PutBucketInventoryRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the inventory.
	InventoryId *string `input:"query,inventoryId,required"`

	// Request body schema.
	InventoryConfiguration *InventoryConfiguration `input:"body,InventoryConfiguration,xml,required"`

	RequestCommon
}

// IncrementalInventory Configuration container for incremental inventory.
type IncrementalInventory struct {
	// IsEnabled Specifies whether incremental inventory is enabled.
	IsEnabled *bool `xml:"IsEnabled"`

	// Schedule Container for incremental inventory export cycle.
	Schedule *IncrementInventorySchedule `xml:"Schedule"`

	// OptionalFields Configuration container for incremental inventory file attributes.
	OptionalFields *IncrementalInventoryOptionalFields `xml:"OptionalFields"`
}

type IncrementalInventoryOptionalFields struct {
	Fields []IncrementalInventoryOptionalFieldType `xml:"Field"`
}

// IncrementInventorySchedule Container for incremental inventory export cycle information.
type IncrementInventorySchedule struct {
	// Frequency Describes the frequency at which incremental inventory files are exported.
	Frequency *int64 `xml:"Frequency"`
}

type PutBucketInventoryResult struct {
	ResultCommon
}

// PutBucketInventory Configures an inventory for a bucket.
func (c *Client) PutBucketInventory(ctx context.Context, request *PutBucketInventoryRequest, optFns ...func(*Options)) (*PutBucketInventoryResult, error) {
	var err error
	if request == nil {
		request = &PutBucketInventoryRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketInventory",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"inventory": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"inventory", "inventoryId"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketInventoryResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketInventoryRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the inventory to be queried.
	InventoryId *string `input:"query,inventoryId,required"`

	RequestCommon
}

type GetBucketInventoryResult struct {
	// The inventory task configured for a bucket.
	InventoryConfiguration *InventoryConfiguration `output:"body,InventoryConfiguration,xml"`

	ResultCommon
}

// GetBucketInventory Queries the inventories that are configured for a bucket.
func (c *Client) GetBucketInventory(ctx context.Context, request *GetBucketInventoryRequest, optFns ...func(*Options)) (*GetBucketInventoryResult, error) {
	var err error
	if request == nil {
		request = &GetBucketInventoryRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketInventory",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"inventory": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"inventory", "inventoryId"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketInventoryResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type ListBucketInventoryRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// Specify the start position of the list operation. You can obtain this token from the NextContinuationToken field of last ListBucketInventory's result.
	ContinuationToken *string `input:"query,continuation-token"`

	RequestCommon
}

type ListBucketInventoryResult struct {
	// The container that stores inventory configuration list.
	ListInventoryConfigurationsResult *ListInventoryConfigurationsResult `output:"body,ListInventoryConfigurationsResult,xml"`

	ResultCommon
}

// ListBucketInventory Queries all inventories in a bucket at a time.
func (c *Client) ListBucketInventory(ctx context.Context, request *ListBucketInventoryRequest, optFns ...func(*Options)) (*ListBucketInventoryResult, error) {
	var err error
	if request == nil {
		request = &ListBucketInventoryRequest{}
	}
	input := &OperationInput{
		OpName: "ListBucketInventory",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"inventory": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"inventory"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListBucketInventoryResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteBucketInventoryRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the inventory that you want to delete.
	InventoryId *string `input:"query,inventoryId,required"`

	RequestCommon
}

type DeleteBucketInventoryResult struct {
	ResultCommon
}

// DeleteBucketInventory Deletes an inventory for a bucket.
func (c *Client) DeleteBucketInventory(ctx context.Context, request *DeleteBucketInventoryRequest, optFns ...func(*Options)) (*DeleteBucketInventoryResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketInventoryRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucketInventory",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"inventory": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"inventory", "inventoryId"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteBucketInventoryResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
