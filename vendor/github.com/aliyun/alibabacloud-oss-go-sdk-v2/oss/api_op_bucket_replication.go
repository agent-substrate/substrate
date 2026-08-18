package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type RtcConfiguration struct {
	// The container that stores the status of RTC.
	RTC *ReplicationTimeControl `xml:"RTC"`

	// The ID of the data replication rule for which you want to configure RTC.
	ID *string `xml:"ID"`
}

type ReplicationSourceSelectionCriteria struct {
	// The container that is used to filter the source objects that are encrypted by using SSE-KMS. This parameter must be specified if the SourceSelectionCriteria parameter is specified in the data replication rule.
	SseKmsEncryptedObjects *SseKmsEncryptedObjects `xml:"SseKmsEncryptedObjects"`
}

type ReplicationPrefixSet struct {
	// The prefix that is used to specify the object that you want to replicate. Only objects whose names contain the specified prefix are replicated to the destination bucket.*   The value of the Prefix parameter can be up to 1,023 characters in length.*   If you specify the Prefix parameter in a data replication rule, OSS synchronizes new data and historical data based on the value of the Prefix parameter.
	Prefixs []string `xml:"Prefix"`
}

type ReplicationProgressRule struct {
	// The container that stores the information about the destination bucket.
	Destination *ReplicationDestination `xml:"Destination"`

	// The status of the data replication task. Valid values:*   starting: OSS creates a data replication task after a data replication rule is configured.*   doing: The replication rule is effective and the replication task is in progress.*   closing: OSS clears a data replication task after the corresponding data replication rule is deleted.
	Status *string `xml:"Status"`

	// Specifies whether to replicate historical data that exists before data replication is enabled from the source bucket to the destination bucket.*   enabled (default): replicates historical data to the destination bucket.*   disabled: ignores historical data and replicates only data uploaded to the source bucket after data replication is enabled for the source bucket.
	HistoricalObjectReplication *string `xml:"HistoricalObjectReplication"`

	// The container that stores the progress of the data replication task. This parameter is returned only when the data replication task is in the doing state.
	Progress *ReplicationProgressInformation `xml:"Progress"`

	// The ID of the data replication rule.
	ID *string `xml:"ID"`

	// The container that stores prefixes. You can specify up to 10 prefixes in each data replication rule.
	PrefixSet *ReplicationPrefixSet `xml:"PrefixSet"`

	// The operations that are synchronized to the destination bucket.*   ALL: PUT, DELETE, and ABORT operations are synchronized to the destination bucket.*   PUT: Write operations are synchronized to the destination bucket, including PutObject, PostObject, AppendObject, CopyObject, PutObjectACL, InitiateMultipartUpload, UploadPart, UploadPartCopy, and CompleteMultipartUpload.
	Action *string `xml:"Action"`
}

type ReplicationDestination struct {
	// The destination bucket to which data is replicated.
	Bucket *string `xml:"Bucket"`

	// The region in which the destination bucket is located.
	Location *string `xml:"Location"`

	// The link that is used to transfer data during data replication. Valid values:*   internal (default): the default data transfer link used in OSS.*   oss_acc: the transfer acceleration link. You can set TransferType to oss_acc only when you create CRR rules.
	TransferType TransferTypeType `xml:"TransferType"`
}

type SseKmsEncryptedObjects struct {
	// Specifies whether to replicate objects that are encrypted by using SSE-KMS. Valid values:*   Enabled*   Disabled
	Status StatusType `xml:"Status"`
}

type LocationTransferType struct {
	// The regions in which the destination bucket can be located.
	Location *string `xml:"Location"`

	// The container that stores the transfer type.
	TransferTypes *TransferTypes `xml:"TransferTypes"`
}

type ReplicationTimeControl struct {
	// Specifies whether to enable RTC.Valid values:*   disabled            *   enabled
	Status *string `xml:"Status"`
}

type ReplicationRule struct {
	// The container that stores the information about the destination bucket.
	Destination *ReplicationDestination `xml:"Destination"`

	// The role that you want to authorize OSS to use to replicate data. If you want to use SSE-KMS to encrypt the objects that are replicated to the destination bucket, you must specify this parameter.
	SyncRole *string `xml:"SyncRole"`

	// The container that specifies other conditions used to filter the source objects that you want to replicate. Filter conditions can be specified only for source objects encrypted by using SSE-KMS.
	SourceSelectionCriteria *ReplicationSourceSelectionCriteria `xml:"SourceSelectionCriteria"`

	// The encryption configuration for the objects replicated to the destination bucket. If the Status parameter is set to Enabled, you must specify this parameter.
	EncryptionConfiguration *ReplicationEncryptionConfiguration `xml:"EncryptionConfiguration"`

	// Specifies whether to replicate historical data that exists before data replication is enabled from the source bucket to the destination bucket. Valid values:*   enabled (default): replicates historical data to the destination bucket.*   disabled: does not replicate historical data to the destination bucket. Only data uploaded to the source bucket after data replication is enabled for the source bucket is replicated.
	HistoricalObjectReplication HistoricalObjectReplicationType `xml:"HistoricalObjectReplication"`

	// The container that stores the status of the RTC feature.
	RTC *ReplicationTimeControl `xml:"RTC"`

	// The ID of the rule.
	ID *string `xml:"ID"`

	// The container that stores prefixes. You can specify up to 10 prefixes in each data replication rule.
	PrefixSet *ReplicationPrefixSet `xml:"PrefixSet"`

	// The operations that can be synchronized to the destination bucket. If you configure Action in a data replication rule, OSS synchronizes new data and historical data based on the specified value of Action. You can set Action to one or more of the following operation types. Valid values:*   ALL (default): PUT, DELETE, and ABORT operations are synchronized to the destination bucket.*   PUT: Write operations are synchronized to the destination bucket, including PutObject, PostObject, AppendObject, CopyObject, PutObjectACL, InitiateMultipartUpload, UploadPart, UploadPartCopy, and CompleteMultipartUpload.
	Action *string `xml:"Action"`

	// The status of the data replication task. Valid values:*   starting: OSS creates a data replication task after a data replication rule is configured.*   doing: The replication rule is effective and the replication task is in progress.*   closing: OSS clears a data replication task after the corresponding data replication rule is deleted.
	Status *string `xml:"Status"`
}

type ReplicationConfiguration struct {
	// The container that stores the data replication rules.
	Rules []ReplicationRule `xml:"Rule"`
}

type LocationTransferTypeConstraint struct {
	// The container that stores regions in which the destination bucket can be located with the TransferType information.
	LocationTransferTypes []LocationTransferType `xml:"LocationTransferType"`
}

type LocationRTCConstraint struct {
	// The regions where RTC is supported.
	Locations []string `xml:"Location"`
}

type ReplicationLocation struct {
	// The regions in which the destination bucket can be located.
	Locations []string `xml:"Location"`

	// The container that stores regions in which the destination bucket can be located with TransferType specified.
	LocationTransferTypeConstraint *LocationTransferTypeConstraint `xml:"LocationTransferTypeConstraint"`

	// The container that stores regions in which the RTC can be enabled.
	LocationRTCConstraint *LocationRTCConstraint `xml:"LocationRTCConstraint"`
}

type ReplicationProgress struct {
	// The container that stores the progress of the data replication task corresponding to each data replication rule.
	Rules []ReplicationProgressRule `xml:"Rule"`
}

type ReplicationEncryptionConfiguration struct {
	ReplicaKmsKeyID *string `xml:"ReplicaKmsKeyID"`
}

type TransferTypes struct {
	// The data transfer type that is used to transfer data in data replication. Valid values:*   internal (default): the default data transfer link used in OSS.*   oss_acc: the link in which data transmission is accelerated. You can set TransferType to oss_acc only when you create CRR rules.
	Types []string `xml:"Type"`
}

type ReplicationProgressInformation struct {
	// The percentage of the replicated historical data. This parameter is valid only when HistoricalObjectReplication is set to enabled.
	HistoricalObject *string `xml:"HistoricalObject"`

	// The time used to determine whether data is replicated to the destination bucket. Data that is written to the source bucket before the time is replicated to the destination bucket. The value of this parameter is in the GMT format. Example: Thu, 24 Sep 2015 15:39:18 GMT.
	NewObject *string `xml:"NewObject"`
}

type PutBucketRtcRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The container of the request body.
	RtcConfiguration *RtcConfiguration `input:"body,ReplicationRule,xml,required"`

	RequestCommon
}

type PutBucketRtcResult struct {
	ResultCommon
}

// PutBucketRtc Enables or disables the Replication Time Control (RTC) feature for existing cross-region replication (CRR) rules.
func (c *Client) PutBucketRtc(ctx context.Context, request *PutBucketRtcRequest, optFns ...func(*Options)) (*PutBucketRtcResult, error) {
	var err error
	if request == nil {
		request = &PutBucketRtcRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketRtc",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"rtc": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"rtc"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketRtcResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutBucketReplicationRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The container of the request body.
	ReplicationConfiguration *ReplicationConfiguration `input:"body,ReplicationConfiguration,xml,required"`

	RequestCommon
}

type PutBucketReplicationResult struct {
	ReplicationRuleId *string `output:"header,x-oss-replication-rule-id"`

	ResultCommon
}

// PutBucketReplication Configures data replication rules for a bucket. Object Storage Service (OSS) supports cross-region replication (CRR) and same-region replication (SRR).
func (c *Client) PutBucketReplication(ctx context.Context, request *PutBucketReplicationRequest, optFns ...func(*Options)) (*PutBucketReplicationResult, error) {
	var err error
	if request == nil {
		request = &PutBucketReplicationRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketReplication",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"comp":        "add",
			"replication": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"replication", "comp"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutBucketReplicationResult{}

	if err = c.unmarshalOutput(result, output, unmarshalHeader, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketReplicationRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketReplicationResult struct {
	// The container that stores data replication configurations.
	ReplicationConfiguration *ReplicationConfiguration `output:"body,ReplicationConfiguration,xml"`

	ResultCommon
}

// GetBucketReplication Queries the data replication rules configured for a bucket.
func (c *Client) GetBucketReplication(ctx context.Context, request *GetBucketReplicationRequest, optFns ...func(*Options)) (*GetBucketReplicationResult, error) {
	var err error
	if request == nil {
		request = &GetBucketReplicationRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketReplication",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"replication": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"replication"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketReplicationResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketReplicationLocationRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketReplicationLocationResult struct {
	// The container that stores the region in which the destination bucket can be located.
	ReplicationLocation *ReplicationLocation `output:"body,ReplicationLocation,xml"`

	ResultCommon
}

// GetBucketReplicationLocation Queries the regions in which available destination buckets reside. You can determine the region of the destination bucket to which the data in the source bucket are replicated based on the returned response.
func (c *Client) GetBucketReplicationLocation(ctx context.Context, request *GetBucketReplicationLocationRequest, optFns ...func(*Options)) (*GetBucketReplicationLocationResult, error) {
	var err error
	if request == nil {
		request = &GetBucketReplicationLocationRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketReplicationLocation",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"replicationLocation": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"replicationLocation"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketReplicationLocationResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetBucketReplicationProgressRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The ID of the data replication rule. You can call the GetBucketReplication operation to query the ID.
	RuleId *string `input:"query,rule-id,required"`

	RequestCommon
}

type GetBucketReplicationProgressResult struct {
	// The container that is used to store the progress of data replication tasks.
	ReplicationProgress *ReplicationProgress `output:"body,ReplicationProgress,xml"`

	ResultCommon
}

// GetBucketReplicationProgress Queries the information about the data replication process of a bucket.
func (c *Client) GetBucketReplicationProgress(ctx context.Context, request *GetBucketReplicationProgressRequest, optFns ...func(*Options)) (*GetBucketReplicationProgressResult, error) {
	var err error
	if request == nil {
		request = &GetBucketReplicationProgressRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketReplicationProgress",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"replicationProgress": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"replicationProgress"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetBucketReplicationProgressResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type ReplicationRules struct {
	// The ID of data replication rules that you want to delete. You can call the GetBucketReplication operation to obtain the ID.
	IDs []string `xml:"ID"`
}

type DeleteBucketReplicationRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The container of the request body.
	ReplicationRules *ReplicationRules `input:"body,ReplicationRules,xml,required"`

	RequestCommon
}

type DeleteBucketReplicationResult struct {
	ResultCommon
}

// DeleteBucketReplication Disables data replication for a bucket and deletes the data replication rule configured for the bucket. After you call this operation, all operations performed on the source bucket are not synchronized to the destination bucket.
func (c *Client) DeleteBucketReplication(ctx context.Context, request *DeleteBucketReplicationRequest, optFns ...func(*Options)) (*DeleteBucketReplicationResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketReplicationRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucketReplication",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"comp":        "delete",
			"replication": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"comp", "replication"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteBucketReplicationResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
