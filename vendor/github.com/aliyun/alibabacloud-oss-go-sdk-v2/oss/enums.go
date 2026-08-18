package oss

// BucketACLType The access control list (ACL) of the bucket
type BucketACLType string

// Enum values for BucketACLType
const (
	// BucketACLPrivate Only the bucket owner can perform read and write operations on objects in the bucket.
	// Other users cannot access the objects in the bucket.
	BucketACLPrivate BucketACLType = "private"

	// BucketACLPublicRead Only the bucket owner can write data to objects in the bucket.
	// Other users, including anonymous users, can only read objects in the bucket.
	BucketACLPublicRead BucketACLType = "public-read"

	// BucketACLPublicReadWrite All users, including anonymous users, can perform read and write operations on the bucket.
	BucketACLPublicReadWrite BucketACLType = "public-read-write"
)

// StorageClassType The storage class of the bucket
type StorageClassType string

// Enum values for StorageClassType
const (
	// StorageClassStandard Standard provides highly reliable, highly available,
	// and high-performance object storage for data that is frequently accessed.
	StorageClassStandard StorageClassType = "Standard"

	// StorageClassIA IA provides highly durable storage at lower prices compared with Standard.
	// IA has a minimum billable size of 64 KB and a minimum billable storage duration of 30 days.
	StorageClassIA StorageClassType = "IA"

	// StorageClassArchive Archive provides high-durability storage at lower prices compared with Standard and IA.
	// Archive has a minimum billable size of 64 KB and a minimum billable storage duration of 60 days.
	StorageClassArchive StorageClassType = "Archive"

	// StorageClassColdArchive Cold Archive provides highly durable storage at lower prices compared with Archive.
	// Cold Archive has a minimum billable size of 64 KB and a minimum billable storage duration of 180 days.
	StorageClassColdArchive StorageClassType = "ColdArchive"

	// StorageClassDeepColdArchive Deep Cold Archive provides highly durable storage at lower prices compared with Cold Archive.
	// Deep Cold Archive has a minimum billable size of 64 KB and a minimum billable storage duration of 180 days.
	StorageClassDeepColdArchive StorageClassType = "DeepColdArchive"
)

// DataRedundancyType The redundancy type of the bucket
type DataRedundancyType string

// Enum values for BucketACLType
const (
	// DataRedundancyLRS Locally redundant storage (LRS) stores copies of each object across different devices in the same zone.
	// This ensures data reliability and availability even if two storage devices are damaged at the same time.
	DataRedundancyLRS DataRedundancyType = "LRS"

	// DataRedundancyZRS Zone-redundant storage (ZRS) uses the multi-zone mechanism to distribute user data across
	// multiple zones in the same region. If one zone becomes unavailable, you can continue to access the data
	// that is stored in other zones.
	DataRedundancyZRS DataRedundancyType = "ZRS"
)

// ObjectACLType The access control list (ACL) of the object
type ObjectACLType string

// Enum values for ObjectACLType
const (
	// ObjectACLPrivate Only the object owner is allowed to perform read and write operations on the object.
	// Other users cannot access the object.
	ObjectACLPrivate ObjectACLType = "private"

	// ObjectACLPublicRead Only the object owner can write data to the object.
	// Other users, including anonymous users, can only read the object.
	ObjectACLPublicRead ObjectACLType = "public-read"

	// ObjectACLPublicReadWrite All users, including anonymous users, can perform read and write operations on the object.
	ObjectACLPublicReadWrite ObjectACLType = "public-read-write"

	// ObjectACLDefault The ACL of the object is the same as that of the bucket in which the object is stored.
	ObjectACLDefault ObjectACLType = "default"
)

// VersioningStatusType bucket versioning status
type VersioningStatusType string

const (
	// VersionEnabled Versioning Status definition: Enabled
	VersionEnabled VersioningStatusType = "Enabled"

	// VersionSuspended Versioning Status definition: Suspended
	VersionSuspended VersioningStatusType = "Suspended"
)

// PayerType the type of request payer
type PayerType string

const (
	// Requester the requester who send the request
	Requester PayerType = "Requester"

	// BucketOwner the requester who send the request
	BucketOwner PayerType = "BucketOwner"
)

// BucketWormStateType the type of bucket worm state
type BucketWormStateType string

const (
	BucketWormStateInProgress BucketWormStateType = "InProgress"
	BucketWormStateLocked     BucketWormStateType = "Locked"
)

// InventoryFormatType The format of exported inventory lists
type InventoryFormatType string

// InventoryFormatCSV Enum values for InventoryFormatType
const (
	InventoryFormatCSV InventoryFormatType = "CSV"
)

// InventoryFrequencyType The frequency at which inventory lists are exported
type InventoryFrequencyType string

// Enum values for InventoryFrequencyType
const (
	InventoryFrequencyDaily   InventoryFrequencyType = "Daily"
	InventoryFrequencyWeekly  InventoryFrequencyType = "Weekly"
	InventoryFrequencyMonthly InventoryFrequencyType = "Monthly"
	InventoryFrequencyOnce    InventoryFrequencyType = "Once"
)

// InventoryOptionalFieldType The configuration fields that are included in inventory lists.
type InventoryOptionalFieldType string

// Enum values for InventoryOptionalFieldType
const (
	InventoryOptionalFieldSize                InventoryOptionalFieldType = "Size"
	InventoryOptionalFieldLastModifiedDate    InventoryOptionalFieldType = "LastModifiedDate"
	InventoryOptionalFieldETag                InventoryOptionalFieldType = "ETag"
	InventoryOptionalFieldStorageClass        InventoryOptionalFieldType = "StorageClass"
	InventoryOptionalFieldIsMultipartUploaded InventoryOptionalFieldType = "IsMultipartUploaded"
	InventoryOptionalFieldEncryptionStatus    InventoryOptionalFieldType = "EncryptionStatus"
	InventoryOptionalFieldTransitionTime      InventoryOptionalFieldType = "TransitionTime"
	InventoryOptionalFieldObjectAcl           InventoryOptionalFieldType = "ObjectAcl"
	InventoryOptionalFieldTaggingCount        InventoryOptionalFieldType = "TaggingCount"
	InventoryOptionalFieldObjectType          InventoryOptionalFieldType = "ObjectType"
	InventoryOptionalFieldCRC64               InventoryOptionalFieldType = "Crc64"
)

// IncrementalInventoryOptionalFieldType The optional fields included in OSS incremental inventory reports.
type IncrementalInventoryOptionalFieldType string

// Enum values for IncrementalInventoryOptionalFieldType
const (
	// IncrementalInventoryOptionalFieldSequenceNumber Sequence number.
	IncrementalInventoryOptionalFieldSequenceNumber IncrementalInventoryOptionalFieldType = "SequenceNumber"

	// IncrementalInventoryOptionalFieldRecordType Event type.
	IncrementalInventoryOptionalFieldRecordType IncrementalInventoryOptionalFieldType = "RecordType"

	// IncrementalInventoryOptionalFieldRecordTimestamp Timestamp.
	IncrementalInventoryOptionalFieldRecordTimestamp IncrementalInventoryOptionalFieldType = "RecordTimestamp"

	// IncrementalInventoryOptionalFieldRequester Alibaba Cloud ID or Principal ID of the requester.
	IncrementalInventoryOptionalFieldRequester IncrementalInventoryOptionalFieldType = "Requester"

	// IncrementalInventoryOptionalFieldSourceIp Source IP of the requester.
	IncrementalInventoryOptionalFieldSourceIp IncrementalInventoryOptionalFieldType = "SourceIp"

	// IncrementalInventoryOptionalFieldRequestId Unique identifier of the request.
	IncrementalInventoryOptionalFieldRequestId IncrementalInventoryOptionalFieldType = "RequestId"

	// IncrementalInventoryOptionalFieldSize Size of the object.
	IncrementalInventoryOptionalFieldSize IncrementalInventoryOptionalFieldType = "Size"

	// IncrementalInventoryOptionalFieldStorageClass Storage class of the object.
	IncrementalInventoryOptionalFieldStorageClass IncrementalInventoryOptionalFieldType = "StorageClass"

	// IncrementalInventoryOptionalFieldLastModifiedDate Last modified date of the object.
	IncrementalInventoryOptionalFieldLastModifiedDate IncrementalInventoryOptionalFieldType = "LastModifiedDate"

	// IncrementalInventoryOptionalFieldETag ETag used to identify the content of an object.
	IncrementalInventoryOptionalFieldETag IncrementalInventoryOptionalFieldType = "ETag"

	// IncrementalInventoryOptionalFieldIsMultipartUploaded Whether the object is uploaded via multipart upload.
	IncrementalInventoryOptionalFieldIsMultipartUploaded IncrementalInventoryOptionalFieldType = "IsMultipartUploaded"

	// IncrementalInventoryOptionalFieldObjectType Type of the object.
	IncrementalInventoryOptionalFieldObjectType IncrementalInventoryOptionalFieldType = "ObjectType"

	// IncrementalInventoryOptionalFieldObjectAcl Read/write permissions of the object.
	IncrementalInventoryOptionalFieldObjectAcl IncrementalInventoryOptionalFieldType = "ObjectAcl"

	// IncrementalInventoryOptionalFieldCRC64 CRC64 of the object.
	IncrementalInventoryOptionalFieldCRC64 IncrementalInventoryOptionalFieldType = "Crc64"

	// IncrementalInventoryOptionalFieldEncryptionStatus Whether the object is encrypted.
	IncrementalInventoryOptionalFieldEncryptionStatus IncrementalInventoryOptionalFieldType = "EncryptionStatus"
)

// AccessMonitorStatusType The type of access monitor status
type AccessMonitorStatusType string

// Enum values for AccessMonitorStatusType
const (
	AccessMonitorStatusEnabled  AccessMonitorStatusType = "Enabled"
	AccessMonitorStatusDisabled AccessMonitorStatusType = "Disabled"
)

type HistoricalObjectReplicationType string

// Enum values for HistoricalObjectReplicationType
const (
	HistoricalObjectReplicationEnabled  HistoricalObjectReplicationType = "enabled"
	HistoricalObjectReplicationDisabled HistoricalObjectReplicationType = "disabled"
)

type TransferTypeType string

// Enum values for TransferTypeType
const (
	TransferTypeInternal TransferTypeType = "internal"
	TransferTypeOssAcc   TransferTypeType = "oss_acc"
)

type StatusType string

// Enum values for StatusType
const (
	StatusEnabled  StatusType = "Enabled"
	StatusDisabled StatusType = "Disabled"
)

type MetaQueryOrderType string

// Enum values for MetaQueryOrderType
const (
	MetaQueryOrderAsc  MetaQueryOrderType = "asc"
	MetaQueryOrderDesc MetaQueryOrderType = "desc"
)

// OSS headers
const (
	HeaderOssPrefix                      string = "X-Oss-"
	HeaderOssMetaPrefix                         = "X-Oss-Meta-"
	HeaderOssACL                                = "X-Oss-Acl"
	HeaderOssObjectACL                          = "X-Oss-Object-Acl"
	HeaderOssObjectType                         = "X-Oss-Object-Type"
	HeaderOssSecurityToken                      = "X-Oss-Security-Token"
	HeaderOssServerSideEncryption               = "X-Oss-Server-Side-Encryption"
	HeaderOssServerSideEncryptionKeyID          = "X-Oss-Server-Side-Encryption-Key-Id"
	HeaderOssServerSideDataEncryption           = "X-Oss-Server-Side-Data-Encryption"
	HeaderOssSSECAlgorithm                      = "X-Oss-Server-Side-Encryption-Customer-Algorithm"
	HeaderOssSSECKey                            = "X-Oss-Server-Side-Encryption-Customer-Key"
	HeaderOssSSECKeyMd5                         = "X-Oss-Server-Side-Encryption-Customer-Key-MD5"
	HeaderOssCopySource                         = "X-Oss-Copy-Source"
	HeaderOssCopySourceRange                    = "X-Oss-Copy-Source-Range"
	HeaderOssCopySourceIfMatch                  = "X-Oss-Copy-Source-If-Match"
	HeaderOssCopySourceIfNoneMatch              = "X-Oss-Copy-Source-If-None-Match"
	HeaderOssCopySourceIfModifiedSince          = "X-Oss-Copy-Source-If-Modified-Since"
	HeaderOssCopySourceIfUnmodifiedSince        = "X-Oss-Copy-Source-If-Unmodified-Since"
	HeaderOssMetadataDirective                  = "X-Oss-Metadata-Directive"
	HeaderOssNextAppendPosition                 = "X-Oss-Next-Append-Position"
	HeaderOssRequestID                          = "X-Oss-Request-Id"
	HeaderOssCRC64                              = "X-Oss-Hash-Crc64ecma"
	HeaderOssSymlinkTarget                      = "X-Oss-Symlink-Target"
	HeaderOssStorageClass                       = "X-Oss-Storage-Class"
	HeaderOssCallback                           = "X-Oss-Callback"
	HeaderOssCallbackVar                        = "X-Oss-Callback-Var"
	HeaderOssRequester                          = "X-Oss-Request-Payer"
	HeaderOssTagging                            = "X-Oss-Tagging"
	HeaderOssTaggingDirective                   = "X-Oss-Tagging-Directive"
	HeaderOssTrafficLimit                       = "X-Oss-Traffic-Limit"
	HeaderOssForbidOverWrite                    = "X-Oss-Forbid-Overwrite"
	HeaderOssRangeBehavior                      = "X-Oss-Range-Behavior"
	HeaderOssAllowSameActionOverLap             = "X-Oss-Allow-Same-Action-Overlap"
	HeaderOssDate                               = "X-Oss-Date"
	HeaderOssContentSha256                      = "X-Oss-Content-Sha256"
	HeaderOssEC                                 = "X-Oss-Ec"
	HeaderOssERR                                = "X-Oss-Err"
)

// OSS headers for client sider encryption
const (
	OssClientSideEncryptionKey                      string = "X-Oss-Meta-Client-Side-Encryption-Key"
	OssClientSideEncryptionStart                           = "X-Oss-Meta-Client-Side-Encryption-Start"
	OssClientSideEncryptionCekAlg                          = "X-Oss-Meta-Client-Side-Encryption-Cek-Alg"
	OssClientSideEncryptionWrapAlg                         = "X-Oss-Meta-Client-Side-Encryption-Wrap-Alg"
	OssClientSideEncryptionMatDesc                         = "X-Oss-Meta-Client-Side-Encryption-Matdesc"
	OssClientSideEncryptionUnencryptedContentLength        = "X-Oss-Meta-Client-Side-Encryption-Unencrypted-Content-Length"
	OssClientSideEncryptionUnencryptedContentMD5           = "X-Oss-Meta-Client-Side-Encryption-Unencrypted-Content-Md5"
	OssClientSideEncryptionDataSize                        = "X-Oss-Meta-Client-Side-Encryption-Data-Size"
	OssClientSideEncryptionPartSize                        = "X-Oss-Meta-Client-Side-Encryption-Part-Size"
)

// HTTP headers
const (
	HTTPHeaderAcceptEncoding     string = "Accept-Encoding"
	HTTPHeaderAuthorization             = "Authorization"
	HTTPHeaderCacheControl              = "Cache-Control"
	HTTPHeaderContentDisposition        = "Content-Disposition"
	HTTPHeaderContentEncoding           = "Content-Encoding"
	HTTPHeaderContentLength             = "Content-Length"
	HTTPHeaderContentMD5                = "Content-MD5"
	HTTPHeaderContentType               = "Content-Type"
	HTTPHeaderContentLanguage           = "Content-Language"
	HTTPHeaderContentRange              = "Content-Range"
	HTTPHeaderDate                      = "Date"
	HTTPHeaderETag                      = "ETag"
	HTTPHeaderExpires                   = "Expires"
	HTTPHeaderHost                      = "Host"
	HTTPHeaderLastModified              = "Last-Modified"
	HTTPHeaderRange                     = "Range"
	HTTPHeaderLocation                  = "Location"
	HTTPHeaderUserAgent                 = "User-Agent"
	HTTPHeaderIfModifiedSince           = "If-Modified-Since"
	HTTPHeaderIfUnmodifiedSince         = "If-Unmodified-Since"
	HTTPHeaderIfMatch                   = "If-Match"
	HTTPHeaderIfNoneMatch               = "If-None-Match"
)

type UrlStyleType int

const (
	UrlStyleVirtualHosted UrlStyleType = iota
	UrlStylePath
	UrlStyleCName
)

func (f UrlStyleType) String() string {
	switch f {
	default:
		return "virtual-hosted-style"
	case UrlStylePath:
		return "path-style"
	case UrlStyleCName:
		return "cname-style"
	}
}

type FeatureFlagsType int

const (
	// FeatureCorrectClockSkew If the client time is different from server time by more than about 15 minutes,
	// the requests your application makes will be signed with the incorrect time, and the server will reject them.
	// The feature to help to identify this case, and SDK will correct for clock skew.
	FeatureCorrectClockSkew FeatureFlagsType = 1 << iota

	FeatureEnableMD5

	// FeatureAutoDetectMimeType Content-Type is automatically added based on the object name if not specified.
	// This feature takes effect for PutObject, AppendObject and InitiateMultipartUpload
	FeatureAutoDetectMimeType

	// FeatureEnableCRC64CheckUpload check data integrity of uploads via the crc64.
	// This feature takes effect for PutObject, AppendObject, UploadPart, Uploader.UploadFrom and Uploader.UploadFile
	FeatureEnableCRC64CheckUpload

	// FeatureEnableCRC64CheckDownload check data integrity of downloads via the crc64.
	// This feature takes effect for Downloader.DownloadFile
	FeatureEnableCRC64CheckDownload

	FeatureFlagsDefault = FeatureCorrectClockSkew + FeatureAutoDetectMimeType +
		FeatureEnableCRC64CheckUpload + FeatureEnableCRC64CheckDownload
)

type SignatureVersionType int

const (
	SignatureVersionV1 SignatureVersionType = iota
	SignatureVersionV4
)

func (f SignatureVersionType) String() string {
	switch f {
	case SignatureVersionV4:
		return "OSS Signature Version 4"
	default:
		return "OSS Signature Version 1"
	}
}

type AuthMethodType int

const (
	AuthMethodHeader AuthMethodType = iota
	AuthMethodQuery
)

func (f AuthMethodType) String() string {
	switch f {
	case AuthMethodQuery:
		return "authentication in query"
	default:
		return "authentication in header"
	}
}

// OperationMetadata Keys
const (
	OpMetaKeyResponsHandler     string = "opm-response-handler"
	OpMetaKeyRequestBodyTracker string = "opm-request-body-tracker"
	OpMetaKeyIsBucketArn        string = "opm-is-bucket-arn"
)
