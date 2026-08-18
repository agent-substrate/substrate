package oss

import (
	"context"
	"fmt"
	"hash"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type PutObjectRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The caching behavior of the web page when the object is downloaded.
	CacheControl *string `input:"header,Cache-Control"`

	// The method that is used to access the object.
	ContentDisposition *string `input:"header,Content-Disposition"`

	// The method that is used to encode the object.
	ContentEncoding *string `input:"header,Content-Encoding"`

	// The size of the data in the HTTP message body. Unit: bytes.
	ContentLength *int64 `input:"header,Content-Length"`

	// The MD5 hash of the object that you want to upload.
	ContentMD5 *string `input:"header,Content-MD5"`

	// A standard MIME type describing the format of the contents.
	ContentType *string `input:"header,Content-Type"`

	// The expiration time of the cache in UTC.
	Expires *string `input:"header,Expires"`

	// Specifies whether the object that is uploaded by calling the PutObject operation overwrites the existing object that has the same name.
	// When versioning is enabled or suspended for the bucket to which you want to upload the object, the x-oss-forbid-overwrite header does not take effect. In this case, the object that is uploaded by calling the PutObject operation overwrites the existing object that has the same name. Default value: false.
	// If you do not specify the x-oss-forbid-overwrite header or you set the x-oss-forbid-overwrite header to false, the object that is uploaded by calling the PutObject operation overwrites the existing object that has the same name.
	// If you set the x-oss-forbid-overwrite header to true, an existing object that has the same name cannot be overwritten.
	ForbidOverwrite *string `input:"header,x-oss-forbid-overwrite"`

	// The encryption method on the server side when an object is created. Valid values: AES256, KMS, SM4.
	// If you specify the header, the header is returned in the response.
	// OSS uses the method that is specified by this header to encrypt the uploaded object.
	// When you download the encrypted object, the x-oss-server-side-encryption header is included in the response and the header value is set to the algorithm that is used to encrypt the object.
	ServerSideEncryption *string `input:"header,x-oss-server-side-encryption"`

	// Specify the encryption algorithm for the object. Valid values: SM4.
	// If this option is not specified, it indicates that the Object uses AES256 encryption algorithm.
	// This option is only valid when x-oss-ser-side-encryption is KMS.
	ServerSideDataEncryption *string `input:"header,x-oss-server-side-data-encryption"`

	// Deprecated: Please use ServerSideEncryptionKeyId
	SSEKMSKeyId *string `input:"header,x-oss-server-side-encryption-key-id"`

	// The ID of the customer master key (CMK) that is managed by Key Management Service (KMS).
	// This header is valid only when the x-oss-server-side-encryption header is set to KMS.
	ServerSideEncryptionKeyId *string `input:"header,x-oss-server-side-encryption-key-id"`

	// The access control list (ACL) of the object.
	Acl ObjectACLType `input:"header,x-oss-object-acl"`

	// The storage class of the object.
	StorageClass StorageClassType `input:"header,x-oss-storage-class"`

	// The metadata of the object that you want to upload.
	Metadata map[string]string `input:"header,x-oss-meta-,usermeta"`

	// The tags that are specified for the object by using a key-value pair.
	// You can specify multiple tags for an object. Example: TagA=A&TagB=B.
	Tagging *string `input:"header,x-oss-tagging"`

	// A callback parameter is a Base64-encoded string that contains multiple fields in the JSON format.
	Callback *string `input:"header,x-oss-callback"`

	// Configure custom parameters by using the callback-var parameter.
	CallbackVar *string `input:"header,x-oss-callback-var"`

	// Specify the speed limit value. The speed limit value ranges from 245760 to 838860800, with a unit of bit/s.
	TrafficLimit int64 `input:"header,x-oss-traffic-limit"`

	// Object data.
	Body io.Reader `input:"body,nop"`

	// Progress callback function
	ProgressFn ProgressFunc

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type PutObjectResult struct {
	// Content-Md5 for the uploaded object.
	ContentMD5 *string `output:"header,Content-MD5"`

	// Entity tag for the uploaded object.
	ETag *string `output:"header,ETag"`

	// The 64-bit CRC value of the object.
	// This value is calculated based on the ECMA-182 standard.
	HashCRC64 *string `output:"header,x-oss-hash-crc64ecma"`

	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	CallbackResult map[string]any

	ResultCommon
}

// PutObject Uploads a object.
func (c *Client) PutObject(ctx context.Context, request *PutObjectRequest, optFns ...func(*Options)) (*PutObjectResult, error) {
	var err error
	if request == nil {
		request = &PutObjectRequest{}
	}
	input := &OperationInput{
		OpName: "PutObject",
		Method: "PUT",
		Bucket: request.Bucket,
		Key:    request.Key,
	}

	marshalFns := []func(any, *OperationInput) error{
		addProgress,
		c.updateContentType,
		c.addCrcCheck,
	}
	unmarshalFns := []func(result any, output *OperationOutput) error{
		unmarshalHeader,
	}

	if request.Callback != nil {
		marshalFns = append(marshalFns, addCallback)
		unmarshalFns = append(unmarshalFns, unmarshalCallbackBody)
	} else {
		unmarshalFns = append(unmarshalFns, discardBody)
	}

	if err = c.marshalInput(request, input, marshalFns...); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutObjectResult{}
	if err = c.unmarshalOutput(result, output, unmarshalFns...); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type HTTPRange struct {
	Offset int64
	Count  int64
}

func (r HTTPRange) FormatHTTPRange() *string {
	if r.Offset == 0 && r.Count == 0 {
		return nil // No specified range
	}
	endOffset := "" // if count == CountToEnd (0)
	if r.Count > 0 {
		endOffset = strconv.FormatInt((r.Offset+r.Count)-1, 10)
	}
	dataRange := fmt.Sprintf("bytes=%v-%s", r.Offset, endOffset)
	return &dataRange
}

type HTTPContentRange struct {
	Offset int64
	Count  int64
	Total  int64
}

func (r HTTPContentRange) FormatHTTPContentRange() *string {
	if r.Offset == 0 && r.Count == 0 {
		return nil // No specified range
	}
	endOffset := "" // if count == CountToEnd (0)
	if r.Count > 0 {
		endOffset = strconv.FormatInt((r.Offset+r.Count)-1, 10)
	}
	dataRange := fmt.Sprintf("bytes %v-%s/%s", r.Offset, endOffset, strconv.FormatInt(r.Total, 10))
	return &dataRange
}

type GetObjectRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// If the ETag specified in the request matches the ETag value of the object,
	// the object and 200 OK are returned. Otherwise, 412 Precondition Failed is returned.
	IfMatch *string `input:"header,If-Match"`

	// If the ETag specified in the request does not match the ETag value of the object,
	// the object and 200 OK are returned. Otherwise, 304 Not Modified is returned.
	IfNoneMatch *string `input:"header,If-None-Match"`

	// If the time specified in this header is earlier than the object modified time or is invalid,
	// the object and 200 OK are returned. Otherwise, 304 Not Modified is returned.
	// The time must be in GMT. Example: Fri, 13 Nov 2015 14:47:53 GMT.
	IfModifiedSince *string `input:"header,If-Modified-Since"`

	// If the time specified in this header is the same as or later than the object modified time,
	// the object and 200 OK are returned. Otherwise, 412 Precondition Failed is returned.
	// The time must be in GMT. Example: Fri, 13 Nov 2015 14:47:53 GMT.
	IfUnmodifiedSince *string `input:"header,If-Unmodified-Since"`

	// The content range of the object to be returned.
	// If the value of Range is valid, the total size of the object and the content range are returned.
	// For example, Content-Range: bytes 0~9/44 indicates that the total size of the object is 44 bytes,
	// and the range of data returned is the first 10 bytes.
	// However, if the value of Range is invalid, the entire object is returned,
	// and the response does not include the Content-Range parameter.
	Range *string `input:"header,Range"`

	// Specify standard behaviors to download data by range
	// If the value is "standard", the download behavior is modified when the specified range is not within the valid range.
	// For an object whose size is 1,000 bytes:
	// 1) If you set Range: bytes to 500-2000, the value at the end of the range is invalid.
	// In this case, OSS returns HTTP status code 206 and the data that is within the range of byte 500 to byte 999.
	// 2) If you set Range: bytes to 1000-2000, the value at the start of the range is invalid.
	// In this case, OSS returns HTTP status code 416 and the InvalidRange error code.
	RangeBehavior *string `input:"header,x-oss-range-behavior"`

	// The cache-control header to be returned in the response.
	ResponseCacheControl *string `input:"query,response-cache-control"`

	// The content-disposition header to be returned in the response.
	ResponseContentDisposition *string `input:"query,response-content-disposition"`

	// The content-encoding header to be returned in the response.
	ResponseContentEncoding *string `input:"query,response-content-encoding"`

	// The content-language header to be returned in the response.
	ResponseContentLanguage *string `input:"query,response-content-language"`

	// The content-type header to be returned in the response.
	ResponseContentType *string `input:"query,response-content-type"`

	// The expires header to be returned in the response.
	ResponseExpires *string `input:"query,response-expires"`

	// VersionId used to reference a specific version of the object.
	VersionId *string `input:"query,versionId"`

	// Specify the speed limit value. The speed limit value ranges from 245760 to 838860800, with a unit of bit/s.
	TrafficLimit int64 `input:"header,x-oss-traffic-limit"`

	// Progress callback function
	ProgressFn ProgressFunc

	// Image processing parameters
	Process *string `input:"query,x-oss-process"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type GetObjectResult struct {
	// Size of the body in bytes. -1 indicates that the Content-Length dose not exist.
	ContentLength int64 `output:"header,Content-Length"`

	// The portion of the object returned in the response.
	ContentRange *string `output:"header,Content-Range"`

	// A standard MIME type describing the format of the object data.
	ContentType *string `output:"header,Content-Type"`

	// The entity tag (ETag). An ETag is created when an object is created to identify the content of the object.
	ETag *string `output:"header,ETag"`

	// The time when the returned objects were last modified.
	LastModified *time.Time `output:"header,Last-Modified,time"`

	// The storage class of the object.
	StorageClass *string `output:"header,x-oss-storage-class"`

	// Content-Md5 for the uploaded object.
	ContentMD5 *string `output:"header,Content-MD5"`

	// A map of metadata to store with the object.
	Metadata map[string]string `output:"header,x-oss-meta-,usermeta"`

	// If the requested object is encrypted by using a server-side encryption algorithm based on entropy encoding,
	// OSS automatically decrypts the object and returns the decrypted object after OSS receives the GetObject request.
	// The x-oss-server-side-encryption header is included in the response to indicate
	// the encryption algorithm used to encrypt the object on the server.
	ServerSideEncryption *string `output:"header,x-oss-server-side-encryption"`

	// The server side data encryption algorithm.
	ServerSideDataEncryption *string `output:"header,x-oss-server-side-data-encryption"`

	// Deprecated: Please use ServerSideEncryptionKeyId
	SSEKMSKeyId *string `output:"header,x-oss-server-side-encryption-key-id"`

	// The ID of the customer master key (CMK) that is managed by Key Management Service (KMS).
	// This header is valid only when the x-oss-server-side-encryption header is set to KMS.
	ServerSideEncryptionKeyId *string `output:"header,x-oss-server-side-encryption-key-id"`

	// The type of the object.
	ObjectType *string `output:"header,x-oss-object-type"`

	// The position for the next append operation.
	// If the type of the object is Appendable, this header is included in the response.
	NextAppendPosition *string `output:"header,x-oss-next-append-position"`

	// The 64-bit CRC value of the object.
	// This value is calculated based on the ECMA-182 standard.
	HashCRC64 *string `output:"header,x-oss-hash-crc64ecma"`

	// The lifecycle information about the object.
	// If lifecycle rules are configured for the object, this header is included in the response.
	// This header contains the following parameters: expiry-date that indicates the expiration time of the object,
	// and rule-id that indicates the ID of the matched lifecycle rule.
	Expiration *string `output:"header,x-oss-expiration"`

	// The status of the object when you restore an object.
	// If the storage class of the bucket is Archive and a RestoreObject request is submitted,
	Restore *string `output:"header,x-oss-restore"`

	// The result of an event notification that is triggered for the object.
	ProcessStatus *string `output:"header,x-oss-process-status"`

	// The number of tags added to the object.
	// This header is included in the response only when you have read permissions on tags.
	TaggingCount int32 `output:"header,x-oss-tagging-count"`

	// Specifies whether the object retrieved was (true) or was not (false) a Delete  Marker.
	DeleteMarker bool `output:"header,x-oss-delete-marker"`

	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	// Object data.
	Body io.ReadCloser

	ResultCommon
}

func (c *Client) GetObject(ctx context.Context, request *GetObjectRequest, optFns ...func(*Options)) (*GetObjectResult, error) {
	var err error
	if request == nil {
		request = &GetObjectRequest{}
	}
	input := &OperationInput{
		OpName: "GetObject",
		Method: "GET",
		Bucket: request.Bucket,
		Key:    request.Key,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetObjectResult{
		Body: output.Body,
	}
	if err = c.unmarshalOutput(result, output, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type CopyObjectRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The name of the source bucket.
	SourceBucket *string `input:"nop,bucket"`

	// The path of the source object.
	SourceKey *string `input:"nop,key,required"`

	// The version ID of the source object.
	SourceVersionId *string `input:"nop,versionId"`

	// Specifies whether the CopyObject operation overwrites objects with the same name. The x-oss-forbid-overwrite request header does not take effect when versioning is enabled or suspended for the destination bucket. In this case, the CopyObject operation overwrites the existing object that has the same name as the destination object.
	// If you do not specify the x-oss-forbid-overwrite header or set the header to false, an existing object that has the same name as the object that you want to copy is overwritten.
	// If you set the x-oss-forbid-overwrite header to true, an existing object that has the same name as the object that you want to copy is not overwritten.
	ForbidOverwrite *string `input:"header,x-oss-forbid-overwrite"`

	// If the ETag specified in the request matches the ETag value of the object,
	// the object and 200 OK are returned. Otherwise, 412 Precondition Failed is returned.
	IfMatch *string `input:"header,x-oss-copy-source-if-match"`

	// If the ETag specified in the request does not match the ETag value of the object,
	// the object and 200 OK are returned. Otherwise, 304 Not Modified is returned.
	IfNoneMatch *string `input:"header,x-oss-copy-source-if-none-match"`

	// If the time specified in this header is earlier than the object modified time or is invalid,
	// the object and 200 OK are returned. Otherwise, 304 Not Modified is returned.
	// The time must be in GMT. Example: Fri, 13 Nov 2015 14:47:53 GMT.
	IfModifiedSince *string `input:"header,x-oss-copy-source-if-modified-since"`

	// If the time specified in this header is the same as or later than the object modified time,
	// the object and 200 OK are returned. Otherwise, 412 Precondition Failed is returned.
	// The time must be in GMT. Example: Fri, 13 Nov 2015 14:47:53 GMT.
	IfUnmodifiedSince *string `input:"header,x-oss-copy-source-if-unmodified-since"`

	// The method that is used to configure the metadata of the destination object.
	// COPY (default): The metadata of the source object is copied to the destination object.
	// The configurations of the x-oss-server-side-encryption
	// header of the source object are not copied to the destination object.
	// The x-oss-server-side-encryption header in the CopyObject request specifies
	// the method used to encrypt the destination object.
	// REPLACE: The metadata specified in the request is used as the metadata of the destination object.
	MetadataDirective *string `input:"header,x-oss-metadata-directive"`

	// The entropy coding-based encryption algorithm that OSS uses to encrypt an object when you create the object.
	// Valid values: AES256, KMS, SM4
	ServerSideEncryption *string `input:"header,x-oss-server-side-encryption"`

	// The server side data encryption algorithm. Invalid value: SM4
	ServerSideDataEncryption *string `input:"header,x-oss-server-side-data-encryption"`

	// Deprecated: Please use ServerSideEncryptionKeyId
	SSEKMSKeyId *string `input:"header,x-oss-server-side-encryption-key-id"`

	// The ID of the customer master key (CMK) that is managed by Key Management Service (KMS).
	// This header is valid only when the x-oss-server-side-encryption header is set to KMS.
	ServerSideEncryptionKeyId *string `input:"header,x-oss-server-side-encryption-key-id"`

	// The access control list (ACL) of the object.
	Acl ObjectACLType `input:"header,x-oss-object-acl"`

	// The storage class of the object.
	StorageClass StorageClassType `input:"header,x-oss-storage-class"`

	// The caching behavior of the web page when the object is downloaded.
	CacheControl *string `input:"header,Cache-Control"`

	// The method that is used to access the object.
	ContentDisposition *string `input:"header,Content-Disposition"`

	// The method that is used to encode the object.
	ContentEncoding *string `input:"header,Content-Encoding"`

	// A standard MIME type describing the format of the contents.
	ContentType *string `input:"header,Content-Type"`

	// The expiration time of the cache in UTC.
	Expires *string `input:"header,Expires"`

	// The metadata of the object that you want to upload.
	Metadata map[string]string `input:"header,x-oss-meta-,usermeta"`

	// The tags that are specified for the object by using a key-value pair.
	// You can specify multiple tags for an object. Example: TagA=A&TagB=B.
	Tagging *string `input:"header,x-oss-tagging"`

	// The method that is used to configure tags for the destination object.
	// Valid values: Copy (default): The tags of the source object are copied to the destination object.
	// Replace: The tags specified in the request are configured for the destination object.
	TaggingDirective *string `input:"header,x-oss-tagging-directive"`

	// Specify the speed limit value. The speed limit value ranges from  245760 to 838860800, with a unit of bit/s.
	TrafficLimit int64 `input:"header,x-oss-traffic-limit"`

	// Progress callback function, it works in Copier.Copy only.
	ProgressFn ProgressFunc

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type CopyObjectResult struct {
	// The 64-bit CRC value of the object.
	// This value is calculated based on the ECMA-182 standard.
	HashCRC64 *string `output:"header,x-oss-hash-crc64ecma"`

	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	// The version ID of the source object.
	SourceVersionId *string `output:"header,x-oss-copy-source-version-id"`

	// If the requested object is encrypted by using a server-side encryption algorithm based on entropy encoding,
	// OSS automatically decrypts the object and returns the decrypted object after OSS receives the GetObject request.
	// The x-oss-server-side-encryption header is included in the response to indicate
	// the encryption algorithm used to encrypt the object on the server.
	ServerSideEncryption *string `output:"header,x-oss-server-side-encryption"`

	// The server side data encryption algorithm.
	ServerSideDataEncryption *string `output:"header,x-oss-server-side-data-encryption"`

	// Deprecated: Please use ServerSideEncryptionKeyId
	SSEKMSKeyId *string `output:"header,x-oss-server-side-encryption-key-id"`

	// The ID of the customer master key (CMK) that is managed by Key Management Service (KMS).
	// This header is valid only when the x-oss-server-side-encryption header is set to KMS.
	ServerSideEncryptionKeyId *string `output:"header,x-oss-server-side-encryption-key-id"`

	// The time when the returned objects were last modified.
	LastModified *time.Time `xml:"LastModified"`

	// The entity tag (ETag). An ETag is created when an object is created to identify the content of the object.
	ETag *string `xml:"ETag"`

	ResultCommon
}

// CopyObject Copies objects within a bucket or between buckets in the same region
func (c *Client) CopyObject(ctx context.Context, request *CopyObjectRequest, optFns ...func(*Options)) (*CopyObjectResult, error) {
	var err error
	if request == nil {
		request = &CopyObjectRequest{}
	}

	input := &OperationInput{
		OpName: "CopyObject",
		Method: "PUT",
		Bucket: request.Bucket,
		Key:    request.Key,
		Headers: map[string]string{
			"x-oss-copy-source": encodeSourceObject(request),
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &CopyObjectResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type AppendObjectRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The position from which the AppendObject operation starts.
	// Each time an AppendObject operation succeeds, the x-oss-next-append-position header is included in
	// the response to specify the position from which the next AppendObject operation starts.
	Position *int64 `input:"query,position,required"`

	// The caching behavior of the web page when the object is downloaded.
	CacheControl *string `input:"header,Cache-Control"`

	// The method that is used to access the object.
	ContentDisposition *string `input:"header,Content-Disposition"`

	// The method that is used to encode the object.
	ContentEncoding *string `input:"header,Content-Encoding"`

	// The size of the data in the HTTP message body. Unit: bytes.
	ContentLength *int64 `input:"header,Content-Length"`

	// The MD5 hash of the object that you want to upload.
	ContentMD5 *string `input:"header,Content-MD5"`

	// The expiration time of the cache in UTC.
	Expires *string `input:"header,Expires"`

	// A standard MIME type describing the format of the contents.
	ContentType *string `input:"header,Content-Type"`

	// Specifies whether the AppendObject operation overwrites objects with the same name. The x-oss-forbid-overwrite request header does not take effect when versioning is enabled or suspended for the destination bucket. In this case, the AppendObject operation overwrites the existing object that has the same name as the destination object.
	// If you do not specify the x-oss-forbid-overwrite header or set the header to false, an existing object that has the same name as the object that you want to copy is overwritten.
	// If you set the x-oss-forbid-overwrite header to true, an existing object that has the same name as the object that you want to copy is not overwritten.
	ForbidOverwrite *string `input:"header,x-oss-forbid-overwrite"`

	// The method used to encrypt objects on the specified OSS server.Valid values: AES256, KMS, SM4
	// AES256: Keys managed by OSS are used for encryption and decryption (SSE-OSS).
	// KMS: Keys managed by Key Management Service (KMS) are used for encryption and decryption.
	// SM4: The SM4 block cipher algorithm is used for encryption and decryption.
	ServerSideEncryption *string `input:"header,x-oss-server-side-encryption"`

	// Specify the encryption algorithm for the object. Valid values: SM4.
	// If this option is not specified, it indicates that the Object uses AES256 encryption algorithm.
	// This option is only valid when x-oss-ser-side-encryption is KMS.
	ServerSideDataEncryption *string `input:"header,x-oss-server-side-data-encryption"`

	// Deprecated: Please use ServerSideEncryptionKeyId
	SSEKMSKeyId *string `input:"header,x-oss-server-side-encryption-key-id"`

	// The ID of the customer master key (CMK) that is managed by Key Management Service (KMS).
	// This header is valid only when the x-oss-server-side-encryption header is set to KMS.
	ServerSideEncryptionKeyId *string `input:"header,x-oss-server-side-encryption-key-id"`

	// The access control list (ACL) of the object.
	Acl ObjectACLType `input:"header,x-oss-object-acl"`

	// The storage class of the object.
	StorageClass StorageClassType `input:"header,x-oss-storage-class"`

	// The metadata of the object that you want to upload.
	Metadata map[string]string `input:"header,x-oss-meta-,usermeta"`

	// The tags that are specified for the object by using a key-value pair.
	// You can specify multiple tags for an object. Example: TagA=A&TagB=B.
	Tagging *string `input:"header,x-oss-tagging"`

	// Specify the speed limit value. The speed limit value ranges from  245760 to 838860800, with a unit of bit/s.
	TrafficLimit int64 `input:"header,x-oss-traffic-limit"`

	// Object data.
	Body io.Reader `input:"body,nop"`

	// Specify the initial value of CRC64. If not set, the crc check is ignored.
	InitHashCRC64 *string

	// Progress callback function
	ProgressFn ProgressFunc

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type AppendObjectResult struct {
	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	// The 64-bit CRC value of the object.
	// This value is calculated based on the ECMA-182 standard.
	HashCRC64 *string `output:"header,x-oss-hash-crc64ecma"`

	// The position that must be provided in the next request, which is the current length of the object.
	NextPosition int64 `output:"header,x-oss-next-append-position"`

	// The encryption method on the server side when an object is created.
	// Valid values: AES256, KMS, SM4
	ServerSideEncryption *string `output:"header,x-oss-server-side-encryption"`

	// The server side data encryption algorithm.
	ServerSideDataEncryption *string `output:"header,x-oss-server-side-data-encryption"`

	// Deprecated: Please use ServerSideEncryptionKeyId
	SSEKMSKeyId *string `output:"header,x-oss-server-side-encryption-key-id"`

	// The ID of the customer master key (CMK) that is managed by Key Management Service (KMS).
	// This header is valid only when the x-oss-server-side-encryption header is set to KMS.
	ServerSideEncryptionKeyId *string `output:"header,x-oss-server-side-encryption-key-id"`

	ResultCommon
}

// AppendObject Uploads an object by appending the object to an existing object.
// Objects created by using the AppendObject operation are appendable objects.
func (c *Client) AppendObject(ctx context.Context, request *AppendObjectRequest, optFns ...func(*Options)) (*AppendObjectResult, error) {
	var err error
	if request == nil {
		request = &AppendObjectRequest{}
	}
	input := &OperationInput{
		OpName:     "AppendObject",
		Method:     "POST",
		Parameters: map[string]string{"append": ""},
		Bucket:     request.Bucket,
		Key:        request.Key,
	}

	marshalFns := []func(any, *OperationInput) error{
		addProgress,
		c.updateContentType,
	}

	unmarshalFns := []func(any, *OperationOutput) error{
		discardBody,
		unmarshalHeader,
	}

	// AppendObject is not idempotent, and cannot be retried
	if c.hasFeature(FeatureEnableCRC64CheckUpload) && request.InitHashCRC64 != nil {
		var init uint64
		init, err = strconv.ParseUint(ToString(request.InitHashCRC64), 10, 64)
		if err != nil {
			return nil, NewErrParamInvalid("request.InitHashCRC64")
		}
		var w io.Writer = NewCRC64(init)
		input.OpMetadata.Add(OpMetaKeyRequestBodyTracker, w)
		unmarshalFns = append(unmarshalFns, func(result any, output *OperationOutput) error {
			return checkResponseHeaderCRC64(fmt.Sprint(w.(hash.Hash64).Sum64()), output.Headers)
		})
	}

	if err = c.marshalInput(request, input, marshalFns...); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &AppendObjectResult{}
	if err = c.unmarshalOutput(result, output, unmarshalFns...); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type SealAppendObjectRequest struct {
	// Bucket name
	Bucket *string `input:"host,bucket,required"`

	// Name of the Appendable Object
	Key *string `input:"path,key,required"`

	// Used to specify the expected length of the file when the user wants to seal it.
	Position *int64 `input:"query,position,required"`

	RequestCommon
}

type SealAppendObjectResult struct {
	// The time in GMT format when the SealAppendObject operation was first performed on the object.
	// This timestamp does not change even if the operation is performed again.
	SealedTime *string `output:"header,x-oss-sealed-time"`

	ResultCommon
}

// SealAppendObject This operation stops writing to the Appendable Object, after which the user can configure lifecycle rules to change the storage class of the corresponding Appendable Object to Cold Archive or Deep Cold Archive.
func (c *Client) SealAppendObject(ctx context.Context, request *SealAppendObjectRequest, optFns ...func(*Options)) (*SealAppendObjectResult, error) {
	var err error
	if request == nil {
		request = &SealAppendObjectRequest{}
	}
	input := &OperationInput{
		OpName: "SealAppendObject",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"seal": "",
		},
		Bucket: request.Bucket,
		Key:    request.Key,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"seal"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &SealAppendObjectResult{}

	if err = c.unmarshalOutput(result, output, discardBody, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteObjectRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The version ID of the source object.
	VersionId *string `input:"query,versionId"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type DeleteObjectResult struct {
	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	// Specifies whether the object retrieved was (true) or was not (false) a Delete  Marker.
	DeleteMarker bool `output:"header,x-oss-delete-marker"`

	ResultCommon
}

// DeleteObject Deletes an object.
func (c *Client) DeleteObject(ctx context.Context, request *DeleteObjectRequest, optFns ...func(*Options)) (*DeleteObjectResult, error) {
	var err error
	if request == nil {
		request = &DeleteObjectRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteObject",
		Method: "DELETE",
		Bucket: request.Bucket,
		Key:    request.Key,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &DeleteObjectResult{}
	if err = c.unmarshalOutput(result, output, discardBody, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteObject struct {
	// The name of the object that you want to delete.
	Key *string `xml:"Key"`

	// The version ID of the object that you want to delete.
	VersionId *string `xml:"VersionId"`
}

type ObjectIdentifier = DeleteObject

type Delete struct {
	// The container that stores information about you want to delete objects.
	Objects []ObjectIdentifier

	// Specifies whether to enable the Quiet return mode.
	// The DeleteMultipleObjects operation provides the following return modes: Valid value: true,false
	Quiet bool
}

type DeleteMultipleObjectsRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The encoding type of the object names in the response. Valid value: url
	EncodingType *string `input:"query,encoding-type"`

	// The size of the data in the HTTP message body. Unit: bytes.
	ContentLength int64 `input:"header,Content-Length"`

	// The container that stores information about you want to delete objects.
	// Objects will be deprecated and removed in the future. Use Delete.Objects instead.
	// If both exist simultaneously, the value of Delete will take precedence.
	Objects []DeleteObject

	// Specifies whether to enable the Quiet return mode.
	// The DeleteMultipleObjects operation provides the following return modes: Valid value: true,false
	// Quiet will be deprecated and removed in the future. Use Delete.Quiet instead.
	// If both exist simultaneously, the value of Delete will take precedence.
	Quiet bool

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	// The container that stores information about you want to delete objects.
	Delete *Delete

	RequestCommon
}

type DeleteMultipleObjectsResult struct {
	// The container that stores information about the deleted objects.
	DeletedObjects []DeletedInfo `xml:"Deleted"`

	// The encoding type of the name of the deleted object in the response.
	// If encoding-type is specified in the request, the object name is encoded in the returned result.
	EncodingType *string `xml:"EncodingType"`

	ResultCommon
}

type DeletedInfo struct {
	// The name of the deleted object.
	Key *string `xml:"Key"`

	// The version ID of the object that you deleted.
	VersionId *string `xml:"VersionId"`

	// Indicates whether the deleted version is a delete marker.
	DeleteMarker bool `xml:"DeleteMarker"`

	// The version ID of the delete marker.
	DeleteMarkerVersionId *string `xml:"DeleteMarkerVersionId"`
}

// DeleteMultipleObjects Deletes multiple objects from a bucket.
func (c *Client) DeleteMultipleObjects(ctx context.Context, request *DeleteMultipleObjectsRequest, optFns ...func(*Options)) (*DeleteMultipleObjectsResult, error) {
	var err error
	if request == nil {
		request = &DeleteMultipleObjectsRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteMultipleObjects",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"delete":        "",
			"encoding-type": "url",
		},
		Bucket: request.Bucket,
	}
	if err = c.marshalInput(request, input, marshalDeleteObjects, updateContentMd5, enableNonStream); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &DeleteMultipleObjectsResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml, unmarshalHeader, unmarshalEncodeType); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type HeadObjectRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The version ID of the source object.
	VersionId *string `input:"query,versionId"`

	// If the ETag specified in the request matches the ETag value of the object,
	// the object and 200 OK are returned. Otherwise, 412 Precondition Failed is returned.
	IfMatch *string `input:"header,If-Match"`

	// If the ETag specified in the request does not match the ETag value of the object,
	// the object and 200 OK are returned. Otherwise, 304 Not Modified is returned.
	IfNoneMatch *string `input:"header,If-None-Match"`

	// If the time specified in this header is earlier than the object modified time or is invalid,
	// the object and 200 OK are returned. Otherwise, 304 Not Modified is returned.
	// The time must be in GMT. Example: Fri, 13 Nov 2015 14:47:53 GMT.
	IfModifiedSince *string `input:"header,If-Modified-Since"`

	// If the time specified in this header is the same as or later than the object modified time,
	// the object and 200 OK are returned. Otherwise, 412 Precondition Failed is returned.
	// The time must be in GMT. Example: Fri, 13 Nov 2015 14:47:53 GMT.
	IfUnmodifiedSince *string `input:"header,If-Unmodified-Since"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type HeadObjectResult struct {
	// Size of the body in bytes. -1 indicates that the Content-Length dose not exist.
	ContentLength int64 `output:"header,Content-Length"`

	// A standard MIME type describing the format of the object data.
	ContentType *string `output:"header,Content-Type"`

	// The entity tag (ETag). An ETag is created when an object is created to identify the content of the object.
	ETag *string `output:"header,ETag"`

	// The time when the returned objects were last modified.
	LastModified *time.Time `output:"header,Last-Modified,time"`

	// The storage class of the object.
	StorageClass *string `output:"header,x-oss-storage-class"`

	// Content-Md5 for the uploaded object.
	ContentMD5 *string `output:"header,Content-MD5"`

	// A map of metadata to store with the object.
	Metadata map[string]string `output:"header,x-oss-meta-,usermeta"`

	// If the requested object is encrypted by using a server-side encryption algorithm based on entropy encoding,
	// OSS automatically decrypts the object and returns the decrypted object after OSS receives the GetObject request.
	// The x-oss-server-side-encryption header is included in the response to indicate
	// the encryption algorithm used to encrypt the object on the server.
	ServerSideEncryption *string `output:"header,x-oss-server-side-encryption"`

	// The server side data encryption algorithm.
	ServerSideDataEncryption *string `output:"header,x-oss-server-side-data-encryption"`

	// Deprecated: Please use ServerSideEncryptionKeyId
	SSEKMSKeyId *string `output:"header,x-oss-server-side-encryption-key-id"`

	// The ID of the customer master key (CMK) that is managed by Key Management Service (KMS).
	// This header is valid only when the x-oss-server-side-encryption header is set to KMS.
	ServerSideEncryptionKeyId *string `output:"header,x-oss-server-side-encryption-key-id"`

	// The type of the object.
	ObjectType *string `output:"header,x-oss-object-type"`

	// The position for the next append operation.
	// If the type of the object is Appendable, this header is included in the response.
	NextAppendPosition *string `output:"header,x-oss-next-append-position"`

	// The 64-bit CRC value of the object.
	// This value is calculated based on the ECMA-182 standard.
	HashCRC64 *string `output:"header,x-oss-hash-crc64ecma"`

	// The lifecycle information about the object.
	// If lifecycle rules are configured for the object, this header is included in the response.
	// This header contains the following parameters: expiry-date that indicates the expiration time of the object,
	// and rule-id that indicates the ID of the matched lifecycle rule.
	Expiration *string `output:"header,x-oss-expiration"`

	// The status of the object when you restore an object.
	// If the storage class of the bucket is Archive and a RestoreObject request is submitted,
	Restore *string `output:"header,x-oss-restore"`

	// The result of an event notification that is triggered for the object.
	ProcessStatus *string `output:"header,x-oss-process-status"`

	// The requester. This header is included in the response if the pay-by-requester mode
	// is enabled for the bucket and the requester is not the bucket owner. The value of this header is requester
	RequestCharged *string `output:"header,x-oss-request-charged"`

	// The number of tags added to the object.
	// This header is included in the response only when you have read permissions on tags.
	TaggingCount int32 `output:"header,x-oss-tagging-count"`

	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	// The origins allowed for cross-origin resource sharing (CORS).
	// If a CORS rule is configured for the bucket that stores the object and the Origin header
	// in the request meets the CORS rule, this header is included in the response.
	AllowOrigin *string `output:"header,Access-Control-Allow-Origin"`

	// The methods allowed for CORS. If a CORS rule is configured for the bucket that stores the object
	// and the Access-Control-Request-Method header in the request meets the CORS rule, this header is included in the response.
	AllowMethods *string `output:"header,Access-Control-Allow-Methods"`

	// The maximum caching period for CORS. If a CORS rule is configured for the bucket that stores
	// the object and the request meets the CORS rule, this header is included in the response.
	AllowAge *string `output:"header,Access-Control-Allow-Age"`

	// The headers allowed for CORS. If a CORS rule is configured for the bucket that stores
	// the object and the request meets the CORS rule, this header is included in the response
	AllowHeaders *string `output:"header,Access-Control-Allow-Headers"`

	// The headers that can be accessed by JavaScript applications on the client.
	// If a CORS rule is configured for the bucket that stores the object and the request meets
	// the CORS rule, this header is included in the response
	ExposeHeaders *string `output:"header,Access-Control-Expose-Headers"`

	// The caching behavior of the web page when the object is downloaded.
	CacheControl *string `output:"header,Cache-Control"`

	// The method that is used to access the object.
	ContentDisposition *string `output:"header,Content-Disposition"`

	// The method that is used to encode the object.
	ContentEncoding *string `output:"header,Content-Encoding"`

	// The expiration time of the cache in UTC.
	Expires *string `output:"header,Expires"`

	// The time when the storage class of the object is converted to Cold Archive or Deep Cold Archive based on lifecycle rules.
	TransitionTime *time.Time `output:"header,x-oss-transition-time,time"`

	ResultCommon
}

// HeadObject Queries information about all objects in a bucket.
func (c *Client) HeadObject(ctx context.Context, request *HeadObjectRequest, optFns ...func(*Options)) (*HeadObjectResult, error) {
	var err error
	if request == nil {
		request = &HeadObjectRequest{}
	}
	input := &OperationInput{
		OpName: "HeadObject",
		Method: "HEAD",
		Bucket: request.Bucket,
		Key:    request.Key,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &HeadObjectResult{}
	if err = c.unmarshalOutput(result, output, discardBody, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetObjectMetaRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The version ID of the source object.
	VersionId *string `input:"query,versionId"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type GetObjectMetaResult struct {
	// Size of the body in bytes. -1 indicates that the Content-Length dose not exist.
	ContentLength int64 `output:"header,Content-Length"`

	// The entity tag (ETag). An ETag is created when an object is created to identify the content of the object.
	ETag *string `output:"header,ETag"`

	// The time when the returned objects were last modified.
	LastModified *time.Time `output:"header,Last-Modified,time"`

	// The time when the object was last accessed.
	LastAccessTime *time.Time `output:"header,x-oss-last-access-time,time"`

	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	// The 64-bit CRC value of the object.
	// This value is calculated based on the ECMA-182 standard.
	HashCRC64 *string `output:"header,x-oss-hash-crc64ecma"`

	// The time when the storage class of the object is converted to Cold Archive or Deep Cold Archive based on lifecycle rules.
	TransitionTime *time.Time `output:"header,x-oss-transition-time,time"`

	ResultCommon
}

// GetObjectMeta Queries the metadata of an object, including ETag, Size, and LastModified.
// The content of the object is not returned.
func (c *Client) GetObjectMeta(ctx context.Context, request *GetObjectMetaRequest, optFns ...func(*Options)) (*GetObjectMetaResult, error) {
	var err error
	if request == nil {
		request = &GetObjectMetaRequest{}
	}
	input := &OperationInput{
		OpName: "GetObjectMeta",
		Method: "HEAD",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"objectMeta": "",
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &GetObjectMetaResult{}
	if err = c.unmarshalOutput(result, output, discardBody, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type RestoreObjectRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The version ID of the source object.
	VersionId *string `input:"query,versionId"`

	// The container that stores information about the RestoreObject request.
	RestoreRequest *RestoreRequest //`input:"body,RestoreRequest,xml"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type RestoreRequest struct {
	// The duration within which the restored object remains in the restored state.
	Days int32 `xml:"Days"`

	// The restoration priority of Cold Archive or Deep Cold Archive objects. Valid values:Expedited,Standard,Bulk
	// Tier will be deprecated and removed in the future. Use JobParameters.Tier instead.
	// If both exist simultaneously, the value of JobParameters will take precedence.
	Tier *string //`xml:"JobParameters>Tier"`

	// The container that stores the restoration priority coniguration.
	// This configuration takes effect only when the request is sent to restore Cold Archive objects.
	// If you do not specify the JobParameters parameter, the default restoration priority Standard is used.
	JobParameters *JobParameters `xml:"JobParameters"`
}

type JobParameters struct {
	// The restoration priority of Cold Archive or Deep Cold Archive objects. Valid values:Expedited,Standard,Bulk
	Tier *string `xml:"Tier"`
}

type RestoreObjectResult struct {
	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	// The restoration priority.
	// This header is displayed only for the Cold Archive or Deep Cold Archive object in the restored state.
	RestorePriority *string `output:"header,x-oss-object-restore-priority"`

	ResultCommon
}

// RestoreObject Restores Archive, Cold Archive, or Deep Cold Archive objects.
// You are charged based on tier(restoration priority) when you restore object. For more information, see OSS document.
func (c *Client) RestoreObject(ctx context.Context, request *RestoreObjectRequest, optFns ...func(*Options)) (*RestoreObjectResult, error) {
	var err error
	if request == nil {
		request = &RestoreObjectRequest{}
	}
	input := &OperationInput{
		OpName: "RestoreObject",
		Method: "POST",
		Bucket: request.Bucket,
		Key:    request.Key,
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"restore": "",
		},
	}
	if err = c.marshalInput(request, input, marshalRestoreObject, updateContentMd5); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &RestoreObjectResult{}
	if err = c.unmarshalOutput(result, output, discardBody, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutObjectAclRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The access control list (ACL) of the object.
	Acl ObjectACLType `input:"header,x-oss-object-acl,required"`

	// The version ID of the source object.
	VersionId *string `input:"query,versionId"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type PutObjectAclResult struct {
	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	ResultCommon
}

// PutObjectAcl You can call this operation to modify the access control list (ACL) of an object.
func (c *Client) PutObjectAcl(ctx context.Context, request *PutObjectAclRequest, optFns ...func(*Options)) (*PutObjectAclResult, error) {
	var err error
	if request == nil {
		request = &PutObjectAclRequest{}
	}
	input := &OperationInput{
		OpName: "PutObjectAcl",
		Method: "PUT",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"acl": "",
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutObjectAclResult{}
	if err = c.unmarshalOutput(result, output, discardBody, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetObjectAclRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The version ID of the source object.
	VersionId *string `input:"query,versionId"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type GetObjectAclResult struct {
	// The ACL of the object. Default value: default.
	ACL *string `xml:"AccessControlList>Grant"`

	// The container that stores information about the object owner.
	Owner *Owner `xml:"Owner"`

	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	ResultCommon
}

// GetObjectAcl Queries the access control list (ACL) of an object in a bucket.
func (c *Client) GetObjectAcl(ctx context.Context, request *GetObjectAclRequest, optFns ...func(*Options)) (*GetObjectAclResult, error) {
	var err error
	if request == nil {
		request = &GetObjectAclRequest{}
	}
	input := &OperationInput{
		OpName: "GetObjectAcl",
		Method: "GET",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"acl": "",
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetObjectAclResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type InitiateMultipartUploadRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The encoding type of the object names in the response. Valid value: url
	EncodingType *string `input:"query,encoding-type"`

	// The caching behavior of the web page when the object is downloaded.
	CacheControl *string `input:"header,Cache-Control"`

	// The method that is used to access the object.
	ContentDisposition *string `input:"header,Content-Disposition"`

	// The method that is used to encode the object.
	ContentEncoding *string `input:"header,Content-Encoding"`

	// A standard MIME type describing the format of the contents.
	ContentType *string `input:"header,Content-Type"`

	// The expiration time of the cache in UTC.
	Expires *string `input:"header,Expires"`

	// Specifies whether the InitiateMultipartUpload operation overwrites the existing object that has the same name as the object that you want to upload. If versioning is enabled or suspended for the bucket to which you want to upload the object, the x-oss-forbid-overwrite header does not take effect. As a result, the object that is uploaded by calling the InitiateMultipartUpload operation overwrites the existing object that has the same name.
	// If you do not specify the x-oss-forbid-overwrite header or you set the x-oss-forbid-overwrite header to false, the operation overwrites an existing object that has the same name.
	// If you set the x-oss-forbid-overwrite header to true, an existing object that has the same name cannot be overwritten.
	ForbidOverwrite *string `input:"header,x-oss-forbid-overwrite"`

	// The server-side encryption method that is used to encrypt each part of the object that you want to upload.Valid values: AES256, KMS, SM4.
	// If you specify this header in the request, this header is included in the response.
	// OSS uses the method specified by this header to encrypt each uploaded part.
	// When you download the object, the x-oss-server-side-encryption header is included in the response and the header value is set to the method that is used to encrypt the object.
	ServerSideEncryption *string `input:"header,x-oss-server-side-encryption"`

	// The server side data encryption algorithm. Valid values: SM4
	// If this option is not specified, it indicates that the Object uses AES256 encryption algorithm.
	// This option is only valid when x-oss-ser-side-encryption is KMS.
	ServerSideDataEncryption *string `input:"header,x-oss-server-side-data-encryption"`

	// Deprecated: Please use ServerSideEncryptionKeyId
	SSEKMSKeyId *string `input:"header,x-oss-server-side-encryption-key-id"`

	// The ID of the customer master key (CMK) that is managed by Key Management Service (KMS).
	// This header is valid only when the x-oss-server-side-encryption header is set to KMS.
	ServerSideEncryptionKeyId *string `input:"header,x-oss-server-side-encryption-key-id"`

	// The storage class of the object.
	StorageClass StorageClassType `input:"header,x-oss-storage-class"`

	// The metadata of the object that you want to upload.
	Metadata map[string]string `input:"header,x-oss-meta-,usermeta"`

	// The tags that are specified for the object by using a key-value pair.
	// You can specify multiple tags for an object. Example: TagA=A&TagB=B.
	Tagging *string `input:"header,x-oss-tagging"`

	// The total size when using client side encryption, only valid in EncryptionClient
	CSEDataSize *int64

	// The part size when using client side encryption, only valid in EncryptionClient
	// CSEPartSize must aligned to the secret iv length
	CSEPartSize *int64

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	// To disable the feature that Content-Type is automatically added based on the object name if not specified.
	DisableAutoDetectMimeType bool

	RequestCommon
}

type InitiateMultipartUploadResult struct {
	// The name of the bucket to which the object is uploaded by the multipart upload task.
	Bucket *string `xml:"Bucket"`

	// The name of the object that is uploaded by the multipart upload task.
	Key *string `xml:"Key"`

	// The upload ID that uniquely identifies the multipart upload task.
	UploadId *string `xml:"UploadId"`

	// The encoding type of the object names in the response. Valid value: url
	EncodingType *string `xml:"EncodingType"`

	// The encryption context for multipart upload when using client side encryption, only valid in EncryptionClient
	CSEMultiPartContext *EncryptionMultiPartContext

	ResultCommon
}

// InitiateMultipartUpload Initiates a multipart upload task before you can upload data in parts to Object Storage Service (OSS).
func (c *Client) InitiateMultipartUpload(ctx context.Context, request *InitiateMultipartUploadRequest, optFns ...func(*Options)) (*InitiateMultipartUploadResult, error) {
	var err error
	if request == nil {
		request = &InitiateMultipartUploadRequest{}
	}
	input := &OperationInput{
		OpName: "InitiateMultipartUpload",
		Method: "POST",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"uploads":       "",
			"encoding-type": "url",
		},
	}

	marshalFns := []func(any, *OperationInput) error{
		updateContentMd5,
	}
	if !request.DisableAutoDetectMimeType {
		marshalFns = append(marshalFns, c.updateContentType)
	}
	if err = c.marshalInput(request, input, marshalFns...); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &InitiateMultipartUploadResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml, unmarshalEncodeType); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type UploadPartRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,uploadId,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Each uploaded part is identified by a number.
	// Value: 1-10000
	//The size limit of a single part is between 100 KB and 5 GB.
	PartNumber int32 `input:"query,partNumber,required"`

	// The ID of the multipart upload task.
	UploadId *string `input:"query,uploadId,required"`

	// The MD5 hash of the object that you want to upload.
	ContentMD5 *string `input:"header,Content-MD5"`

	// Specify the speed limit value. The speed limit value ranges from  245760 to 838860800, with a unit of bit/s.
	TrafficLimit int64 `input:"header,x-oss-traffic-limit"`

	// Object data.
	Body io.Reader `input:"body,nop"`

	// Progress callback function
	ProgressFn ProgressFunc

	// The size of the data in the HTTP message body. Unit: bytes.
	ContentLength *int64 `input:"header,Content-Length"`

	// The encryption context for multipart upload when using client side encryption, only valid in EncryptionClient
	CSEMultiPartContext *EncryptionMultiPartContext

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type UploadPartResult struct {
	// Entity tag for the uploaded part.
	ETag *string `output:"header,ETag"`

	// The MD5 hash of the part that you want to upload.
	ContentMD5 *string `output:"header,Content-MD5"`

	// The 64-bit CRC value of the part.
	// This value is calculated based on the ECMA-182 standard.
	HashCRC64 *string `output:"header,x-oss-hash-crc64ecma"`

	ResultCommon
}

// UploadPart Call the UploadPart interface to upload data in blocks (parts) based on the specified Object name and uploadId.
func (c *Client) UploadPart(ctx context.Context, request *UploadPartRequest, optFns ...func(*Options)) (*UploadPartResult, error) {
	var err error
	if request == nil {
		request = &UploadPartRequest{}
	}
	input := &OperationInput{
		OpName: "UploadPart",
		Method: "PUT",
		Bucket: request.Bucket,
		Key:    request.Key,
	}

	marshalFns := []func(any, *OperationInput) error{
		addProgress,
		c.addCrcCheck,
	}

	if err = c.marshalInput(request, input, marshalFns...); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &UploadPartResult{}
	if err = c.unmarshalOutput(result, output, discardBody, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type UploadPartCopyRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,uploadId,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Each uploaded part is identified by a number.
	// Value: 1-10000
	//The size limit of a single part is between 100 KB and 5 GB.
	PartNumber int32 `input:"query,partNumber,required"`

	// The ID of the multipart upload task.
	UploadId *string `input:"query,uploadId,required"`

	// The name of the source bucket.
	SourceBucket *string `input:"nop,bucket"`

	// The path of the source object.
	SourceKey *string `input:"nop,key,required"`

	// The version ID of the source object.
	SourceVersionId *string `input:"nop,versionId"`

	// The range of bytes to copy data from the source object.
	Range *string `input:"header,x-oss-copy-source-range"`

	// The copy operation condition. If the ETag value of the source object is
	// the same as the ETag value provided by the user, OSS copies data. Otherwise,
	// OSS returns 412 Precondition Failed.
	IfMatch *string `input:"header,x-oss-copy-source-if-match"`

	// The object transfer condition. If the input ETag value does not match the ETag value of the object
	// the system transfers the object normally and returns 200 OK. Otherwise, OSS returns 304 Not Modified.
	IfNoneMatch *string `input:"header,x-oss-copy-source-if-none-match"`

	// The object transfer condition. If the specified time is earlier than the actual modified time of the object,
	// the system transfers the object normally and returns 200 OK. Otherwise, OSS returns 304 Not Modified.
	// The time must be in GMT. Example: Fri, 13 Nov 2015 14:47:53 GMT.
	IfModifiedSince *string `input:"header,x-oss-copy-source-if-modified-since"`

	// The object transfer condition. If the specified time is the same as or later than the actual modified time of the object,
	// OSS transfers the object normally and returns 200 OK. Otherwise, OSS returns 412 Precondition Failed.
	// The time must be in GMT. Example: Fri, 13 Nov 2015 14:47:53 GMT.
	IfUnmodifiedSince *string `input:"header,x-oss-copy-source-if-unmodified-since"`

	// Specify the speed limit value. The speed limit value ranges from  245760 to 838860800, with a unit of bit/s.
	TrafficLimit int64 `input:"header,x-oss-traffic-limit"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type UploadPartCopyResult struct {
	// The time when the returned objects were last modified.
	LastModified *time.Time `xml:"LastModified"`

	// Entity tag for the uploaded part.
	ETag *string `xml:"ETag"`

	// The version ID of the source object.
	VersionId *string `output:"header,x-oss-copy-source-version-id"`

	ResultCommon
}

// UploadPartCopy You can call this operation to copy data from an existing object to upload a part by adding a x-oss-copy-request header to UploadPart.
func (c *Client) UploadPartCopy(ctx context.Context, request *UploadPartCopyRequest, optFns ...func(*Options)) (*UploadPartCopyResult, error) {
	var err error
	if request == nil {
		request = &UploadPartCopyRequest{}
	}
	input := &OperationInput{
		OpName: "UploadPartCopy",
		Method: "PUT",
		Bucket: request.Bucket,
		Key:    request.Key,
		Headers: map[string]string{
			"x-oss-copy-source": encodeSourceObject(request),
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &UploadPartCopyResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type CompleteMultipartUploadRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,uploadId,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The ID of the multipart upload task.
	UploadId *string `input:"query,uploadId,required"`

	// The encoding type of the object names in the response. Valid value: url
	EncodingType *string `input:"query,encoding-type"`

	// Specifies whether the object with the same object name is overwritten when you call the CompleteMultipartUpload operation.
	// If x-oss-forbid-overwrite is not specified or set to false, existing objects can be overwritten by objects that have the same names.
	// If x-oss-forbid-overwrite is set to true, existing objects cannot be overwritten by objects that have the same names.
	ForbidOverwrite *string `input:"header,x-oss-forbid-overwrite"`

	// Specifies whether to list all parts that are uploaded by using the current upload ID. Valid value: yes
	CompleteAll *string `input:"header,x-oss-complete-all"`

	// The container that stores the content of the CompleteMultipartUpload
	CompleteMultipartUpload *CompleteMultipartUpload `input:"body,CompleteMultipartUpload,xml"`

	// The access control list (ACL) of the object.
	Acl ObjectACLType `input:"header,x-oss-object-acl"`

	// A callback parameter is a Base64-encoded string that contains multiple fields in the JSON format.
	Callback *string `input:"header,x-oss-callback"`

	// Configure custom parameters by using the callback-var parameter.
	CallbackVar *string `input:"header,x-oss-callback-var"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type UploadPart struct {
	// The number of parts.
	PartNumber int32 `xml:"PartNumber"`

	// The ETag values that are returned by OSS after parts are uploaded.
	ETag *string `xml:"ETag"`
}

type CompleteMultipartUpload struct {
	Parts []UploadPart `xml:"Part"`
}
type UploadParts []UploadPart

func (slice UploadParts) Len() int {
	return len(slice)
}
func (slice UploadParts) Less(i, j int) bool {
	return slice[i].PartNumber < slice[j].PartNumber
}
func (slice UploadParts) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

type CompleteMultipartUploadResult struct {
	// The version ID of the source object.
	VersionId *string `output:"header,x-oss-version-id"`

	// The 64-bit CRC value of the object.
	// This value is calculated based on the ECMA-182 standard.
	HashCRC64 *string `output:"header,x-oss-hash-crc64ecma"`

	// The encoding type of the name of the deleted object in the response.
	// If encoding-type is specified in the request, the object name is encoded in the returned result.
	EncodingType *string `xml:"EncodingType"`

	// The URL that is used to access the uploaded object.
	Location *string `xml:"Location"`

	// The name of the bucket.
	Bucket *string `xml:"Bucket"`

	// The name of the uploaded object.
	Key *string `xml:"Key"`

	// The ETag that is generated when an object is created.
	// ETags are used to identify the content of objects.
	ETag *string `xml:"ETag"`

	CallbackResult map[string]any

	ResultCommon
}

// CompleteMultipartUpload Completes the multipart upload task of an object after all parts of the object are uploaded.
func (c *Client) CompleteMultipartUpload(ctx context.Context, request *CompleteMultipartUploadRequest, optFns ...func(*Options)) (*CompleteMultipartUploadResult, error) {
	var err error
	if request == nil {
		request = &CompleteMultipartUploadRequest{}
	}
	input := &OperationInput{
		OpName: "CompleteMultipartUpload",
		Method: "POST",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"encoding-type": "url",
		},
	}

	if request.CompleteMultipartUpload != nil && len(request.CompleteMultipartUpload.Parts) > 0 {
		sort.Sort(UploadParts(request.CompleteMultipartUpload.Parts))
	}

	marshalFns := []func(any, *OperationInput) error{
		updateContentMd5,
	}
	unmarshalFns := []func(result any, output *OperationOutput) error{
		unmarshalHeader,
	}

	if request.Callback != nil {
		marshalFns = append(marshalFns, addCallback)
		unmarshalFns = append(unmarshalFns, unmarshalCallbackBody)
	} else {
		unmarshalFns = append(unmarshalFns, unmarshalBodyXml, unmarshalEncodeType)
	}

	if err = c.marshalInput(request, input, marshalFns...); err != nil {
		return nil, err
	}

	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &CompleteMultipartUploadResult{}
	if err = c.unmarshalOutput(result, output, unmarshalFns...); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type AbortMultipartUploadRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,uploadId,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The ID of the multipart upload task.
	UploadId *string `input:"query,uploadId,required"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type AbortMultipartUploadResult struct {
	ResultCommon
}

// AbortMultipartUpload Cancels a multipart upload task and deletes the parts uploaded in the task.
func (c *Client) AbortMultipartUpload(ctx context.Context, request *AbortMultipartUploadRequest, optFns ...func(*Options)) (*AbortMultipartUploadResult, error) {
	var err error
	if request == nil {
		request = &AbortMultipartUploadRequest{}
	}
	input := &OperationInput{
		OpName: "AbortMultipartUpload",
		Method: "DELETE",
		Bucket: request.Bucket,
		Key:    request.Key,
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &AbortMultipartUploadResult{}
	if err = c.unmarshalOutput(result, output, discardBody); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type ListMultipartUploadsRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,uploadId,required"`

	// The character that is used to group objects by name. If you specify the delimiter parameter in the request,
	// the response contains the CommonPrefixes parameter. The objects whose names contain the same string from
	// the prefix to the next occurrence of the delimiter are grouped as a single result element in CommonPrefixes.
	Delimiter *string `input:"query,delimiter"`

	// The encoding type of the content in the response. Valid value: url
	EncodingType *string `input:"query,encoding-type"`

	// This parameter is used together with the upload-id-marker parameter to specify
	// the position from which the next list begins.
	KeyMarker *string `input:"query,key-marker"`

	// The maximum number of multipart upload tasks that can be returned for the current request.
	// Default value: 1000. Maximum value: 1000.
	MaxUploads int32 `input:"query,max-uploads"`

	// The prefix that the names of the returned objects must contain.
	Prefix *string `input:"query,prefix"`

	// The upload ID of the multipart upload task after which the list begins.
	// This parameter is used together with the key-marker parameter.
	UploadIdMarker *string `input:"query,upload-id-marker"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type ListMultipartUploadsResult struct {
	// The method used to encode the object name in the response.
	// If encoding-type is specified in the request, values of those elements including
	// Delimiter, KeyMarker, Prefix, NextKeyMarker, and Key are encoded in the returned result.
	EncodingType *string `xml:"EncodingType"`

	// The name of the bucket.
	Bucket *string `xml:"Bucket"`

	// The name of the object that corresponds to the multipart upload task after which the list begins.
	KeyMarker *string `xml:"KeyMarker"`

	// The upload ID of the multipart upload task after which the list begins.
	UploadIdMarker *string `xml:"UploadIdMarker"`

	// The upload ID of the multipart upload task after which the list begins.
	NextKeyMarker *string `xml:"NextKeyMarker"`

	// The NextUploadMarker value that is used for the UploadMarker value in
	// the next request if the response does not contain all required results.
	NextUploadIdMarker *string `xml:"NextUploadIdMarker"`

	// The character that is used to group objects by name.
	Delimiter *string `xml:"Delimiter"`

	// The prefix contained in the returned object names.
	Prefix *string `xml:"Prefix"`

	// The maximum number of multipart upload tasks returned by OSS.
	MaxUploads int32 `xml:"MaxUploads"`

	// Indicates whether the list of multipart upload tasks returned in the response is truncated.
	// true: Only part of the results are returned this time.
	// false: All results are returned.
	IsTruncated bool `xml:"IsTruncated"`

	Uploads []Upload `xml:"Upload"`

	ResultCommon
}

type Upload struct {
	// The name of the object for which a multipart upload task was initiated.
	Key *string `xml:"Key"`

	// The ID of the multipart upload task
	UploadId *string `xml:"UploadId"`

	// The time when the multipart upload task was initialized.
	Initiated *time.Time `xml:"Initiated"`
}

// ListMultipartUploads Lists all multipart upload tasks in progress. The tasks are not completed or canceled.
func (c *Client) ListMultipartUploads(ctx context.Context, request *ListMultipartUploadsRequest, optFns ...func(*Options)) (*ListMultipartUploadsResult, error) {
	var err error
	if request == nil {
		request = &ListMultipartUploadsRequest{}
	}
	input := &OperationInput{
		OpName: "ListMultipartUploads",
		Method: "GET",
		Bucket: request.Bucket,
		Parameters: map[string]string{
			"encoding-type": "url",
			"uploads":       "",
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5, enableNonStream); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListMultipartUploadsResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml, unmarshalEncodeType); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type ListPartsRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,uploadId,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The ID of the multipart upload task.
	UploadId *string `input:"query,uploadId,required"`

	// The encoding type of the content in the response. Valid value: url
	EncodingType *string `input:"query,encoding-type"`

	// The maximum number of parts that can be returned by OSS.
	// Default value: 1000. Maximum value: 1000.
	MaxParts int32 `input:"query,max-parts"`

	// The position from which the list starts.
	// All parts whose part numbers are greater than the value of this parameter are listed.
	PartNumberMarker int32 `input:"query,part-number-marker"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type ListPartsResult struct {
	// The method used to encode the object name in the response.
	// If encoding-type is specified in the request, values of those elements including
	// Delimiter, KeyMarker, Prefix, NextKeyMarker, and Key are encoded in the returned result.
	EncodingType *string `xml:"EncodingType"`

	// The name of the bucket.
	Bucket *string `xml:"Bucket"`

	// The name of the object that corresponds to the multipart upload task after which the list begins.
	Key *string `xml:"Key"`

	// The ID of the upload task.
	UploadId *string `xml:"UploadId"`

	// The position from which the list starts.
	// All parts whose part numbers are greater than the value of this parameter are listed.
	PartNumberMarker int32 `xml:"PartNumberMarker"`

	// The NextPartNumberMarker value that is used for the PartNumberMarker value in a subsequent
	// request when the response does not contain all required results.
	NextPartNumberMarker int32 `xml:"NextPartNumberMarker"`

	// he maximum number of parts in the response.
	MaxParts int32 `xml:"MaxParts"`

	// Indicates whether the list of parts returned in the response has been truncated.
	// true: Only part of the results are returned this time.
	// false: All results are returned.
	IsTruncated bool `xml:"IsTruncated"`

	// The storage class of the object.
	StorageClass *string `xml:"StorageClass"`

	// The encrypted data key.
	// The encrypted data key is a string encrypted by a customer master key and encoded in Base64.
	// Only available in client-side encryption
	ClientEncryptionKey *string `xml:"ClientEncryptionKey"`

	// The initial value that is randomly generated for data encryption.
	// The initial value is is a string encrypted by a customer master key and encoded in Base64.
	// Only available in client-side encryption
	ClientEncryptionStart *string `xml:"ClientEncryptionStart"`

	// The algorithm used to encrypt data.
	// Only available in client-side encryption
	ClientEncryptionCekAlg *string `xml:"ClientEncryptionCekAlg"`

	// The algorithm used to encrypt the data key.
	// Only available in client-side encryption
	ClientEncryptionWrapAlg *string `xml:"ClientEncryptionWrapAlg"`

	// The total size of the data to encrypt for multipart upload when init_multipart is called.
	// Only available in client-side encryption
	ClientEncryptionDataSize *int64 `xml:"ClientEncryptionDataSize"`

	// The size of each part to encrypt for multipart upload when init_multipart is called.
	// Only available in client-side encryption
	ClientEncryptionPartSize *int64 `xml:"ClientEncryptionPartSize"`

	Parts []Part `xml:"Part"`

	ResultCommon
}

type Part struct {
	// The number that identifies a part.
	PartNumber int32 `xml:"PartNumber"`

	// The ETag value of the content of the uploaded part.
	ETag *string `xml:"ETag"`

	// The time when the part was uploaded.
	LastModified *time.Time `xml:"LastModified"`

	// The size of the uploaded parts.
	Size int64 `xml:"Size"`

	// The 64-bit CRC value of the object.
	// This value is calculated based on the ECMA-182 standard.
	HashCRC64 *string `xml:"HashCrc64ecma"`
}

// ListParts Lists all parts that are uploaded by using a specified upload ID.
func (c *Client) ListParts(ctx context.Context, request *ListPartsRequest, optFns ...func(*Options)) (*ListPartsResult, error) {
	var err error
	if request == nil {
		request = &ListPartsRequest{}
	}
	input := &OperationInput{
		OpName: "ListParts",
		Method: "GET",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"encoding-type": "url",
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5, enableNonStream); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListPartsResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml, unmarshalEncodeType); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutSymlinkRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// The destination object to which the symbolic link points.
	Target *string `input:"header,x-oss-symlink-target,required"`

	// Specifies whether the PutSymlink operation overwrites the object that has the same name.
	// If you do not specify the x-oss-forbid-overwrite header or if you set the x-oss-forbid-overwrite header to false, the object that has the same name is overwritten.
	// If you set the x-oss-forbid-overwrite header to true, the object that has the same name cannot be overwritten.
	ForbidOverwrite *string `input:"header,x-oss-forbid-overwrite"`

	// The ACL of the object. Default value: default.
	Acl ObjectACLType `input:"header,x-oss-object-acl"`

	// The storage class of the object.
	StorageClass StorageClassType `input:"header,x-oss-storage-class"`

	// The metadata of the object that you want to symlink.
	Metadata map[string]string `input:"header,x-oss-meta-,usermeta"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type PutSymlinkResult struct {
	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	ResultCommon
}

// PutSymlink Creates a symbolic link that points to a destination object. You can use the symbolic link to access the destination object.
func (c *Client) PutSymlink(ctx context.Context, request *PutSymlinkRequest, optFns ...func(*Options)) (*PutSymlinkResult, error) {
	var err error
	if request == nil {
		request = &PutSymlinkRequest{}
	}
	input := &OperationInput{
		OpName: "PutSymlink",
		Method: "PUT",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"symlink": "",
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutSymlinkResult{}
	if err = c.unmarshalOutput(result, output, discardBody, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetSymlinkRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Version of the object.
	VersionId *string `input:"query,versionId"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type GetSymlinkResult struct {
	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	// Indicates the target object that the symbol link directs to.
	Target *string `output:"header,x-oss-symlink-target"`

	// Entity tag for the uploaded object.
	ETag *string `output:"header,ETag"`

	// The metadata of the object that you want to symlink.
	Metadata map[string]string `output:"header,x-oss-meta-,usermeta"`

	ResultCommon
}

// GetSymlink Obtains a symbol link. To perform GetSymlink operations, you must have the read permission on the symbol link.
func (c *Client) GetSymlink(ctx context.Context, request *GetSymlinkRequest, optFns ...func(*Options)) (*GetSymlinkResult, error) {
	var err error
	if request == nil {
		request = &GetSymlinkRequest{}
	}
	input := &OperationInput{
		OpName: "GetSymlink",
		Method: "GET",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"symlink": "",
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetSymlinkResult{}
	if err = c.unmarshalOutput(result, output, discardBody, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutObjectTaggingRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Version of the object.
	VersionId *string `input:"query,versionId"`

	Tagging *Tagging `input:"body,Tagging,xml,required"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type Tagging struct {
	// The container used to store a set of Tags.
	TagSet *TagSet `xml:"TagSet" json:"TagSet,omitempty"`
}

type TagSet struct {
	// The tags.
	Tags []Tag `xml:"Tag" json:"Tag,omitempty"`
}

type Tag struct {
	// The key of a tag.
	// *  A tag key can be up to 64 bytes in length.
	// *  A tag key cannot start with `http://`, `https://`, or `Aliyun`.
	// *  A tag key must be UTF-8 encoded.
	// *  A tag key cannot be left empty.
	Key *string `xml:"Key" json:"Key,omitempty"`

	// The value of the tag that you want to add or modify.
	// * A tag value can be up to 128 bytes in length.
	// * A tag value must be UTF-8 encoded.
	// * The tag value can be left empty.
	Value *string `xml:"Value" json:"Value,omitempty"`
}

type PutObjectTaggingResult struct {
	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	ResultCommon
}

// PutObjectTagging Adds tags to an object or updates the tags added to the object. Each tag added to an object is a key-value pair.
func (c *Client) PutObjectTagging(ctx context.Context, request *PutObjectTaggingRequest, optFns ...func(*Options)) (*PutObjectTaggingResult, error) {
	var err error
	if request == nil {
		request = &PutObjectTaggingRequest{}
	}
	input := &OperationInput{
		OpName: "PutObjectTagging",
		Method: "PUT",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"tagging": "",
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutObjectTaggingResult{}
	if err = c.unmarshalOutput(result, output, discardBody, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetObjectTaggingRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Version of the object.
	VersionId *string `input:"query,versionId"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type GetObjectTaggingResult struct {
	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	// The container used to store the collection of tags.
	Tags []Tag `xml:"TagSet>Tag"`

	ResultCommon
}

// GetObjectTagging You can call this operation to query the tags of an object.
func (c *Client) GetObjectTagging(ctx context.Context, request *GetObjectTaggingRequest, optFns ...func(*Options)) (*GetObjectTaggingResult, error) {
	var err error
	if request == nil {
		request = &GetObjectTaggingRequest{}
	}
	input := &OperationInput{
		OpName: "GetObjectTagging",
		Method: "GET",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"tagging": "",
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetObjectTaggingResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXml, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteObjectTaggingRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Version of the object.
	VersionId *string `input:"query,versionId"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type DeleteObjectTaggingResult struct {
	// Version of the object.
	VersionId *string `output:"header,x-oss-version-id"`

	ResultCommon
}

// DeleteObjectTagging You can call this operation to delete the tags of a specified object.
func (c *Client) DeleteObjectTagging(ctx context.Context, request *DeleteObjectTaggingRequest, optFns ...func(*Options)) (*DeleteObjectTaggingResult, error) {
	var err error
	if request == nil {
		request = &DeleteObjectTaggingRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteObjectTagging",
		Method: "DELETE",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"tagging": "",
		},
	}
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteObjectTaggingResult{}
	if err = c.unmarshalOutput(result, output, discardBody, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type ProcessObjectRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Image processing parameters
	Process *string `input:"x-oss-process,nop,required"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type ProcessObjectResult struct {
	Bucket        string `json:"bucket"`
	FileSize      int    `json:"fileSize"`
	Object        string `json:"object"`
	ProcessStatus string `json:"status"`
	ResultCommon
}

// ProcessObject apply process on the specified image file.
func (c *Client) ProcessObject(ctx context.Context, request *ProcessObjectRequest, optFns ...func(*Options)) (*ProcessObjectResult, error) {
	var err error
	if request == nil {
		request = &ProcessObjectRequest{}
	}
	input := &OperationInput{
		OpName: "ProcessObject",
		Method: "POST",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"x-oss-process": "",
		},
	}
	if err = c.marshalInput(request, input, addProcess, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ProcessObjectResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyDefault, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type AsyncProcessObjectRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Image async processing parameters
	AsyncProcess *string `input:"x-oss-async-process,nop,required"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type AsyncProcessObjectResult struct {
	EventId   string `json:"EventId"`
	RequestId string `json:"RequestId"`
	TaskId    string `json:"TaskId"`
	ResultCommon
}

// AsyncProcessObject apply async process on the specified image file.
func (c *Client) AsyncProcessObject(ctx context.Context, request *AsyncProcessObjectRequest, optFns ...func(*Options)) (*AsyncProcessObjectResult, error) {
	var err error
	if request == nil {
		request = &AsyncProcessObjectRequest{}
	}
	input := &OperationInput{
		OpName: "AsyncProcessObject",
		Method: "POST",
		Bucket: request.Bucket,
		Key:    request.Key,
		Parameters: map[string]string{
			"x-oss-async-process": "",
		},
	}
	if err = c.marshalInput(request, input, addProcess, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &AsyncProcessObjectResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyDefault, unmarshalHeader); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type CleanRestoredObjectRequest struct {
	// The name of the bucket
	Bucket *string `input:"host,bucket,required"`

	// The name of the object.
	Key *string `input:"path,key,required"`

	// Version of the object.
	VersionId *string `input:"query,versionId"`

	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string `input:"header,x-oss-request-payer"`

	RequestCommon
}

type CleanRestoredObjectResult struct {
	ResultCommon
}

// CleanRestoredObject You can call this operation to clean an object restored from Archive or Cold Archive state. After that, the restored object returns to the frozen state.
func (c *Client) CleanRestoredObject(ctx context.Context, request *CleanRestoredObjectRequest, optFns ...func(*Options)) (*CleanRestoredObjectResult, error) {
	var err error
	if request == nil {
		request = &CleanRestoredObjectRequest{}
	}
	input := &OperationInput{
		OpName: "CleanRestoredObject",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"cleanRestoredObject": "",
		},
		Bucket: request.Bucket,
		Key:    request.Key,
	}

	input.OpMetadata.Set(signer.SubResource, []string{"cleanRestoredObject"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &CleanRestoredObjectResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
