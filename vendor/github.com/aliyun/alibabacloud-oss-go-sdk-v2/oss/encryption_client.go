package oss

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/crypto"
)

// EncryptionUaSuffix user agent tag for client encryption
const (
	EncryptionUaSuffix string = "OssEncryptionClient"
)

type EncryptionClientOptions struct {
	MasterCiphers []crypto.MasterCipher
}

type EncryptionClient struct {
	client           *Client
	defualtCCBuilder crypto.ContentCipherBuilder
	ccBuilderMap     map[string]crypto.ContentCipherBuilder
	alignLen         int
}

// EncryptionMultiPartContext save encryption or decryption information
type EncryptionMultiPartContext struct {
	ContentCipher crypto.ContentCipher
	DataSize      int64
	PartSize      int64
}

// Valid judge PartCryptoContext is valid or not
func (ec EncryptionMultiPartContext) Valid() bool {
	if ec.ContentCipher == nil || ec.DataSize == 0 || ec.PartSize == 0 {
		return false
	}
	return true
}

func NewEncryptionClient(c *Client, masterCipher crypto.MasterCipher, optFns ...func(*EncryptionClientOptions)) (*EncryptionClient, error) {
	options := EncryptionClientOptions{}
	for _, fn := range optFns {
		fn(&options)
	}

	if masterCipher == nil {
		return nil, NewErrParamNull("masterCipher")
	}

	defualtCCBuilder := crypto.CreateAesCtrCipher(masterCipher)
	ccBuilderMap := map[string]crypto.ContentCipherBuilder{}
	for _, m := range options.MasterCiphers {
		if m != nil && len(m.GetMatDesc()) > 0 {
			ccBuilderMap[m.GetMatDesc()] = crypto.CreateAesCtrCipher(m)
		}
	}

	e := &EncryptionClient{
		client:           c,
		defualtCCBuilder: defualtCCBuilder,
		ccBuilderMap:     ccBuilderMap,
		alignLen:         16,
	}

	return e, nil
}

func (e *EncryptionClient) Unwrap() *Client { return e.client }

// GetObjectMeta Queries the metadata of an object, including ETag, Size, and LastModified.
// The content of the object is not returned.
func (e *EncryptionClient) GetObjectMeta(ctx context.Context, request *GetObjectMetaRequest, optFns ...func(*Options)) (*GetObjectMetaResult, error) {
	return e.client.GetObjectMeta(ctx, request, optFns...)
}

// HeadObject Queries information about all objects in a bucket.
func (e *EncryptionClient) HeadObject(ctx context.Context, request *HeadObjectRequest, optFns ...func(*Options)) (*HeadObjectResult, error) {
	return e.client.HeadObject(ctx, request, optFns...)
}

// GetObject Downloads a object.
func (e *EncryptionClient) GetObject(ctx context.Context, request *GetObjectRequest, optFns ...func(*Options)) (*GetObjectResult, error) {
	return e.getObjectSecurely(ctx, request, optFns...)
}

// PutObject Uploads a object.
func (e *EncryptionClient) PutObject(ctx context.Context, request *PutObjectRequest, optFns ...func(*Options)) (*PutObjectResult, error) {
	return e.putObjectSecurely(ctx, request, optFns...)
}

// InitiateMultipartUpload Initiates a multipart upload task before you can upload data in parts to Object Storage Service (OSS).
func (e *EncryptionClient) InitiateMultipartUpload(ctx context.Context, request *InitiateMultipartUploadRequest, optFns ...func(*Options)) (*InitiateMultipartUploadResult, error) {
	return e.initiateMultipartUploadSecurely(ctx, request, optFns...)
}

// UploadPart Call the UploadPart interface to upload data in blocks (parts) based on the specified Object name and uploadId.
func (e *EncryptionClient) UploadPart(ctx context.Context, request *UploadPartRequest, optFns ...func(*Options)) (*UploadPartResult, error) {
	return e.uploadPartSecurely(ctx, request, optFns...)
}

// CompleteMultipartUpload Completes the multipart upload task of an object after all parts of the object are uploaded.
func (e *EncryptionClient) CompleteMultipartUpload(ctx context.Context, request *CompleteMultipartUploadRequest, optFns ...func(*Options)) (*CompleteMultipartUploadResult, error) {
	return e.client.CompleteMultipartUpload(ctx, request, optFns...)
}

// AbortMultipartUpload Cancels a multipart upload task and deletes the parts uploaded in the task.
func (e *EncryptionClient) AbortMultipartUpload(ctx context.Context, request *AbortMultipartUploadRequest, optFns ...func(*Options)) (*AbortMultipartUploadResult, error) {
	return e.client.AbortMultipartUpload(ctx, request, optFns...)
}

// ListParts Lists all parts that are uploaded by using a specified upload ID.
func (e *EncryptionClient) ListParts(ctx context.Context, request *ListPartsRequest, optFns ...func(*Options)) (*ListPartsResult, error) {
	return e.client.ListParts(ctx, request, optFns...)
}

// NewDownloader creates a new Downloader instance to download objects.
func (c *EncryptionClient) NewDownloader(optFns ...func(*DownloaderOptions)) *Downloader {
	return NewDownloader(c, optFns...)
}

// NewUploader creates a new Uploader instance to upload objects.
func (c *EncryptionClient) NewUploader(optFns ...func(*UploaderOptions)) *Uploader {
	return NewUploader(c, optFns...)
}

// OpenFile opens the named file for reading.
func (c *EncryptionClient) OpenFile(ctx context.Context, bucket string, key string, optFns ...func(*OpenOptions)) (*ReadOnlyFile, error) {
	return NewReadOnlyFile(ctx, c, bucket, key, optFns...)
}

func (e *EncryptionClient) getObjectSecurely(ctx context.Context, request *GetObjectRequest, optFns ...func(*Options)) (*GetObjectResult, error) {
	if request == nil {
		return nil, NewErrParamNull("request")
	}

	var (
		err          error
		httpRange    *HTTPRange
		discardCount int64 = 0
		adjustOffset int64 = 0
		closeBody    bool  = true
	)

	if request.Range != nil {
		httpRange, err = ParseRange(*request.Range)
		if err != nil {
			return nil, err
		}
		offset := httpRange.Offset
		count := httpRange.Count
		adjustOffset = adjustRangeStart(offset, int64(e.alignLen))
		discardCount = httpRange.Offset - adjustOffset

		if discardCount != 0 {
			if count > 0 {
				count += discardCount
			}
			httpRange.Offset = adjustOffset
			httpRange.Count = count
		}
	}

	eRequest := request
	if httpRange != nil && discardCount > 0 {
		_request := *request
		eRequest = &_request
		eRequest.Range = httpRange.FormatHTTPRange()
		eRequest.RangeBehavior = Ptr("standard")
	}

	result, err := e.client.GetObject(ctx, eRequest, optFns...)

	if err != nil {
		return nil, err
	}

	defer func() {
		if closeBody && result.Body != nil {
			result.Body.Close()
		}
	}()

	if hasEncryptedHeader(result.Headers) {
		envelope, err := getEnvelopeFromHeader(result.Headers)
		if err != nil {
			return nil, err
		}
		if !isValidContentAlg(envelope.CEKAlg) {
			return nil, fmt.Errorf("not supported content algorithm %s,object:%s", envelope.CEKAlg, ToString(request.Key))
		}
		if !envelope.IsValid() {
			return nil, fmt.Errorf("getEnvelopeFromHeader error,object:%s", ToString(request.Key))
		}

		// use ContentCipherBuilder to decrpt object by default
		cc, err := e.getContentCipherBuilder(envelope).ContentCipherEnv(envelope)
		if err != nil {
			return nil, fmt.Errorf("%s,object:%s", err.Error(), ToString(request.Key))
		}

		if adjustOffset > 0 {
			cipherData := cc.GetCipherData().Clone()
			cipherData.SeekIV(uint64(adjustOffset))
			cc, _ = cc.Clone(cipherData)
		}

		result.Body, err = cc.DecryptContent(result.Body)
	}

	if discardCount > 0 && err == nil {
		//rewrite ContentRange & ContentRange
		if result.ContentRange != nil {
			if from, to, total, cerr := ParseContentRange(*result.ContentRange); cerr == nil {
				from += discardCount
				value := fmt.Sprintf("bytes %v-%v/%v", from, to, total)
				result.ContentRange = Ptr(value)
				result.Headers.Set(HTTPHeaderContentRange, value)
			}
		} else {
			result.Headers.Set(HTTPHeaderContentRange, fmt.Sprintf("bytes %v-/*", discardCount))
		}
		if result.ContentLength > 0 {
			result.ContentLength -= discardCount
			result.Headers.Set(HTTPHeaderContentLength, fmt.Sprint(result.ContentLength))
		}
		result.Body = &DiscardReadCloser{
			RC:      result.Body,
			Discard: int(discardCount),
		}
	}

	closeBody = false
	return result, err
}

func (e *EncryptionClient) putObjectSecurely(ctx context.Context, request *PutObjectRequest, optFns ...func(*Options)) (*PutObjectResult, error) {
	if request == nil {
		return nil, NewErrParamNull("request")
	}
	cc, err := e.defualtCCBuilder.ContentCipher()
	if err != nil {
		return nil, err
	}
	cryptoReader, err := cc.EncryptContent(request.Body)
	if err != nil {
		return nil, err
	}

	eRequest := *request
	eRequest.Body = cryptoReader
	addCryptoHeaders(&eRequest, cc.GetCipherData())

	return e.client.PutObject(ctx, &eRequest, optFns...)
}

func (e *EncryptionClient) initiateMultipartUploadSecurely(ctx context.Context, request *InitiateMultipartUploadRequest, optFns ...func(*Options)) (*InitiateMultipartUploadResult, error) {
	var err error
	if request == nil {
		return nil, NewErrParamNull("request")
	}
	if err = e.validEncryptionContext(request); err != nil {
		return nil, err
	}
	cc, err := e.defualtCCBuilder.ContentCipher()
	if err != nil {
		return nil, err
	}
	eRequest := *request
	addMultiPartCryptoHeaders(&eRequest, cc.GetCipherData())

	result, err := e.client.InitiateMultipartUpload(ctx, &eRequest, optFns...)
	if err != nil {
		return nil, err
	}

	result.CSEMultiPartContext = &EncryptionMultiPartContext{
		ContentCipher: cc,
		PartSize:      ToInt64(request.CSEPartSize),
		DataSize:      ToInt64(request.CSEDataSize),
	}
	return result, nil
}

func (e *EncryptionClient) uploadPartSecurely(ctx context.Context, request *UploadPartRequest, optFns ...func(*Options)) (*UploadPartResult, error) {
	if request == nil {
		return nil, NewErrParamNull("request")
	}
	if request.CSEMultiPartContext == nil {
		return nil, NewErrParamNull("request.CSEMultiPartContext")
	}
	cseCtx := request.CSEMultiPartContext
	if !cseCtx.Valid() {
		return nil, fmt.Errorf("request.CSEMultiPartContext is invalid")
	}
	if cseCtx.PartSize%int64(e.alignLen) != 0 {
		return nil, fmt.Errorf("CSEMultiPartContext's PartSize must be aligned to %v", e.alignLen)
	}

	cipherData := cseCtx.ContentCipher.GetCipherData().Clone()
	// caclulate iv based on part number
	if request.PartNumber > 1 {
		cipherData.SeekIV(uint64(request.PartNumber-1) * uint64(cseCtx.PartSize))
	}

	// for parallel upload part
	cc, _ := cseCtx.ContentCipher.Clone(cipherData)

	cryptoReader, err := cc.EncryptContent(request.Body)
	if err != nil {
		return nil, err
	}

	eRequest := *request
	eRequest.Body = cryptoReader

	addUploadPartCryptoHeaders(&eRequest, cseCtx, cc.GetCipherData())

	return e.client.UploadPart(ctx, &eRequest, optFns...)
}

func (e *EncryptionClient) getContentCipherBuilder(envelope crypto.Envelope) crypto.ContentCipherBuilder {
	if ccb, ok := e.ccBuilderMap[envelope.MatDesc]; ok {
		return ccb
	}
	return e.defualtCCBuilder
}

func (e *EncryptionClient) validEncryptionContext(request *InitiateMultipartUploadRequest) error {
	partSize := ToInt64(request.CSEPartSize)
	if partSize <= 0 {
		return NewErrParamInvalid("request.CSEPartSize")
	}

	if partSize%int64(e.alignLen) != 0 {
		return fmt.Errorf("request.CSEPartSize must aligned to the %v", e.alignLen)
	}

	return nil
}

func hasEncryptedHeader(headers http.Header) bool {
	return len(headers.Get(OssClientSideEncryptionKey)) > 0
}

// addCryptoHeaders save Envelope information in oss meta
func addCryptoHeaders(request *PutObjectRequest, cd *crypto.CipherData) {
	if request.Headers == nil {
		request.Headers = map[string]string{}
	}

	// convert content-md5
	if request.ContentMD5 != nil {
		request.Headers[OssClientSideEncryptionUnencryptedContentMD5] = *request.ContentMD5
		request.ContentMD5 = nil
	}

	// convert content-length
	if request.ContentLength != nil {
		request.Headers[OssClientSideEncryptionUnencryptedContentLength] = fmt.Sprint(*request.ContentLength)
		request.ContentLength = nil
	}

	// matDesc
	if len(cd.MatDesc) > 0 {
		request.Headers[OssClientSideEncryptionMatDesc] = cd.MatDesc
	}

	// encrypted key
	strEncryptedKey := base64.StdEncoding.EncodeToString(cd.EncryptedKey)
	request.Headers[OssClientSideEncryptionKey] = strEncryptedKey

	// encrypted iv
	strEncryptedIV := base64.StdEncoding.EncodeToString(cd.EncryptedIV)
	request.Headers[OssClientSideEncryptionStart] = strEncryptedIV

	// wrap alg
	request.Headers[OssClientSideEncryptionWrapAlg] = cd.WrapAlgorithm

	// cek alg
	request.Headers[OssClientSideEncryptionCekAlg] = cd.CEKAlgorithm
}

// addMultiPartCryptoHeaders save Envelope information in oss meta
func addMultiPartCryptoHeaders(request *InitiateMultipartUploadRequest, cd *crypto.CipherData) {
	if request.Headers == nil {
		request.Headers = map[string]string{}
	}

	// matDesc
	if len(cd.MatDesc) > 0 {
		request.Headers[OssClientSideEncryptionMatDesc] = cd.MatDesc
	}

	if ToInt64(request.CSEDataSize) > 0 {
		request.Headers[OssClientSideEncryptionDataSize] = fmt.Sprint(*request.CSEDataSize)
	}

	request.Headers[OssClientSideEncryptionPartSize] = fmt.Sprint(*request.CSEPartSize)

	// encrypted key
	strEncryptedKey := base64.StdEncoding.EncodeToString(cd.EncryptedKey)
	request.Headers[OssClientSideEncryptionKey] = strEncryptedKey

	// encrypted iv
	strEncryptedIV := base64.StdEncoding.EncodeToString(cd.EncryptedIV)
	request.Headers[OssClientSideEncryptionStart] = strEncryptedIV

	// wrap alg
	request.Headers[OssClientSideEncryptionWrapAlg] = cd.WrapAlgorithm

	// cek alg
	request.Headers[OssClientSideEncryptionCekAlg] = cd.CEKAlgorithm
}

// addUploadPartCryptoHeaders save Envelope information in oss meta
func addUploadPartCryptoHeaders(request *UploadPartRequest, cseContext *EncryptionMultiPartContext, cd *crypto.CipherData) {
	if request.Headers == nil {
		request.Headers = map[string]string{}
	}

	// matDesc
	if len(cd.MatDesc) > 0 {
		request.Headers[OssClientSideEncryptionMatDesc] = cd.MatDesc
	}

	if cseContext.DataSize > 0 {
		request.Headers[OssClientSideEncryptionDataSize] = fmt.Sprint(cseContext.DataSize)
	}

	request.Headers[OssClientSideEncryptionPartSize] = fmt.Sprint(cseContext.PartSize)

	// encrypted key
	strEncryptedKey := base64.StdEncoding.EncodeToString(cd.EncryptedKey)
	request.Headers[OssClientSideEncryptionKey] = strEncryptedKey

	// encrypted iv
	strEncryptedIV := base64.StdEncoding.EncodeToString(cd.EncryptedIV)
	request.Headers[OssClientSideEncryptionStart] = strEncryptedIV

	// wrap alg
	request.Headers[OssClientSideEncryptionWrapAlg] = cd.WrapAlgorithm

	// cek alg
	request.Headers[OssClientSideEncryptionCekAlg] = cd.CEKAlgorithm
}

func isValidContentAlg(algName string) bool {
	// now content encyrption only support aec/ctr algorithm
	return algName == crypto.AesCtrAlgorithm
}

func adjustRangeStart(start, align int64) int64 {
	return (start / align) * align
}

func getEnvelopeFromHeader(header http.Header) (crypto.Envelope, error) {
	var envelope crypto.Envelope
	envelope.IV = header.Get(OssClientSideEncryptionStart)
	decodedIV, err := base64.StdEncoding.DecodeString(envelope.IV)
	if err != nil {
		return envelope, err
	}
	envelope.IV = string(decodedIV)

	envelope.CipherKey = header.Get(OssClientSideEncryptionKey)
	decodedKey, err := base64.StdEncoding.DecodeString(envelope.CipherKey)
	if err != nil {
		return envelope, err
	}
	envelope.CipherKey = string(decodedKey)
	envelope.MatDesc = header.Get(OssClientSideEncryptionMatDesc)
	envelope.WrapAlg = header.Get(OssClientSideEncryptionWrapAlg)
	envelope.CEKAlg = header.Get(OssClientSideEncryptionCekAlg)
	envelope.UnencryptedMD5 = header.Get(OssClientSideEncryptionUnencryptedContentMD5)
	envelope.UnencryptedContentLen = header.Get(OssClientSideEncryptionUnencryptedContentLength)
	return envelope, err
}

func getEnvelopeFromListParts(result *ListPartsResult) (crypto.Envelope, error) {
	var envelope crypto.Envelope
	if result == nil {
		return envelope, NewErrParamNull("result.*ListPartsResult")
	}
	envelope.IV = ToString(result.ClientEncryptionStart)
	decodedIV, err := base64.StdEncoding.DecodeString(envelope.IV)
	if err != nil {
		return envelope, err
	}
	envelope.IV = string(decodedIV)

	envelope.CipherKey = ToString(result.ClientEncryptionKey)
	decodedKey, err := base64.StdEncoding.DecodeString(envelope.CipherKey)
	if err != nil {
		return envelope, err
	}
	envelope.CipherKey = string(decodedKey)
	envelope.WrapAlg = ToString(result.ClientEncryptionWrapAlg)
	envelope.CEKAlg = ToString(result.ClientEncryptionCekAlg)
	return envelope, err
}
