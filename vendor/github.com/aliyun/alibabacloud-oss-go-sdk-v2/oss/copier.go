package oss

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/transport"
)

var metadataCopied = map[string]struct{}{
	"content-type":        {},
	"content-language":    {},
	"content-encoding":    {},
	"content-disposition": {},
	"cache-control":       {},
	"expires":             {},
}

type CopierOptions struct {
	PartSize int64

	ParallelNum int

	MultipartCopyThreshold int64

	LeavePartsOnError bool

	DisableShallowCopy bool

	ClientOptions []func(*Options)

	// ShallowCopy Flags
	NoCheckSSE         bool
	NoCheckCrossBucket bool

	// MetaProperties and TagProperties takes effect in Copier.Copy
	MetadataProperties *HeadObjectResult

	TagProperties *GetObjectTaggingResult
}

func WithCopierPartSize(value int64) func(*CopierOptions) {
	return func(o *CopierOptions) {
		o.PartSize = value
	}
}

func WithCopierParallelNum(value int) func(*CopierOptions) {
	return func(o *CopierOptions) {
		o.ParallelNum = value
	}
}

func WithCopierNoCheckSSE(value bool) func(*CopierOptions) {
	return func(o *CopierOptions) {
		o.NoCheckSSE = value
	}
}

func WithCopierNoCheckCrossBucket(value bool) func(*CopierOptions) {
	return func(o *CopierOptions) {
		o.NoCheckCrossBucket = value
	}
}

type Copier struct {
	options      CopierOptions
	client       CopyAPIClient
	featureFlags FeatureFlagsType
}

// NewCopier creates a new Copier instance to copy objects.
// Pass In additional functional options to customize the copier's behavior.
func NewCopier(api CopyAPIClient, optFns ...func(*CopierOptions)) *Copier {
	options := CopierOptions{
		PartSize:               DefaultCopyPartSize,
		ParallelNum:            DefaultCopyParallel,
		MultipartCopyThreshold: DefaultCopyThreshold,
		LeavePartsOnError:      false,
		DisableShallowCopy:     false,
	}

	for _, fn := range optFns {
		fn(&options)
	}

	options.TagProperties = nil
	options.MetadataProperties = nil

	c := &Copier{
		client:  api,
		options: options,
	}

	//Get Client Feature
	switch t := api.(type) {
	case *Client:
		c.featureFlags = t.options.FeatureFlags
	}

	return c
}

type CopyResult struct {
	UploadId *string

	ETag *string

	VersionId *string

	HashCRC64 *string

	ResultCommon
}

type CopyError struct {
	Err      error
	UploadId string
	Path     string
}

func (m *CopyError) Error() string {
	var extra string
	if m.Err != nil {
		extra = fmt.Sprintf(", cause: %s", m.Err.Error())
	}
	return fmt.Sprintf("copy failed, upload id: %s%s", m.UploadId, extra)
}

func (m *CopyError) Unwrap() error {
	return m.Err
}

func (c *Copier) Copy(ctx context.Context, request *CopyObjectRequest, optFns ...func(*CopierOptions)) (*CopyResult, error) {
	// Copier wrapper
	delegate, err := c.newDelegate(ctx, request, optFns...)
	if err != nil {
		return nil, err
	}

	if err = delegate.checkSource(); err != nil {
		return nil, err
	}

	if err = delegate.applySource(); err != nil {
		return nil, err
	}

	return delegate.copy()
}

type copierDelegate struct {
	base    *Copier
	options CopierOptions
	context context.Context

	request *CopyObjectRequest

	// Source's Info
	metaProp *HeadObjectResult
	tagProp  *GetObjectTaggingResult

	sizeInBytes int64
	transferred int64
}

func (c *Copier) newDelegate(ctx context.Context, request *CopyObjectRequest, optFns ...func(*CopierOptions)) (*copierDelegate, error) {
	if request == nil {
		return nil, NewErrParamNull("request")
	}

	if request.Bucket == nil {
		return nil, NewErrParamNull("request.Bucket")
	}

	if request.Key == nil {
		return nil, NewErrParamNull("request.Key")
	}

	if request.SourceKey == nil {
		return nil, NewErrParamNull("request.SourceKey")
	}

	if request.MetadataDirective != nil && !isValidCopyDirective(*request.MetadataDirective) {
		return nil, NewErrParamInvalid("request.MetadataDirective")
	}

	if request.TaggingDirective != nil && !isValidCopyDirective(*request.TaggingDirective) {
		return nil, NewErrParamInvalid("request.TaggingDirective")
	}

	d := copierDelegate{
		base:    c,
		options: c.options,
		context: ctx,
		request: request,
	}

	for _, opt := range optFns {
		opt(&d.options)
	}

	if d.options.ParallelNum <= 0 {
		d.options.ParallelNum = DefaultCopyParallel
	}

	if d.options.PartSize <= 0 {
		d.options.PartSize = DefaultCopyPartSize
	}

	if d.options.MultipartCopyThreshold < 0 {
		d.options.MultipartCopyThreshold = DefaultCopyThreshold
	}

	d.tagProp = d.options.TagProperties
	d.metaProp = d.options.MetadataProperties

	return &d, nil
}

func (d *copierDelegate) checkSource() error {
	if d.metaProp != nil {
		return nil
	}

	var request HeadObjectRequest
	copyRequest(&request, d.request)
	if d.request.SourceBucket != nil {
		request.Bucket = d.request.SourceBucket
	}
	request.Key = d.request.SourceKey
	request.VersionId = d.request.SourceVersionId

	result, err := d.base.client.HeadObject(d.context, &request, d.options.ClientOptions...)
	if err != nil {
		return err
	}

	d.metaProp = result

	return nil
}

func (d *copierDelegate) applySource() error {

	d.sizeInBytes = d.metaProp.ContentLength

	// signle copy mode
	if d.sizeInBytes <= d.options.MultipartCopyThreshold {
		return nil
	}

	// multi part copy mode
	//Part Size
	partSize := d.options.PartSize
	if d.sizeInBytes > 0 {
		for d.sizeInBytes/partSize >= int64(MaxUploadParts) {
			partSize += d.options.PartSize
		}
	}
	d.options.PartSize = partSize

	return nil
}

func (d *copierDelegate) canUseShallowCopy() bool {
	if d.options.DisableShallowCopy {
		return false
	}

	// Change StorageClass
	if d.request.StorageClass != "" {
		return false
	}

	// Cross bucket
	if !d.options.NoCheckCrossBucket {
		if d.request.SourceBucket != nil &&
			ToString(d.request.SourceBucket) != ToString(d.request.Bucket) {
			return false
		}
	}

	// Decryption
	if !d.options.NoCheckSSE {
		if d.metaProp.Headers.Get(HeaderOssServerSideEncryption) != "" {
			return false
		}
	}

	return true
}

func (d *copierDelegate) copy() (*CopyResult, error) {
	if d.sizeInBytes <= d.options.MultipartCopyThreshold {
		return d.singleCopy()
	} else if d.canUseShallowCopy() {
		return d.shallowCopy()
	}
	return d.multiCopy()
}

func (d *copierDelegate) singleCopy() (*CopyResult, error) {
	result, err := d.base.client.CopyObject(d.context, d.request, d.options.ClientOptions...)

	if err != nil {
		return nil, d.wrapErr("", err)
	}

	// update
	d.transferred = d.sizeInBytes
	d.progressCallback(d.sizeInBytes)

	return &CopyResult{
		ETag:         result.ETag,
		HashCRC64:    result.HashCRC64,
		VersionId:    result.VersionId,
		ResultCommon: result.ResultCommon,
	}, nil
}

func (d *copierDelegate) shallowCopy() (*CopyResult, error) {
	// use signle copy first, if meets timeout, use multiCopy
	ctx, cancel := context.WithTimeout(d.context, 30*time.Second)
	defer cancel()
	result, err := d.base.client.CopyObject(ctx, d.request, d.options.ClientOptions...)

	if err != nil {
		if isContextError(ctx, &err) {
			return d.multiCopy()
		}
		return nil, d.wrapErr("", err)
	}

	// update
	d.transferred = d.sizeInBytes
	d.progressCallback(d.sizeInBytes)

	return &CopyResult{
		ETag:         result.ETag,
		HashCRC64:    result.HashCRC64,
		VersionId:    result.VersionId,
		ResultCommon: result.ResultCommon,
	}, nil
}

type copyChunk struct {
	partNum     int32
	size        int64
	sourceRange string
}

func (d *copierDelegate) multiCopy() (*CopyResult, error) {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		parts    UploadParts
		errValue atomic.Value
	)

	// Init the multipart
	imRequest, err := d.newInitiateMultipartUpload()
	if err != nil {
		return nil, d.wrapErr("", err)
	}

	initResult, err := d.base.client.InitiateMultipartUpload(d.context, imRequest, d.options.ClientOptions...)
	if err != nil {
		return nil, d.wrapErr("", err)
	}

	saveErrFn := func(e error) {
		errValue.Store(e)
	}

	getErrFn := func() error {
		v := errValue.Load()
		if v == nil {
			return nil
		}
		e, _ := v.(error)
		return e
	}

	// readChunk runs in worker goroutines to pull chunks off of the ch channel
	// timeout for MultiPartCopy API
	// 10s per 200M, max timeout is 50s
	const PART_SIZE int64 = 200 * 1024 * 1024
	const STEP time.Duration = 10 * time.Second
	mpcTimeout := transport.DefaultReadWriteTimeout
	partSize := d.options.PartSize
	for partSize > PART_SIZE {
		mpcTimeout += STEP
		partSize -= PART_SIZE
		if mpcTimeout > 50*time.Second {
			break
		}
	}
	mpcClientOptions := append(d.options.ClientOptions, OpReadWriteTimeout(mpcTimeout))

	readChunkFn := func(ch chan copyChunk) {
		defer wg.Done()
		for {
			data, ok := <-ch
			if !ok {
				break
			}
			if getErrFn() == nil {
				upResult, err := d.base.client.UploadPartCopy(
					d.context,
					&UploadPartCopyRequest{
						Bucket:          d.request.Bucket,
						Key:             d.request.Key,
						SourceBucket:    d.request.SourceBucket,
						SourceKey:       d.request.SourceKey,
						SourceVersionId: d.request.SourceVersionId,
						UploadId:        initResult.UploadId,
						PartNumber:      data.partNum,
						Range:           Ptr(data.sourceRange),
						RequestPayer:    d.request.RequestPayer,
					}, mpcClientOptions...)
				//fmt.Printf("UploadPart result: %#v, %#v\n", upResult, err)
				if err == nil {
					mu.Lock()
					parts = append(parts, UploadPart{ETag: upResult.ETag, PartNumber: data.partNum})
					d.transferred += data.size
					d.progressCallback(data.size)
					mu.Unlock()
				} else {
					saveErrFn(err)
				}
			}
		}
	}

	ch := make(chan copyChunk, d.options.ParallelNum)
	for i := 0; i < d.options.ParallelNum; i++ {
		wg.Add(1)
		go readChunkFn(ch)
	}

	// Read and queue the parts
	var (
		qnum      int32 = 0
		totalSize int64 = d.sizeInBytes
		readerPos int64 = 0
	)
	for getErrFn() == nil && readerPos < totalSize {
		n := d.options.PartSize
		bytesLeft := totalSize - readerPos
		if bytesLeft <= d.options.PartSize {
			n = bytesLeft
		}
		//fmt.Printf("send chunk: %d\n", qnum)
		qnum++
		ch <- copyChunk{partNum: qnum, size: n, sourceRange: fmt.Sprintf("bytes=%v-%v", readerPos, (readerPos + n - 1))}
		readerPos += n
	}

	// Close the channel, wait for workers
	close(ch)
	wg.Wait()

	// Complete upload
	var cmResult *CompleteMultipartUploadResult
	if err = getErrFn(); err == nil {
		sort.Sort(parts)
		cmRequest := &CompleteMultipartUploadRequest{}
		copyRequest(cmRequest, d.request)
		cmRequest.UploadId = initResult.UploadId
		cmRequest.CompleteMultipartUpload = &CompleteMultipartUpload{Parts: parts}
		cmResult, err = d.base.client.CompleteMultipartUpload(d.context, cmRequest, d.options.ClientOptions...)
	}
	//fmt.Printf("CompleteMultipartUpload cmResult: %#v, %#v\n", cmResult, err)

	if err != nil {
		//Abort
		if !d.options.LeavePartsOnError {
			amRequest := &AbortMultipartUploadRequest{}
			copyRequest(amRequest, d.request)
			amRequest.UploadId = initResult.UploadId
			_, _ = d.base.client.AbortMultipartUpload(d.context, amRequest, d.options.ClientOptions...)
		}
		return nil, d.wrapErr(*initResult.UploadId, err)
	}

	// check crc
	if cmResult.HashCRC64 != nil {
		srcCrc := d.metaProp.Headers.Get(HeaderOssCRC64)
		if srcCrc != "" {
			destCrc := ToString(cmResult.HashCRC64)
			if destCrc != srcCrc {
				return nil, d.wrapErr(*initResult.UploadId, fmt.Errorf("crc is inconsistent, source %s, destination %s", srcCrc, destCrc))
			}
		}
	}

	return &CopyResult{
		UploadId:     initResult.UploadId,
		ETag:         cmResult.ETag,
		VersionId:    cmResult.VersionId,
		HashCRC64:    cmResult.HashCRC64,
		ResultCommon: cmResult.ResultCommon,
	}, nil
}

func (d *copierDelegate) newInitiateMultipartUpload() (*InitiateMultipartUploadRequest, error) {
	var err error
	imRequest := &InitiateMultipartUploadRequest{}
	copyRequest(imRequest, d.request)
	imRequest.DisableAutoDetectMimeType = true

	if err = d.overwirteMetadataProp(imRequest); err != nil {
		return nil, err
	}

	if err = d.overwirteTagProp(imRequest); err != nil {
		return nil, err
	}

	return imRequest, nil
}

func (d *copierDelegate) overwirteMetadataProp(imRequest *InitiateMultipartUploadRequest) error {
	copyRequest := d.request
	switch strings.ToLower(ToString(copyRequest.MetadataDirective)) {
	case "", "copy":
		if d.metaProp == nil {
			return fmt.Errorf("request.MetadataDirective is COPY, but meets nil metaProp for source")
		}
		imRequest.CacheControl = nil
		imRequest.ContentType = nil
		imRequest.ContentDisposition = nil
		imRequest.ContentEncoding = nil
		imRequest.Expires = nil
		imRequest.Metadata = nil
		imRequest.Headers = map[string]string{}
		// skip meta in Headers
		for k, v := range d.request.Headers {
			lowK := strings.ToLower(k)
			if strings.HasPrefix(lowK, "x-oss-meta") {
				//skip
			} else if _, ok := metadataCopied[lowK]; ok {
				//skip
			} else {
				imRequest.Headers[k] = v
			}
		}
		// copy meta form source
		for k, v := range d.metaProp.Headers {
			lowK := strings.ToLower(k)
			if strings.HasPrefix(lowK, "x-oss-meta") {
				imRequest.Headers[lowK] = v[0]
			} else if _, ok := metadataCopied[lowK]; ok {
				imRequest.Headers[lowK] = v[0]
			}
		}
	case "replace":
		// the metedata has been copied via the copyRequest function before
	default:
		return fmt.Errorf("Unsupport MetadataDirective, %s", ToString(d.request.MetadataDirective))
	}

	return nil
}

func (d *copierDelegate) overwirteTagProp(imRequest *InitiateMultipartUploadRequest) error {
	switch strings.ToLower(ToString(d.request.TaggingDirective)) {
	case "", "copy":
		imRequest.Tagging = nil
		if d.metaProp.TaggingCount > 0 && d.tagProp == nil {
			request := &GetObjectTaggingRequest{}
			copyRequest(request, d.request)
			if d.request.SourceBucket != nil {
				request.Bucket = d.request.SourceBucket
			}
			request.Key = d.request.SourceKey
			request.VersionId = d.request.SourceVersionId
			result, err := d.base.client.GetObjectTagging(d.context, request, d.options.ClientOptions...)
			if err != nil {
				return err
			}
			d.tagProp = result
		}
		if d.tagProp != nil {
			var tags []string
			for _, t := range d.tagProp.Tags {
				tags = append(tags, fmt.Sprintf("%v=%v", ToString(t.Key), ToString(t.Value)))
			}
			if len(tags) > 0 {
				imRequest.Tagging = Ptr(strings.Join(tags, "&"))
			}
		}
	case "replace":
		// the tag has been copied via the copyRequest function before
	default:
		return fmt.Errorf("Unsupport TaggingDirective, %s", ToString(d.request.TaggingDirective))
	}

	return nil
}

func (d *copierDelegate) wrapErr(uploadId string, err error) error {
	return &CopyError{
		UploadId: uploadId,
		Path:     fmt.Sprintf("oss://%s/%s", *d.request.Bucket, *d.request.Key),
		Err:      err}
}

func (d *copierDelegate) progressCallback(increment int64) {
	if d.request.ProgressFn != nil {
		d.request.ProgressFn(increment, d.transferred, d.sizeInBytes)
	}
}
