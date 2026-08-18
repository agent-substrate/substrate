package oss

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

type UploaderOptions struct {
	PartSize int64

	ParallelNum int

	LeavePartsOnError bool

	EnableCheckpoint bool

	CheckpointDir string

	ClientOptions []func(*Options)
}

type Uploader struct {
	options            UploaderOptions
	client             UploadAPIClient
	featureFlags       FeatureFlagsType
	isEncryptionClient bool
}

// NewUploader creates a new Uploader instance to upload objects.
// Pass In additional functional options to customize the uploader's behavior.
func NewUploader(c UploadAPIClient, optFns ...func(*UploaderOptions)) *Uploader {
	options := UploaderOptions{
		PartSize:          DefaultUploadPartSize,
		ParallelNum:       DefaultUploadParallel,
		LeavePartsOnError: false,
	}

	for _, fn := range optFns {
		fn(&options)
	}

	u := &Uploader{
		client:             c,
		options:            options,
		isEncryptionClient: false,
	}

	//Get Client Feature
	switch t := c.(type) {
	case *Client:
		u.featureFlags = t.options.FeatureFlags
	case *EncryptionClient:
		u.featureFlags = t.Unwrap().options.FeatureFlags
		u.isEncryptionClient = true
	}

	return u
}

type UploadResult struct {
	UploadId *string

	ETag *string

	VersionId *string

	HashCRC64 *string

	ResultCommon
}

type UploadError struct {
	Err      error
	UploadId string
	Path     string
}

func (m *UploadError) Error() string {
	var extra string
	if m.Err != nil {
		extra = fmt.Sprintf(", cause: %s", m.Err.Error())
	}
	return fmt.Sprintf("upload failed, upload id: %s%s", m.UploadId, extra)
}

func (m *UploadError) Unwrap() error {
	return m.Err
}

func (u *Uploader) UploadFrom(ctx context.Context, request *PutObjectRequest, body io.Reader, optFns ...func(*UploaderOptions)) (*UploadResult, error) {
	// Uploader wrapper
	delegate, err := u.newDelegate(ctx, request, optFns...)
	if err != nil {
		return nil, err
	}

	delegate.body = body
	if err = delegate.applySource(); err != nil {
		return nil, err
	}

	return delegate.upload()
}

func (u *Uploader) UploadFile(ctx context.Context, request *PutObjectRequest, filePath string, optFns ...func(*UploaderOptions)) (*UploadResult, error) {
	// Uploader wrapper
	delegate, err := u.newDelegate(ctx, request, optFns...)
	if err != nil {
		return nil, err
	}

	// Source
	if err = delegate.checkSource(filePath); err != nil {
		return nil, err
	}

	var file *os.File
	if file, err = delegate.openReader(); err != nil {
		return nil, err
	}
	delegate.body = file

	if err = delegate.applySource(); err != nil {
		return nil, err
	}

	if err = delegate.checkCheckpoint(); err != nil {
		return nil, err
	}

	if err = delegate.adjustSource(); err != nil {
		return nil, err
	}

	result, err := delegate.upload()

	return result, delegate.closeReader(file, err)
}

type uploaderDelegate struct {
	base    *Uploader
	options UploaderOptions
	client  UploadAPIClient
	context context.Context
	request *PutObjectRequest

	body        io.Reader
	readerPos   int64
	totalSize   int64
	hashCRC64   uint64
	transferred int64

	// Source's Info, from file or reader
	filePath string
	fileInfo os.FileInfo

	// for resumable upload
	uploadId      string
	partNumber    int32
	cseContext    *EncryptionMultiPartContext
	uploadedParts []Part

	partPool byteSlicePool

	checkpoint *uploadCheckpoint
}

type uploadIdInfo struct {
	uploadId   string
	startNum   int32
	cseContext *EncryptionMultiPartContext
}

func (u *Uploader) newDelegate(ctx context.Context, request *PutObjectRequest, optFns ...func(*UploaderOptions)) (*uploaderDelegate, error) {
	if request == nil {
		return nil, NewErrParamNull("request")
	}

	if request.Bucket == nil {
		return nil, NewErrParamNull("request.Bucket")
	}

	if request.Key == nil {
		return nil, NewErrParamNull("request.Key")
	}

	d := uploaderDelegate{
		base:    u,
		options: u.options,
		client:  u.client,
		context: ctx,
		request: request,
	}

	for _, opt := range optFns {
		opt(&d.options)
	}

	if d.options.ParallelNum <= 0 {
		d.options.ParallelNum = DefaultUploadParallel
	}
	if d.options.PartSize <= 0 {
		d.options.PartSize = DefaultUploadPartSize
	}

	if _, ok := d.request.Parameters["sequential"]; ok {
		d.options.ParallelNum = 1
	}

	return &d, nil
}

func (u *uploaderDelegate) checkSource(filePath string) error {
	if filePath == "" {
		return NewErrParamRequired("filePath")
	}

	// if !FileExists(filePath) {
	// 	return fmt.Errorf("File not exists, %v", filePath)
	// }

	info, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("File not exists, %v", filePath)
		}
		return err
	}

	u.filePath = filePath
	u.fileInfo = info

	return nil
}

func (u *uploaderDelegate) applySource() error {
	if u.body == nil {
		return NewErrParamNull("the body is null")
	}

	totalSize := GetReaderLen(u.body)

	//Part Size
	partSize := u.options.PartSize
	if totalSize > 0 {
		for totalSize/partSize >= int64(MaxUploadParts) {
			partSize += u.options.PartSize
		}
	}

	u.totalSize = totalSize
	u.options.PartSize = partSize

	return nil
}

func (u *uploaderDelegate) adjustSource() error {
	// resume from upload id
	if u.uploadId != "" {
		// if the body supports seek
		r, ok := u.body.(io.Seeker)
		// not support
		if !ok {
			u.uploadId = ""
			return nil
		}

		// if upload id is valid
		paginator := NewListPartsPaginator(u.client, &ListPartsRequest{
			Bucket:   u.request.Bucket,
			Key:      u.request.Key,
			UploadId: Ptr(u.uploadId),
		})

		// find consecutive sequence from min part number
		var (
			checkPartNumber int32  = 1
			updateCRC64     bool   = ((u.base.featureFlags & FeatureEnableCRC64CheckUpload) > 0)
			hashCRC64       uint64 = 0
			page            *ListPartsResult
			err             error
			uploadedParts   []Part
		)
	outerLoop:

		for paginator.HasNext() {
			page, err = paginator.NextPage(u.context, u.options.ClientOptions...)
			if err != nil {
				u.uploadId = ""
				return nil
			}
			for _, p := range page.Parts {
				if p.PartNumber != checkPartNumber ||
					p.Size != u.options.PartSize {
					break outerLoop
				}
				checkPartNumber++
				uploadedParts = append(uploadedParts, p)
				if updateCRC64 && p.HashCRC64 != nil {
					value, _ := strconv.ParseUint(ToString(p.HashCRC64), 10, 64)
					hashCRC64 = CRC64Combine(hashCRC64, value, uint64(p.Size))
				}
			}
		}

		partNumber := checkPartNumber - 1
		newOffset := int64(partNumber) * u.options.PartSize
		if _, err := r.Seek(newOffset, io.SeekStart); err != nil {
			u.uploadId = ""
			return nil
		}

		cseContext, err := u.resumeCSEContext(page)
		if err != nil {
			u.uploadId = ""
			return nil
		}

		u.partNumber = partNumber
		u.readerPos = newOffset
		u.hashCRC64 = hashCRC64
		u.cseContext = cseContext
		u.uploadedParts = uploadedParts
	}
	return nil
}

func (d *uploaderDelegate) checkCheckpoint() error {
	if d.options.EnableCheckpoint {
		d.checkpoint = newUploadCheckpoint(d.request, d.filePath, d.options.CheckpointDir, d.fileInfo, d.options.PartSize)
		if err := d.checkpoint.load(); err != nil {
			return err
		}

		if d.checkpoint.Loaded {
			d.uploadId = d.checkpoint.Info.Data.UploadInfo.UploadId
		}
		d.options.LeavePartsOnError = true
	}
	return nil
}

func (d *uploaderDelegate) openReader() (*os.File, error) {
	file, err := os.Open(d.filePath)
	if err != nil {
		return nil, err
	}

	d.body = file
	return file, nil
}

func (d *uploaderDelegate) closeReader(file *os.File, err error) error {
	if file != nil {
		file.Close()
	}

	if d.checkpoint != nil && err == nil {
		d.checkpoint.remove()
	}

	d.body = nil
	d.checkpoint = nil

	return err
}

func (d *uploaderDelegate) resumeCSEContext(result *ListPartsResult) (*EncryptionMultiPartContext, error) {
	if !d.base.isEncryptionClient {
		return nil, nil
	}
	sc, ok := d.client.(*EncryptionClient)
	if !ok {
		return nil, fmt.Errorf("Not EncryptionClient")
	}

	envelope, err := getEnvelopeFromListParts(result)
	if err != nil {
		return nil, err
	}

	cc, err := sc.defualtCCBuilder.ContentCipherEnv(envelope)
	if err != nil {
		return nil, err
	}

	cseContext := &EncryptionMultiPartContext{
		ContentCipher: cc,
		PartSize:      ToInt64(result.ClientEncryptionPartSize),
		DataSize:      ToInt64(result.ClientEncryptionDataSize),
	}

	if !cseContext.Valid() {
		return nil, fmt.Errorf("EncryptionMultiPartContext is invalid")
	}

	return cseContext, nil
}

func (u *uploaderDelegate) upload() (*UploadResult, error) {
	if u.totalSize >= 0 && u.totalSize < u.options.PartSize {
		return u.singlePart()
	}
	return u.multiPart()
}

func (u *uploaderDelegate) singlePart() (*UploadResult, error) {
	request := &PutObjectRequest{}
	copyRequest(request, u.request)
	request.Body = u.body
	if request.ContentType == nil {
		request.ContentType = u.getContentType()
	}

	result, err := u.client.PutObject(u.context, request, u.options.ClientOptions...)

	if err != nil {
		return nil, u.wrapErr("", err)
	}

	return &UploadResult{
		ETag:         result.ETag,
		VersionId:    result.VersionId,
		HashCRC64:    result.HashCRC64,
		ResultCommon: result.ResultCommon,
	}, nil
}

func (u *uploaderDelegate) nextReader() (io.ReadSeeker, int, func(), error) {
	type readerAtSeeker interface {
		io.ReaderAt
		io.ReadSeeker
	}
	switch r := u.body.(type) {
	case readerAtSeeker:
		var err error

		n := u.options.PartSize
		if u.totalSize >= 0 {
			bytesLeft := u.totalSize - u.readerPos
			if bytesLeft <= u.options.PartSize {
				err = io.EOF
				n = bytesLeft
			}
		}

		reader := io.NewSectionReader(r, u.readerPos, n)
		cleanup := func() {}

		u.readerPos += n

		return reader, int(n), cleanup, err

	default:
		if u.partPool == nil {
			u.partPool = newByteSlicePool(u.options.PartSize)
			u.partPool.ModifyCapacity(u.options.ParallelNum + 1)
		}

		part, err := u.partPool.Get(u.context)
		if err != nil {
			return nil, 0, func() {}, err
		}

		n, err := readFill(r, *part)
		u.readerPos += int64(n)

		cleanup := func() {
			u.partPool.Put(part)
		}

		return bytes.NewReader((*part)[0:n]), n, cleanup, err
	}
}

type uploaderChunk struct {
	partNum int32
	size    int
	body    io.ReadSeeker
	cleanup func()
}

type uploadPartCRC struct {
	partNumber int32
	size       int
	hashCRC64  *string
}

type uploadPartCRCs []uploadPartCRC

func (slice uploadPartCRCs) Len() int {
	return len(slice)
}
func (slice uploadPartCRCs) Less(i, j int) bool {
	return slice[i].partNumber < slice[j].partNumber
}
func (slice uploadPartCRCs) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

type saveErr struct {
	Err error
}

func (e saveErr) Error() string {
	return fmt.Sprintf("saveErr: %v", e.Err)
}

func (e saveErr) Unwrap() error { return e.Err }

func (u *uploaderDelegate) multiPart() (*UploadResult, error) {
	release := func() {
		if u.partPool != nil {
			u.partPool.Close()
		}
	}
	defer release()

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		parts     UploadParts
		errValue  atomic.Value
		crcParts  uploadPartCRCs
		enableCRC = (u.base.featureFlags & FeatureEnableCRC64CheckUpload) > 0
	)

	// Init the multipart
	uploadIdInfo, err := u.getUploadId()
	if err != nil {
		return nil, u.wrapErr("", err)
	}
	//fmt.Printf("getUploadId result: %v, %#v\n", uploadId, err)
	uploadId := uploadIdInfo.uploadId
	startPartNum := uploadIdInfo.startNum

	// Update Checkpoint
	if u.checkpoint != nil {
		u.checkpoint.Info.Data.UploadInfo.UploadId = uploadId
		u.checkpoint.dump()
	}

	saveErrFn := func(e error) {
		if e == nil {
			return
		}
		errValue.Store(saveErr{Err: e})
	}

	getErrFn := func() error {
		v := errValue.Load()
		if v == nil {
			return nil
		}
		e, _ := v.(saveErr)
		return e.Unwrap()
	}

	// readChunk runs in worker goroutines to pull chunks off of the ch channel
	readChunkFn := func(ch chan uploaderChunk) {
		defer wg.Done()
		for {
			data, ok := <-ch
			if !ok {
				break
			}

			if getErrFn() == nil {
				upResult, err := u.client.UploadPart(
					u.context,
					&UploadPartRequest{
						Bucket:              u.request.Bucket,
						Key:                 u.request.Key,
						UploadId:            Ptr(uploadId),
						PartNumber:          data.partNum,
						Body:                data.body,
						CSEMultiPartContext: uploadIdInfo.cseContext,
						RequestPayer:        u.request.RequestPayer,
					},
					u.options.ClientOptions...)
				//fmt.Printf("UploadPart result: %#v, %#v\n", upResult, err)

				if err == nil {
					mu.Lock()
					parts = append(parts, UploadPart{ETag: upResult.ETag, PartNumber: data.partNum})
					if enableCRC {
						crcParts = append(crcParts,
							uploadPartCRC{partNumber: data.partNum, hashCRC64: upResult.HashCRC64, size: data.size})
					}
					if u.request.ProgressFn != nil {
						u.transferred += int64(data.size)
						u.request.ProgressFn(int64(data.size), u.transferred, u.totalSize)
					}
					mu.Unlock()
				} else {
					saveErrFn(err)
				}
			}
			data.cleanup()
		}
	}

	ch := make(chan uploaderChunk, u.options.ParallelNum)
	for i := 0; i < u.options.ParallelNum; i++ {
		wg.Add(1)
		go readChunkFn(ch)
	}

	// Read and queue the parts
	var (
		qnum int32 = startPartNum
		qerr error = nil
	)

	// consume uploaded parts
	if u.readerPos > 0 {
		for _, p := range u.uploadedParts {
			parts = append(parts, UploadPart{PartNumber: p.PartNumber, ETag: p.ETag})
		}
		if u.request.ProgressFn != nil {
			u.transferred = u.readerPos
			u.request.ProgressFn(u.readerPos, u.transferred, u.totalSize)
		}
	}

	for getErrFn() == nil && qerr == nil {
		var (
			reader       io.ReadSeeker
			nextChunkLen int
			cleanup      func()
		)

		reader, nextChunkLen, cleanup, qerr = u.nextReader()
		// check err
		if qerr != nil && qerr != io.EOF {
			cleanup()
			saveErrFn(qerr)
			break
		}

		// No need to upload empty part
		if nextChunkLen == 0 {
			cleanup()
			break
		}

		qnum++
		//fmt.Printf("send chunk: %d\n", qnum)
		ch <- uploaderChunk{body: reader, partNum: qnum, cleanup: cleanup, size: nextChunkLen}
	}

	// Close the channel, wait for workers
	close(ch)
	wg.Wait()

	// Complete upload
	var cmResult *CompleteMultipartUploadResult
	if err = getErrFn(); err == nil {
		sort.Sort(parts)
		cmRequest := &CompleteMultipartUploadRequest{}
		copyRequest(cmRequest, u.request)
		cmRequest.UploadId = Ptr(uploadId)
		cmRequest.CompleteMultipartUpload = &CompleteMultipartUpload{Parts: parts}
		cmResult, err = u.client.CompleteMultipartUpload(u.context, cmRequest, u.options.ClientOptions...)
	}
	//fmt.Printf("CompleteMultipartUpload cmResult: %#v, %#v\n", cmResult, err)

	if err != nil {
		//Abort
		if !u.options.LeavePartsOnError {
			abortRequest := &AbortMultipartUploadRequest{}
			copyRequest(abortRequest, u.request)
			abortRequest.UploadId = Ptr(uploadId)
			_, _ = u.client.AbortMultipartUpload(u.context, abortRequest, u.options.ClientOptions...)
		}
		return nil, u.wrapErr(uploadId, err)
	}

	if enableCRC {
		caclCRC := fmt.Sprint(u.combineCRC(crcParts))
		if err = checkResponseHeaderCRC64(caclCRC, cmResult.Headers); err != nil {
			return nil, u.wrapErr(uploadId, err)
		}
	}

	return &UploadResult{
		UploadId:     Ptr(uploadId),
		ETag:         cmResult.ETag,
		VersionId:    cmResult.VersionId,
		HashCRC64:    cmResult.HashCRC64,
		ResultCommon: cmResult.ResultCommon,
	}, nil
}

func (u *uploaderDelegate) getUploadId() (info uploadIdInfo, err error) {
	if u.uploadId != "" {
		return uploadIdInfo{
			uploadId:   u.uploadId,
			startNum:   u.partNumber,
			cseContext: u.cseContext,
		}, nil
	}

	// if not exist or fail, create a new upload id
	request := &InitiateMultipartUploadRequest{}
	copyRequest(request, u.request)
	if request.ContentType == nil {
		request.ContentType = u.getContentType()
	}

	if u.base.isEncryptionClient {
		request.CSEPartSize = &u.options.PartSize
		request.CSEDataSize = &u.totalSize
	}

	result, err := u.client.InitiateMultipartUpload(u.context, request, u.options.ClientOptions...)
	if err != nil {
		return info, err
	}

	return uploadIdInfo{
		uploadId:   *result.UploadId,
		startNum:   0,
		cseContext: result.CSEMultiPartContext,
	}, nil
}

func (u *uploaderDelegate) getContentType() *string {
	if u.filePath != "" {
		if contentType := TypeByExtension(u.filePath); contentType != "" {
			return Ptr(contentType)
		}
	}
	return nil
}

func (u *uploaderDelegate) wrapErr(uploadId string, err error) error {
	return &UploadError{
		UploadId: uploadId,
		Path:     fmt.Sprintf("oss://%s/%s", *u.request.Bucket, *u.request.Key),
		Err:      err}
}

func (u *uploaderDelegate) combineCRC(crcs uploadPartCRCs) uint64 {
	if len(crcs) == 0 {
		return 0
	}
	sort.Sort(crcs)
	crc := u.hashCRC64
	for _, c := range crcs {
		if c.hashCRC64 == nil {
			return 0
		}
		if value, err := strconv.ParseUint(*c.hashCRC64, 10, 64); err == nil {
			crc = CRC64Combine(crc, value, uint64(c.size))
		} else {
			break
		}
	}
	return crc
}
