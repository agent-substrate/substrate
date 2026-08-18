package oss

import (
	"bufio"
	"context"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

type DownloaderOptions struct {
	// The part size.
	PartSize int64

	// The number of download tasks in parallel.
	ParallelNum int

	// Specifies whether to record the download progress in the checkpoint file.
	EnableCheckpoint bool

	// The path in which the checkpoint file is stored. Example: /local/dir/.
	// This parameter is valid only if EnableCheckpoint is set to true.
	CheckpointDir string

	// Specifies whether to verify the CRC-64 of the downloaded object when the download is resumed.
	// By default, the CRC-64 is not verified.
	// This parameter is valid only if EnableCheckpoint is set to true.
	VerifyData bool

	// Specifies whether to use a temporary file when you download an object.
	// A temporary file is used by default.
	// The object is downloaded to the temporary file.
	// Then, the temporary file is renamed and uses the same name as the object that you want to download.
	UseTempFile bool

	// In memory buffer size for writing. Automatically align to 4K (4*1024 bytes).
	WriteBufferSize int

	ClientOptions []func(*Options)
}

type Downloader struct {
	options      DownloaderOptions
	client       DownloadAPIClient
	featureFlags FeatureFlagsType
}

// NewDownloader creates a new Downloader instance to downloads objects.
// Pass in additional functional options  to customize the downloader behavior.
func NewDownloader(c DownloadAPIClient, optFns ...func(*DownloaderOptions)) *Downloader {
	options := DownloaderOptions{
		PartSize:    DefaultDownloadPartSize,
		ParallelNum: DefaultDownloadParallel,
		UseTempFile: true,
	}

	for _, fn := range optFns {
		fn(&options)
	}

	u := &Downloader{
		client:  c,
		options: options,
	}

	//Get Client Feature
	switch t := c.(type) {
	case *Client:
		u.featureFlags = t.options.FeatureFlags
	case *EncryptionClient:
		u.featureFlags = (t.Unwrap().options.FeatureFlags & ^FeatureEnableCRC64CheckDownload)
	}

	return u
}

type DownloadResult struct {
	Written int64
}

type DownloadError struct {
	Err  error
	Path string
}

func (m *DownloadError) Error() string {
	var extra string
	if m.Err != nil {
		extra = fmt.Sprintf(", cause: %s", m.Err.Error())
	}
	return fmt.Sprintf("download failed %s", extra)
}

func (m *DownloadError) Unwrap() error {
	return m.Err
}

func (d *Downloader) DownloadFile(ctx context.Context, request *GetObjectRequest, filePath string, optFns ...func(*DownloaderOptions)) (result *DownloadResult, err error) {
	// Downloader wrapper
	delegate, err := d.newDelegate(ctx, request, optFns...)
	if err != nil {
		return nil, err
	}

	// Source
	if err = delegate.checkSource(); err != nil {
		return nil, err
	}

	// Destination
	var file *os.File
	if file, err = delegate.checkDestination(filePath); err != nil {
		return nil, err
	}

	// Range
	if err = delegate.adjustRange(); err != nil {
		return nil, err
	}

	// Checkpoint
	if err = delegate.checkCheckpoint(); err != nil {
		return nil, err
	}

	// truncate to the right position
	if err = delegate.adjustWriter(file); err != nil {
		return nil, err
	}

	// CRC Part
	delegate.updateCRCFlag()

	// download
	result, err = delegate.download()

	return result, delegate.closeWriter(file, err)
}

type downloaderDelegate struct {
	base    *Downloader
	options DownloaderOptions
	client  DownloadAPIClient
	context context.Context

	m sync.Mutex

	request *GetObjectRequest
	w       io.WriterAt
	rstart  int64
	pos     int64
	epos    int64
	written int64

	// Source's Info
	sizeInBytes int64
	etag        string
	modTime     string
	headers     http.Header

	//Destination's Info
	filePath     string
	tempFilePath string
	fileInfo     os.FileInfo

	//crc
	calcCRC  bool
	checkCRC bool

	checkpoint *downloadCheckpoint
}

type downloaderChunk struct {
	w      io.WriterAt
	start  int64
	size   int64
	cur    int64
	rstart int64 //range start
}

type downloadedChunk struct {
	start int64
	size  int64
	crc64 uint64
}

type downloadedChunks []downloadedChunk

func (slice downloadedChunks) Len() int {
	return len(slice)
}
func (slice downloadedChunks) Less(i, j int) bool {
	return slice[i].start < slice[j].start
}
func (slice downloadedChunks) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func (c *downloaderChunk) Write(p []byte) (n int, err error) {
	if c.cur >= c.size {
		return 0, io.EOF
	}

	n, err = c.w.WriteAt(p, c.start+c.cur-c.rstart)
	c.cur += int64(n)
	return
}

func (d *Downloader) newDelegate(ctx context.Context, request *GetObjectRequest, optFns ...func(*DownloaderOptions)) (*downloaderDelegate, error) {
	if request == nil {
		return nil, NewErrParamNull("request")
	}

	if !isValidBucketName(request.Bucket) {
		return nil, NewErrParamInvalid("request.Bucket")
	}

	if !isValidObjectName(request.Key) {
		return nil, NewErrParamInvalid("request.Key")
	}

	if request.Range != nil && !isValidRange(request.Range) {
		return nil, NewErrParamInvalid("request.Range")
	}

	delegate := downloaderDelegate{
		base:    d,
		options: d.options,
		client:  d.client,
		context: ctx,
		request: request,
	}

	for _, opt := range optFns {
		opt(&delegate.options)
	}

	if delegate.options.ParallelNum <= 0 {
		delegate.options.ParallelNum = DefaultDownloadParallel
	}
	if delegate.options.PartSize <= 0 {
		delegate.options.PartSize = DefaultDownloadPartSize
	}

	if delegate.options.WriteBufferSize > 0 {
		// align to 4K
		const alignSize = 4 * 1024
		delegate.options.WriteBufferSize = (delegate.options.WriteBufferSize + alignSize - 1) &^ (alignSize - 1)
	}

	return &delegate, nil
}

func (d *downloaderDelegate) checkSource() error {
	var request HeadObjectRequest
	copyRequest(&request, d.request)
	result, err := d.client.HeadObject(d.context, &request, d.options.ClientOptions...)
	if err != nil {
		return err
	}

	d.sizeInBytes = result.ContentLength
	d.modTime = result.Headers.Get(HTTPHeaderLastModified)
	d.etag = result.Headers.Get(HTTPHeaderETag)
	d.headers = result.Headers

	return nil
}

func (d *downloaderDelegate) checkDestination(filePath string) (*os.File, error) {
	if filePath == "" {
		return nil, NewErrParamInvalid("filePath")
	}
	absFilePath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, err
	}

	// use temporary file
	tempFilePath := absFilePath
	if d.options.UseTempFile {
		tempFilePath += TempFileSuffix
	}
	d.filePath = absFilePath
	d.tempFilePath = tempFilePath

	// use openfile to check the filepath is valid
	var file *os.File
	if file, err = os.OpenFile(tempFilePath, os.O_WRONLY|os.O_CREATE, FilePermMode); err != nil {
		return nil, err
	}

	if d.fileInfo, err = file.Stat(); err != nil {
		return nil, err
	}

	return file, nil
}

func (d *downloaderDelegate) adjustWriter(file *os.File) error {
	expectSize := d.epos - d.rstart
	if d.fileInfo != nil && d.fileInfo.Size() > expectSize {
		if err := file.Truncate(d.pos - d.rstart); err != nil {
			return err
		}
	}
	d.w = file
	return nil
}

func (d *downloaderDelegate) closeWriter(file *os.File, err error) error {
	if file != nil {
		file.Close()
	}

	if err != nil {
		if d.checkpoint == nil {
			os.Remove(d.tempFilePath)
		}
	} else {
		if d.tempFilePath != d.filePath {
			err = os.Rename(d.tempFilePath, d.filePath)
		}
		if err == nil && d.checkpoint != nil {
			d.checkpoint.remove()
		}
	}

	d.w = nil
	d.checkpoint = nil

	return err
}

func (d *downloaderDelegate) adjustRange() error {
	d.pos = 0
	d.rstart = 0
	d.epos = d.sizeInBytes
	if d.request.Range != nil {
		httpRange, _ := ParseRange(*d.request.Range)
		if httpRange.Offset >= d.sizeInBytes {
			return fmt.Errorf("invalid range, object size :%v, range: %v", d.sizeInBytes, ToString(d.request.Range))
		}
		d.pos = httpRange.Offset
		d.rstart = d.pos
		if httpRange.Count > 0 {
			d.epos = minInt64(httpRange.Offset+httpRange.Count, d.sizeInBytes)
		}
	}

	return nil
}

func (d *downloaderDelegate) checkCheckpoint() error {
	if d.options.EnableCheckpoint {
		d.checkpoint = newDownloadCheckpoint(d.request, d.tempFilePath, d.options.CheckpointDir, d.headers, d.options.PartSize)
		d.checkpoint.VerifyData = d.options.VerifyData
		if err := d.checkpoint.load(); err != nil {
			return err
		}

		if d.checkpoint.Loaded {
			d.pos = d.checkpoint.Info.Data.DownloadInfo.Offset
			d.written = d.pos - d.rstart
		} else {
			d.checkpoint.Info.Data.DownloadInfo.Offset = d.pos
		}
	}
	return nil
}

func (d *downloaderDelegate) updateCRCFlag() error {
	if (d.base.featureFlags & FeatureEnableCRC64CheckDownload) > 0 {
		d.checkCRC = d.request.Range == nil
		d.calcCRC = (d.checkpoint != nil && d.checkpoint.VerifyData) || d.checkCRC
	}
	return nil
}

func (d *downloaderDelegate) download() (*DownloadResult, error) {
	var (
		wg       sync.WaitGroup
		errValue atomic.Value
		cpCh     chan downloadedChunk
		cpWg     sync.WaitGroup
		cpChunks downloadedChunks
		tracker  bool   = d.calcCRC || d.checkpoint != nil
		tCRC64   uint64 = 0
	)

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

	// writeChunkFn runs in worker goroutines to pull chunks off of the ch channel
	writeChunkFn := func(ch chan downloaderChunk) {
		defer wg.Done()
		var hash hash.Hash64
		if d.calcCRC {
			hash = NewCRC64(0)
		}

		for {
			chunk, ok := <-ch
			if !ok {
				break
			}

			if getErrFn() != nil {
				continue
			}

			dchunk, derr := d.downloadChunk(chunk, hash)

			if derr != nil && derr != io.EOF {
				saveErrFn(derr)
			} else {
				// update tracker info
				if tracker {
					cpCh <- dchunk
				}
			}
		}
	}

	// trackerFn runs in worker goroutines to update checkpoint info or calc downloaded crc
	trackerFn := func(ch chan downloadedChunk) {
		defer cpWg.Done()
		var (
			tOffset int64 = 0
		)

		if d.checkpoint != nil {
			tOffset = d.checkpoint.Info.Data.DownloadInfo.Offset
			tCRC64 = d.checkpoint.Info.Data.DownloadInfo.CRC64
		}

		for {
			chunk, ok := <-ch
			if !ok {
				break
			}
			cpChunks = append(cpChunks, chunk)
			sort.Sort(cpChunks)
			newOffset := tOffset
			i := 0
			for ii := range cpChunks {
				if cpChunks[ii].start == newOffset {
					newOffset += cpChunks[ii].size
					i++
				} else {
					break
				}
			}
			if newOffset != tOffset {
				//remove updated chunk in cpChunks
				if d.calcCRC {
					tCRC64 = d.combineCRC(tCRC64, cpChunks[0:i])
				}
				tOffset = newOffset
				cpChunks = cpChunks[i:]
				if d.checkpoint != nil {
					d.checkpoint.Info.Data.DownloadInfo.Offset = tOffset
					d.checkpoint.Info.Data.DownloadInfo.CRC64 = tCRC64
					d.checkpoint.dump()
				}
			}
		}
	}

	// Start the download workers
	ch := make(chan downloaderChunk, d.options.ParallelNum)
	for i := 0; i < d.options.ParallelNum; i++ {
		wg.Add(1)
		go writeChunkFn(ch)
	}

	// Start tracker worker if need track downloaded chunk
	if tracker {
		cpCh = make(chan downloadedChunk, maxInt(3, d.options.ParallelNum))
		cpWg.Add(1)
		go trackerFn(cpCh)
	}

	// Consume downloaded data
	if d.request.ProgressFn != nil && d.written > 0 {
		d.request.ProgressFn(d.written, d.written, d.sizeInBytes)
	}

	// Queue the next range of bytes to read.
	for getErrFn() == nil {
		if d.pos >= d.epos {
			break
		}
		size := minInt64(d.epos-d.pos, d.options.PartSize)
		ch <- downloaderChunk{w: d.w, start: d.pos, size: size, rstart: d.rstart}
		d.pos += size
	}

	// Waiting for parts download finished
	close(ch)
	wg.Wait()

	if tracker {
		close(cpCh)
		cpWg.Wait()
	}

	if err := getErrFn(); err != nil {
		return nil, d.wrapErr(err)
	}

	if d.checkCRC {
		if len(cpChunks) > 0 {
			sort.Sort(cpChunks)
		}
		if derr := checkResponseHeaderCRC64(fmt.Sprint(d.combineCRC(tCRC64, cpChunks)), d.headers); derr != nil {
			return nil, d.wrapErr(derr)
		}
	}

	return &DownloadResult{
		Written: d.written,
	}, nil
}

func (d *downloaderDelegate) incrWritten(n int64) {
	d.m.Lock()
	defer d.m.Unlock()
	d.written += n
	if d.request.ProgressFn != nil && n > 0 {
		d.request.ProgressFn(n, d.written, d.sizeInBytes)
	}
}

func (d *downloaderDelegate) downloadChunk(chunk downloaderChunk, hash hash.Hash64) (downloadedChunk, error) {
	// Get the next byte range of data
	var request GetObjectRequest
	copyRequest(&request, d.request)

	getFn := func(ctx context.Context, httpRange HTTPRange) (output *ReaderRangeGetOutput, err error) {
		// update range
		request.Range = nil
		rangeStr := httpRange.FormatHTTPRange()
		request.RangeBehavior = nil
		if rangeStr != nil {
			request.Range = rangeStr
			request.RangeBehavior = Ptr("standard")
		}

		result, err := d.client.GetObject(ctx, &request, d.options.ClientOptions...)
		if err != nil {
			return nil, err
		}

		return &ReaderRangeGetOutput{
			Body:          result.Body,
			ETag:          result.ETag,
			ContentLength: result.ContentLength,
			ContentRange:  result.ContentRange,
		}, nil
	}

	reader, _ := NewRangeReader(d.context, getFn, &HTTPRange{chunk.start, chunk.size}, d.etag)
	defer reader.Close()

	var (
		r     io.Reader = reader
		crc64 uint64    = 0
	)
	if hash != nil {
		hash.Reset()
		r = io.TeeReader(reader, hash)
	}

	var writer io.Writer = &chunk

	if d.options.WriteBufferSize > 0 {
		writer = bufio.NewWriterSize(writer, d.options.WriteBufferSize)
	}

	n, err := io.Copy(writer, r)
	d.incrWritten(n)

	if hash != nil {
		crc64 = hash.Sum64()
	}

	// if buffering, flush to disk
	if err == nil {
		switch w := writer.(type) {
		case *bufio.Writer:
			err = w.Flush()
		}
	}

	return downloadedChunk{
		start: chunk.start,
		size:  n,
		crc64: crc64,
	}, err
}

func (u *downloaderDelegate) combineCRC(hashCRC uint64, crcs downloadedChunks) uint64 {
	if len(crcs) == 0 {
		return hashCRC
	}
	crc := hashCRC
	for _, c := range crcs {
		crc = CRC64Combine(crc, c.crc64, uint64(c.size))
	}
	return crc
}

func (u *downloaderDelegate) wrapErr(err error) error {
	return &DownloadError{
		Path: fmt.Sprintf("oss://%s/%s", *u.request.Bucket, *u.request.Key),
		Err:  err}
}
