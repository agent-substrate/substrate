package oss

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"
)

type OpenOptions struct {
	Offset int64

	VersionId *string

	EnablePrefetch bool
	PrefetchNum    int
	ChunkSize      int64

	PrefetchThreshold int64
	RequestPayer      *string

	OutOfOrderReadThreshold int64
}

type ReadOnlyFile struct {
	client  OpenFileAPIClient
	context context.Context

	// object info
	bucket       string
	key          string
	versionId    *string
	requestPayer *string

	// file info
	sizeInBytes int64
	modTime     string
	etag        string
	headers     http.Header

	// current read position
	offset int64

	// read
	reader        io.ReadCloser
	readBufOffset int64

	// prefetch
	enablePrefetch    bool
	chunkSize         int64
	prefetchNum       int
	prefetchThreshold int64

	asyncReaders  []*AsyncRangeReader
	seqReadAmount int64 // number of sequential read
	numOOORead    int64 // number of out of order read

	closed bool // whether we have closed the file

	oooReadThreshold int64
}

// NewReadOnlyFile OpenFile opens the named file for reading.
// If successful, methods on the returned file can be used for reading.
func NewReadOnlyFile(ctx context.Context, c OpenFileAPIClient, bucket string, key string, optFns ...func(*OpenOptions)) (*ReadOnlyFile, error) {
	options := OpenOptions{
		Offset:                  0,
		EnablePrefetch:          false,
		PrefetchNum:             DefaultPrefetchNum,
		ChunkSize:               DefaultPrefetchChunkSize,
		PrefetchThreshold:       DefaultPrefetchThreshold,
		OutOfOrderReadThreshold: DefaultOutOfOrderReadThreshold,
	}

	for _, fn := range optFns {
		fn(&options)
	}

	if options.EnablePrefetch {
		var chunkSize int64
		if options.ChunkSize > 0 {
			chunkSize = (options.ChunkSize + AsyncReadeBufferSize - 1) / AsyncReadeBufferSize * AsyncReadeBufferSize
		} else {
			chunkSize = DefaultPrefetchChunkSize
		}
		options.ChunkSize = chunkSize

		if options.PrefetchNum <= 0 {
			options.PrefetchNum = DefaultPrefetchNum
		}

		if options.OutOfOrderReadThreshold <= int64(0) {
			options.OutOfOrderReadThreshold = DefaultOutOfOrderReadThreshold
		}
	}

	f := &ReadOnlyFile{
		client:  c,
		context: ctx,

		bucket:       bucket,
		key:          key,
		versionId:    options.VersionId,
		requestPayer: options.RequestPayer,

		offset: options.Offset,

		enablePrefetch:    options.EnablePrefetch,
		prefetchNum:       options.PrefetchNum,
		chunkSize:         options.ChunkSize,
		prefetchThreshold: options.PrefetchThreshold,
		oooReadThreshold:  options.OutOfOrderReadThreshold,
	}

	result, err := f.client.HeadObject(f.context, &HeadObjectRequest{
		Bucket:       &f.bucket,
		Key:          &f.key,
		VersionId:    f.versionId,
		RequestPayer: f.requestPayer,
	})

	if err != nil {
		return nil, err
	}

	//File info
	f.sizeInBytes = result.ContentLength
	f.modTime = result.Headers.Get(HTTPHeaderLastModified)
	f.etag = result.Headers.Get(HTTPHeaderETag)
	f.headers = result.Headers

	if f.sizeInBytes < 0 {
		return nil, fmt.Errorf("file size is invaid, got %v", f.sizeInBytes)
	}

	if f.offset > f.sizeInBytes {
		return nil, fmt.Errorf("offset is unavailable, offset:%v, file size:%v", f.offset, f.sizeInBytes)
	}

	return f, nil
}

// Close closes the File.
func (f *ReadOnlyFile) Close() error {
	if f == nil {
		return os.ErrInvalid
	}
	return f.close()
}

func (f *ReadOnlyFile) close() error {
	if f.closed {
		return nil
	}

	if f.reader != nil {
		f.reader.Close()
		f.reader = nil
	}
	for _, reader := range f.asyncReaders {
		reader.Close()
	}
	f.asyncReaders = nil

	f.closed = true
	runtime.SetFinalizer(f, nil)
	return nil
}

// Read reads up to len(b) bytes from the File and stores them in b.
// It returns the number of bytes read and any error encountered.
// At end of file, Read returns 0, io.EOF.
func (f *ReadOnlyFile) Read(p []byte) (bytesRead int, err error) {
	if err := f.checkValid("read"); err != nil {
		return 0, err
	}
	n, e := f.read(p)
	return n, f.wrapErr("read", e)
}

func (f *ReadOnlyFile) read(p []byte) (bytesRead int, err error) {
	defer func() {
		f.offset += int64(bytesRead)
	}()
	nwant := len(p)
	var nread int
	for bytesRead < nwant && err == nil {
		nread, err = f.readInternal(f.offset+int64(bytesRead), p[bytesRead:])
		if nread > 0 {
			bytesRead += nread
		}
	}
	return
}

// Seek sets the offset for the next Read or Write on file to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error.
func (f *ReadOnlyFile) Seek(offset int64, whence int) (int64, error) {
	if err := f.checkValid("seek"); err != nil {
		return 0, err
	}
	r, e := f.seek(offset, whence)
	if e != nil {
		return 0, f.wrapErr("seek", e)
	}
	return r, nil
}

func (f *ReadOnlyFile) seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.offset + offset
	case io.SeekEnd:
		abs = f.sizeInBytes + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}
	if abs < 0 {
		return 0, fmt.Errorf("negative position")
	}
	if abs > f.sizeInBytes {
		return offset - (abs - f.sizeInBytes), fmt.Errorf("offset is unavailable")
	}

	f.offset = abs

	return abs, nil
}

type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
	header  http.Header
}

func (fi *fileInfo) Name() string       { return fi.name }
func (fi *fileInfo) Size() int64        { return fi.size }
func (fi *fileInfo) Mode() os.FileMode  { return os.FileMode(0644) }
func (fi *fileInfo) ModTime() time.Time { return fi.modTime }
func (fi *fileInfo) IsDir() bool        { return false }
func (fi *fileInfo) Sys() any           { return fi.header }

// Stat returns the FileInfo structure describing file.
func (f *ReadOnlyFile) Stat() (os.FileInfo, error) {
	if err := f.checkValid("stat"); err != nil {
		return nil, err
	}
	mtime, _ := http.ParseTime(f.modTime)
	return &fileInfo{
		name:    f.name(),
		size:    f.sizeInBytes,
		modTime: mtime,
		header:  f.headers,
	}, nil
}

func (f *ReadOnlyFile) name() string {
	var name string
	if f.versionId != nil {
		name = fmt.Sprintf("oss://%s/%s?versionId=%s", f.bucket, f.key, *f.versionId)
	} else {
		name = fmt.Sprintf("oss://%s/%s", f.bucket, f.key)
	}
	return name
}

func (f *ReadOnlyFile) wrapErr(op string, err error) error {
	if err == nil || err == io.EOF {
		return err
	}
	return &os.PathError{Op: op, Path: f.name(), Err: err}
}

func (f *ReadOnlyFile) checkValid(_ string) error {
	if f == nil {
		return os.ErrInvalid
	} else if f.closed {
		return os.ErrClosed
	}
	return nil
}

func (f *ReadOnlyFile) readInternal(offset int64, p []byte) (bytesRead int, err error) {
	defer func() {
		if bytesRead > 0 {
			f.readBufOffset += int64(bytesRead)
			f.seqReadAmount += int64(bytesRead)
		}
	}()

	if offset >= f.sizeInBytes {
		err = io.EOF
		return
	}

	if f.readBufOffset != offset {
		f.readBufOffset = offset
		f.seqReadAmount = 0

		if f.reader != nil {
			f.reader.Close()
			f.reader = nil
		}

		if f.asyncReaders != nil {
			f.numOOORead++
		}

		for _, ar := range f.asyncReaders {
			ar.Close()
		}
		f.asyncReaders = nil
	}

	if f.enablePrefetch && f.seqReadAmount >= f.prefetchThreshold && f.numOOORead < f.oooReadThreshold {
		//swith to async reader
		if f.reader != nil {
			f.reader.Close()
			f.reader = nil
		}

		err = f.prefetch(offset, len(p))
		if err == nil {
			bytesRead, err = f.readFromPrefetcher(offset, p)
			if err == nil {
				return
			}
		}

		// fall back to read serially
		f.seqReadAmount = 0
		for _, ar := range f.asyncReaders {
			ar.Close()
		}
		f.asyncReaders = nil
	}

	bytesRead, err = f.readDirect(offset, p)
	return
}

func (f *ReadOnlyFile) readDirect(offset int64, buf []byte) (bytesRead int, err error) {
	if offset >= f.sizeInBytes {
		return
	}

	if f.reader == nil {
		var result *GetObjectResult
		result, err = f.client.GetObject(f.context, &GetObjectRequest{
			Bucket:        Ptr(f.bucket),
			Key:           Ptr(f.key),
			VersionId:     f.versionId,
			Range:         Ptr(fmt.Sprintf("bytes=%d-", offset)),
			RangeBehavior: Ptr("standard"),
			RequestPayer:  f.requestPayer,
		})
		if err != nil {
			return bytesRead, err
		}

		if err = f.checkResultValid(offset, result.Headers); err != nil {
			if result != nil {
				result.Body.Close()
			}
			return bytesRead, err
		}

		f.reader = result.Body
	}

	bytesRead, err = f.reader.Read(buf)
	if err != nil {
		f.reader.Close()
		f.reader = nil
		err = nil
	}

	return
}

func (f *ReadOnlyFile) checkResultValid(offset int64, header http.Header) error {
	modTime := header.Get(HTTPHeaderLastModified)
	etag := header.Get(HTTPHeaderETag)
	gotOffset, _ := parseOffsetAndSizeFromHeaders(header)
	if gotOffset != offset {
		return fmt.Errorf("Range get fail, expect offset:%v, got offset:%v", offset, gotOffset)
	}

	if (modTime != "" && f.modTime != "" && modTime != f.modTime) ||
		(etag != "" && f.etag != "" && etag != f.etag) {
		return fmt.Errorf("Source file is changed, origin info [%v,%v], new info [%v,%v]",
			f.modTime, f.etag, modTime, etag)
	}

	return nil
}

func (f *ReadOnlyFile) readFromPrefetcher(offset int64, buf []byte) (bytesRead int, err error) {
	var nread int
	for len(f.asyncReaders) != 0 {
		asyncReader := f.asyncReaders[0]
		//check offset
		if offset != asyncReader.Offset() {
			return 0, errors.New("out of order")
		}
		nread, err = asyncReader.Read(buf)
		bytesRead += nread
		if err != nil {
			if err == io.EOF {
				//fmt.Printf("asyncReader done\n")
				asyncReader.Close()
				f.asyncReaders = f.asyncReaders[1:]
				err = nil
			} else {
				return 0, err
			}
		}
		buf = buf[nread:]
		if len(buf) == 0 {
			return
		}
		// Update offset for the next read
		offset += int64(nread)
	}

	return
}

func (f *ReadOnlyFile) prefetch(offset int64, _ /*needAtLeast*/ int) (err error) {
	off := offset
	for _, ar := range f.asyncReaders {
		off = ar.oriHttpRange.Offset + ar.oriHttpRange.Count
	}
	//fmt.Printf("prefetch:offset %v, needAtLeast:%v, off:%v\n", offset, needAtLeast, off)
	for len(f.asyncReaders) < f.prefetchNum {
		remaining := f.sizeInBytes - off
		size := minInt64(remaining, f.chunkSize)
		cnt := (size + (AsyncReadeBufferSize - 1)) / AsyncReadeBufferSize
		//fmt.Printf("f.sizeInBytes:%v, off:%v, size:%v, cnt:%v\n", f.sizeInBytes, off, size, cnt)
		//NewAsyncRangeReader support softStartInitial, add more buffer count to prevent connections from not being released
		if size > softStartInitial {
			acnt := (AsyncReadeBufferSize+(softStartInitial-1))/softStartInitial - 1
			cnt += int64(acnt)
		}
		if size != 0 {
			getFn := func(ctx context.Context, httpRange HTTPRange) (output *ReaderRangeGetOutput, err error) {
				request := &GetObjectRequest{
					Bucket:       Ptr(f.bucket),
					Key:          Ptr(f.key),
					VersionId:    f.versionId,
					RequestPayer: f.requestPayer,
				}
				rangeStr := httpRange.FormatHTTPRange()
				if rangeStr != nil {
					request.Range = rangeStr
					request.RangeBehavior = Ptr("standard")
				}
				var result *GetObjectResult
				result, err = f.client.GetObject(f.context, request)
				if err != nil {
					return nil, err
				}

				return &ReaderRangeGetOutput{
					Body:          result.Body,
					ETag:          result.ETag,
					ContentLength: result.ContentLength,
					ContentRange:  result.ContentRange,
				}, nil
				//fmt.Printf("result.Headers:%#v\n", result.Headers)
			}
			ar, err := NewAsyncRangeReader(f.context, getFn, &HTTPRange{off, size}, f.etag, int(cnt))
			if err != nil {
				break
			}
			f.asyncReaders = append(f.asyncReaders, ar)
			off += size
		}

		if size != f.chunkSize {
			break
		}
	}
	return nil
}

type AppendOptions struct {
	// To indicate that the requester is aware that the request and data download will incur costs
	RequestPayer *string

	// The parameters when the object is first generated, supports below
	// CacheControl, ContentEncoding, Expires, ContentType, ContentType, Metadata
	// SSE's parameters, Acl, StorageClass, Tagging
	// If the object exists, ignore this parameters
	CreateParameter *AppendObjectRequest
}

type AppendOnlyFile struct {
	client  AppendFileAPIClient
	context context.Context

	// object info
	bucket       string
	key          string
	requestPayer *string

	info os.FileInfo

	created     bool
	createParam *AppendObjectRequest

	// current write position
	offset    int64
	hashCRC64 *string

	closed bool
}

// NewAppendFile AppendFile opens or creates the named file for appending.
// If successful, methods on the returned file can be used for appending.
func NewAppendFile(ctx context.Context, c AppendFileAPIClient, bucket string, key string, optFns ...func(*AppendOptions)) (*AppendOnlyFile, error) {
	options := AppendOptions{}

	for _, fn := range optFns {
		fn(&options)
	}

	f := &AppendOnlyFile{
		client:  c,
		context: ctx,

		bucket:       bucket,
		key:          key,
		requestPayer: options.RequestPayer,

		created:     false,
		createParam: options.CreateParameter,
	}

	result, err := f.client.HeadObject(f.context, &HeadObjectRequest{Bucket: &f.bucket, Key: &f.key, RequestPayer: f.requestPayer})
	if err != nil {
		var serr *ServiceError
		if errors.As(err, &serr) && serr.StatusCode == 404 {
			// not found
		} else {
			return nil, err
		}
	} else {
		if !strings.EqualFold(ToString(result.ObjectType), "Appendable") {
			return nil, errors.New("Not a appendable file")
		}
		f.offset = result.ContentLength
		f.hashCRC64 = result.HashCRC64
		f.created = true
	}

	return f, nil
}

// Write writes len(b) bytes from b to the AppendOnlyFile.
// It returns the number of bytes written and an error, if any.
// Write returns a non-nil error when n != len(b).
func (f *AppendOnlyFile) Write(b []byte) (n int, err error) {
	if err := f.checkValid("write"); err != nil {
		return 0, err
	}

	n, e := f.write(b)
	if n < 0 {
		n = 0
	}

	if e == nil && n != len(b) {
		err = io.ErrShortWrite
	}

	if e != nil {
		err = f.wrapErr("write", e)
	}

	return n, err
}

// write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
func (f *AppendOnlyFile) write(b []byte) (n int, err error) {
	offset := f.offset

	request := &AppendObjectRequest{
		Bucket:        &f.bucket,
		Key:           &f.key,
		Position:      Ptr(f.offset),
		Body:          bytes.NewReader(b),
		InitHashCRC64: f.hashCRC64,
		RequestPayer:  f.requestPayer,
	}

	f.applyCreateParamIfNeed(request)

	if f.offset == 0 {
		request.InitHashCRC64 = Ptr("0")
	}

	var result *AppendObjectResult
	if result, err = f.client.AppendObject(f.context, request); err == nil {
		f.offset = result.NextPosition
		f.hashCRC64 = result.HashCRC64
		f.created = true
	} else {
		var serr *ServiceError
		if errors.As(err, &serr) && serr.Code == "PositionNotEqualToLength" {
			if position, hashcrc, ok := f.nextAppendPosition(); ok {
				if offset+int64(len(b)) == position {
					f.offset = position
					f.hashCRC64 = hashcrc
					f.created = true
					err = nil
				}
			}
		}
	}

	return int(f.offset - offset), err
}

// WriteFrom writes io.Reader to the File.
// It returns the number of bytes written and an error, if any.
func (f *AppendOnlyFile) WriteFrom(r io.Reader) (n int64, err error) {
	if err := f.checkValid("write"); err != nil {
		return 0, err
	}

	n, err = f.writeFrom(r)

	if err != nil {
		err = f.wrapErr("write", err)
	}

	return n, err
}

func (f *AppendOnlyFile) writeFrom(r io.Reader) (n int64, err error) {
	offset := f.offset

	request := &AppendObjectRequest{
		Bucket:       &f.bucket,
		Key:          &f.key,
		Position:     Ptr(f.offset),
		Body:         r,
		RequestPayer: f.requestPayer,
	}

	f.applyCreateParamIfNeed(request)

	var roffset int64
	var rs io.Seeker
	rs, ok := r.(io.Seeker)
	if ok {
		roffset, _ = rs.Seek(0, io.SeekCurrent)
	}

	var result *AppendObjectResult
	if result, err = f.client.AppendObject(f.context, request); err == nil {
		f.offset = result.NextPosition
		f.hashCRC64 = result.HashCRC64
		f.created = true
	} else {
		var serr *ServiceError
		if errors.As(err, &serr) && serr.Code == "PositionNotEqualToLength" {
			if position, hashcrc, ok := f.nextAppendPosition(); ok {
				if rs != nil {
					if curr, e := rs.Seek(0, io.SeekCurrent); e == nil {
						if offset+(curr-roffset) == position {
							f.offset = position
							f.hashCRC64 = hashcrc
							f.created = true
							err = nil
						}
					}
				}
			}
		}
	}

	return f.offset - offset, err
}

func (f *AppendOnlyFile) nextAppendPosition() (int64, *string, bool) {
	if h, e := f.client.HeadObject(f.context, &HeadObjectRequest{Bucket: &f.bucket, Key: &f.key, RequestPayer: f.requestPayer}); e == nil {
		return h.ContentLength, h.HashCRC64, true
	}
	return 0, nil, false
}

func (f *AppendOnlyFile) applyCreateParamIfNeed(request *AppendObjectRequest) {
	if f.created || f.createParam == nil {
		return
	}

	if len(f.createParam.Acl) > 0 {
		request.Acl = f.createParam.Acl
	}
	if len(f.createParam.StorageClass) > 0 {
		request.StorageClass = f.createParam.StorageClass
	}
	request.CacheControl = f.createParam.CacheControl
	request.ContentDisposition = f.createParam.ContentDisposition
	request.ContentEncoding = f.createParam.ContentEncoding
	request.Expires = f.createParam.Expires
	request.ContentType = f.createParam.ContentType
	request.ServerSideEncryption = f.createParam.ServerSideEncryption
	request.ServerSideDataEncryption = f.createParam.ServerSideDataEncryption
	request.ServerSideEncryptionKeyId = f.createParam.ServerSideEncryptionKeyId
	request.Metadata = f.createParam.Metadata
	request.Tagging = f.createParam.Tagging
}

// wrapErr wraps an error that occurred during an operation on an open file.
// It passes io.EOF through unchanged, otherwise converts
// Wraps the error in a PathError.
func (f *AppendOnlyFile) wrapErr(op string, err error) error {
	if err == nil || err == io.EOF {
		return err
	}
	return &os.PathError{Op: op, Path: f.name(), Err: err}
}

func (f *AppendOnlyFile) checkValid(_ string) error {
	if f == nil {
		return os.ErrInvalid
	} else if f.closed {
		return os.ErrClosed
	}
	return nil
}

func (f *AppendOnlyFile) name() string {
	return fmt.Sprintf("oss://%s/%s", f.bucket, f.key)
}

// Stat returns the FileInfo structure describing file.
func (f *AppendOnlyFile) Stat() (os.FileInfo, error) {
	if err := f.checkValid("stat"); err != nil {
		return nil, err
	}

	info, err := f.stat()

	if err != nil {
		return nil, f.wrapErr("stat", err)
	}

	return info, nil
}

func (f *AppendOnlyFile) stat() (os.FileInfo, error) {
	var err error
	if f.info == nil || f.info.Size() != f.offset {
		f.info = nil
		if result, err := f.client.HeadObject(f.context, &HeadObjectRequest{Bucket: &f.bucket, Key: &f.key, RequestPayer: f.requestPayer}); err == nil {
			f.info = &fileInfo{
				name:    f.name(),
				size:    result.ContentLength,
				modTime: ToTime(result.LastModified),
				header:  result.Headers,
			}
		}
	}
	return f.info, err
}

// Close closes the File.
func (f *AppendOnlyFile) Close() error {
	if f == nil {
		return os.ErrInvalid
	}
	return f.close()
}

func (f *AppendOnlyFile) close() error {
	f.closed = true
	return nil
}
