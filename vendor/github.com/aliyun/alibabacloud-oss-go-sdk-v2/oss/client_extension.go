package oss

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
)

// NewDownloader creates a new Downloader instance to download objects.
func (c *Client) NewDownloader(optFns ...func(*DownloaderOptions)) *Downloader {
	return NewDownloader(c, optFns...)
}

// NewUploader creates a new Uploader instance to upload objects.
func (c *Client) NewUploader(optFns ...func(*UploaderOptions)) *Uploader {
	return NewUploader(c, optFns...)
}

// NewCopier creates a new Copier instance to copy objects.
func (c *Client) NewCopier(optFns ...func(*CopierOptions)) *Copier {
	return NewCopier(c, optFns...)
}

// OpenFile opens the named file for reading.
func (c *Client) OpenFile(ctx context.Context, bucket string, key string, optFns ...func(*OpenOptions)) (*ReadOnlyFile, error) {
	return NewReadOnlyFile(ctx, c, bucket, key, optFns...)
}

// AppendFile opens or creates the named file for appending.
func (c *Client) AppendFile(ctx context.Context, bucket string, key string, optFns ...func(*AppendOptions)) (*AppendOnlyFile, error) {
	return NewAppendFile(ctx, c, bucket, key, optFns...)
}

type IsObjectExistOptions struct {
	VersionId    *string
	RequestPayer *string
}

// IsObjectExist checks if the object exists.
func (c *Client) IsObjectExist(ctx context.Context, bucket string, key string, optFns ...func(*IsObjectExistOptions)) (bool, error) {
	options := IsObjectExistOptions{}
	for _, fn := range optFns {
		fn(&options)
	}
	_, err := c.GetObjectMeta(ctx, &GetObjectMetaRequest{Bucket: Ptr(bucket), Key: Ptr(key), VersionId: options.VersionId, RequestPayer: options.RequestPayer})
	if err == nil {
		return true, nil
	}
	var serr *ServiceError
	errors.As(err, &serr)
	if errors.As(err, &serr) {
		if serr.Code == "NoSuchKey" ||
			// error code not in response header
			(serr.StatusCode == 404 && serr.Code == "BadErrorResponse") {
			return false, nil
		}
	}
	return false, err
}

// IsBucketExist checks if the bucket exists.
func (c *Client) IsBucketExist(ctx context.Context, bucket string, optFns ...func(*Options)) (bool, error) {
	_, err := c.GetBucketAcl(ctx, &GetBucketAclRequest{Bucket: Ptr(bucket)}, optFns...)
	if err == nil {
		return true, nil
	}
	var serr *ServiceError
	if errors.As(err, &serr) {
		if serr.Code == "NoSuchBucket" {
			return false, nil
		}
		return true, nil
	}
	return false, err
}

// PutObjectFromFile creates a new object from the local file.
func (c *Client) PutObjectFromFile(ctx context.Context, request *PutObjectRequest, filePath string, optFns ...func(*Options)) (*PutObjectResult, error) {
	if request == nil {
		return nil, NewErrParamNull("request")
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	pRequest := *request
	pRequest.Body = file
	return c.PutObject(ctx, &pRequest, optFns...)
}

// GetObjectToFile downloads the object into a local file.
func (c *Client) GetObjectToFile(ctx context.Context, request *GetObjectRequest, filePath string, optFns ...func(*Options)) (*GetObjectResult, error) {
	if request == nil {
		return nil, NewErrParamNull("request")
	}
	var (
		hash   hash.Hash64
		prog   *progressTracker
		result *GetObjectResult
		err    error
		retry  bool
	)
	if request.ProgressFn != nil {
		prog = &progressTracker{
			pr: request.ProgressFn,
		}
	}
	if c.hasFeature(FeatureEnableCRC64CheckDownload) {
		hash = NewCRC64(0)
	}
	i := 0
	maxRetrys := c.retryMaxAttempts(nil)
	for {
		i++
		result, retry, err = c.getObjectToFileNoRerty(ctx, request, filePath, hash, prog, optFns...)
		if err == nil || !retry {
			break
		}
		if i > maxRetrys {
			break
		}
	}
	return result, err
}

func (c *Client) getObjectToFileNoRerty(ctx context.Context, request *GetObjectRequest, filePath string,
	hash hash.Hash64, prog *progressTracker, optFns ...func(*Options)) (*GetObjectResult, bool, error) {
	result, err := c.GetObject(ctx, request, optFns...)
	if err != nil {
		return nil, false, err
	}
	defer result.Body.Close()

	file, err := os.Create(filePath)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()

	var writers []io.Writer
	if hash != nil {
		hash.Reset()
		writers = append(writers, hash)
	}
	if prog != nil {
		prog.total = result.ContentLength
		prog.Reset()
		writers = append(writers, prog)
	}
	var r io.Reader = result.Body
	if len(writers) > 0 {
		r = io.TeeReader(result.Body, io.MultiWriter(writers...))
	}
	_, err = io.Copy(file, r)

	if err == nil && hash != nil {
		err = checkResponseHeaderCRC64(fmt.Sprint(hash.Sum64()), result.Headers)
	}
	return result, true, err
}

// GetObjectToFileV2 downloads the object into a local file.
// Compared to GetObjectToFile, GetObjectToFileV2 provides the following enhancements:
//  1. Supports configurable write buffer size via writeBufferSize parameter to reduce disk I/O frequency.
//     Pass nil to use unbuffered writes.
//  2. Automatic resume on network disconnection. If the download is interrupted due to network instability,
//     it will automatically reconnect from the point of disconnection and continue downloading,
//     powered by NewRangeReader which transparently handles reconnection.
func (c *Client) GetObjectToFileV2(ctx context.Context, request *GetObjectRequest, filePath string, writeBufferSize *int, optFns ...func(*Options)) (*GetObjectResult, error) {
	if request == nil {
		return nil, NewErrParamNull("request")
	}

	var getRequest GetObjectRequest
	copyRequest(&getRequest, request)

	var (
		firstResult *GetObjectResult
		crcHash     hash.Hash64
		prog        *progressTracker
	)

	if request.ProgressFn != nil {
		prog = &progressTracker{pr: request.ProgressFn}
	}

	if c.hasFeature(FeatureEnableCRC64CheckDownload) && request.Range == nil {
		crcHash = NewCRC64(0)
	}

	getFn := func(ctx context.Context, httpRange HTTPRange) (output *ReaderRangeGetOutput, err error) {
		getRequest.Range = nil
		getRequest.RangeBehavior = nil
		rangeStr := httpRange.FormatHTTPRange()
		if rangeStr != nil {
			getRequest.Range = rangeStr
			getRequest.RangeBehavior = Ptr("standard")
		}

		result, err := c.GetObject(ctx, &getRequest, optFns...)
		if err != nil {
			return nil, err
		}

		if firstResult == nil {
			firstResult = result
			if prog != nil {
				if result.ContentRange != nil {
					_, _, prog.total, _ = ParseContentRange(*result.ContentRange)
				} else {
					prog.total = result.ContentLength
				}
			}
		}

		return &ReaderRangeGetOutput{
			Body:          result.Body,
			ETag:          result.ETag,
			ContentLength: result.ContentLength,
			ContentRange:  result.ContentRange,
		}, nil
	}

	var httpRange *HTTPRange
	if request.Range != nil {
		hr, err := ParseRange(ToString(request.Range))
		if err != nil {
			return nil, err
		}
		httpRange = hr
	}

	reader, err := NewRangeReader(ctx, getFn, httpRange, "")
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var r io.Reader = reader
	var writers []io.Writer
	if crcHash != nil {
		writers = append(writers, crcHash)
	}
	if prog != nil {
		writers = append(writers, prog)
	}
	if len(writers) > 0 {
		r = io.TeeReader(reader, io.MultiWriter(writers...))
	}

	var writer io.Writer = file
	if writeBufferSize != nil && *writeBufferSize > 0 {
		writer = bufio.NewWriterSize(file, *writeBufferSize)
	}

	_, err = io.Copy(writer, r)

	if err == nil {
		switch w := writer.(type) {
		case *bufio.Writer:
			err = w.Flush()
		}
	}

	if err == nil && crcHash != nil && firstResult != nil {
		err = checkResponseHeaderCRC64(fmt.Sprint(crcHash.Sum64()), firstResult.Headers)
	}

	return firstResult, err
}