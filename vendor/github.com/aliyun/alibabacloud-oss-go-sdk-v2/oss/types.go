package oss

import (
	"context"
	"io"
	"net/http"
)

type OperationMetadata struct {
	values map[any][]any
}

func (m OperationMetadata) Get(key any) any {
	if m.values == nil {
		return nil
	}
	v := m.values[key]
	if len(v) == 0 {
		return nil
	}
	return v[0]
}

func (m OperationMetadata) Values(key any) []any {
	if m.values == nil {
		return nil
	}
	return m.values[key]
}

func (m *OperationMetadata) Add(key, value any) {
	if m.values == nil {
		m.values = map[any][]any{}
	}
	m.values[key] = append(m.values[key], value)
}

func (m *OperationMetadata) Set(key, value any) {
	if m.values == nil {
		m.values = map[any][]any{}
	}
	m.values[key] = []any{value}
}

func (m OperationMetadata) Has(key any) bool {
	if m.values == nil {
		return false
	}
	_, ok := m.values[key]
	return ok
}

func (m OperationMetadata) Clone() OperationMetadata {
	vs := make(map[any][]any, len(m.values))
	for k, v := range m.values {
		vv := make([]any, len(v))
		copy(vv, v)
		vs[k] = vv
	}
	return OperationMetadata{
		values: vs,
	}
}

type RequestCommon struct {
	Headers    map[string]string
	Parameters map[string]string
	Payload    io.Reader
}

type RequestCommonInterface interface {
	GetCommonFileds() (map[string]string, map[string]string, io.Reader)
}

func (r *RequestCommon) GetCommonFileds() (map[string]string, map[string]string, io.Reader) {
	return r.Headers, r.Parameters, r.Payload
}

type ResultCommon struct {
	Status     string
	StatusCode int
	Headers    http.Header
	OpMetadata OperationMetadata
}

type ResultCommonInterface interface {
	CopyIn(status string, statusCode int, headers http.Header, meta OperationMetadata)
}

func (r *ResultCommon) CopyIn(status string, statusCode int, headers http.Header, meta OperationMetadata) {
	r.Status = status
	r.StatusCode = statusCode
	r.Headers = headers
	r.OpMetadata = meta
}

type OperationInput struct {
	OpName     string
	Method     string
	Headers    map[string]string
	Parameters map[string]string
	Body       io.Reader

	Bucket *string
	Key    *string

	OpMetadata OperationMetadata
}

type OperationOutput struct {
	Input *OperationInput

	Status     string
	StatusCode int
	Headers    http.Header
	Body       io.ReadCloser

	OpMetadata OperationMetadata

	httpRequest *http.Request
}

type RequestBodyTracker interface {
	io.Writer
	Reset()
}

type DownloadAPIClient interface {
	HeadObject(ctx context.Context, request *HeadObjectRequest, optFns ...func(*Options)) (*HeadObjectResult, error)
	GetObject(ctx context.Context, request *GetObjectRequest, optFns ...func(*Options)) (*GetObjectResult, error)
}

type UploadAPIClient interface {
	HeadObject(ctx context.Context, request *HeadObjectRequest, optFns ...func(*Options)) (*HeadObjectResult, error)
	PutObject(ctx context.Context, request *PutObjectRequest, optFns ...func(*Options)) (*PutObjectResult, error)
	InitiateMultipartUpload(ctx context.Context, request *InitiateMultipartUploadRequest, optFns ...func(*Options)) (*InitiateMultipartUploadResult, error)
	UploadPart(ctx context.Context, request *UploadPartRequest, optFns ...func(*Options)) (*UploadPartResult, error)
	CompleteMultipartUpload(ctx context.Context, request *CompleteMultipartUploadRequest, optFns ...func(*Options)) (*CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, request *AbortMultipartUploadRequest, optFns ...func(*Options)) (*AbortMultipartUploadResult, error)
	ListParts(ctx context.Context, request *ListPartsRequest, optFns ...func(*Options)) (*ListPartsResult, error)
}

type OpenFileAPIClient interface {
	HeadObject(ctx context.Context, request *HeadObjectRequest, optFns ...func(*Options)) (*HeadObjectResult, error)
	GetObject(ctx context.Context, request *GetObjectRequest, optFns ...func(*Options)) (*GetObjectResult, error)
}

type AppendFileAPIClient interface {
	HeadObject(ctx context.Context, request *HeadObjectRequest, optFns ...func(*Options)) (*HeadObjectResult, error)
	AppendObject(ctx context.Context, request *AppendObjectRequest, optFns ...func(*Options)) (*AppendObjectResult, error)
}

type CopyAPIClient interface {
	HeadObject(ctx context.Context, request *HeadObjectRequest, optFns ...func(*Options)) (*HeadObjectResult, error)
	CopyObject(ctx context.Context, request *CopyObjectRequest, optFns ...func(*Options)) (*CopyObjectResult, error)
	InitiateMultipartUpload(ctx context.Context, request *InitiateMultipartUploadRequest, optFns ...func(*Options)) (*InitiateMultipartUploadResult, error)
	UploadPartCopy(ctx context.Context, request *UploadPartCopyRequest, optFns ...func(*Options)) (*UploadPartCopyResult, error)
	CompleteMultipartUpload(ctx context.Context, request *CompleteMultipartUploadRequest, optFns ...func(*Options)) (*CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, request *AbortMultipartUploadRequest, optFns ...func(*Options)) (*AbortMultipartUploadResult, error)
	ListParts(ctx context.Context, request *ListPartsRequest, optFns ...func(*Options)) (*ListPartsResult, error)
	GetObjectTagging(ctx context.Context, request *GetObjectTaggingRequest, optFns ...func(*Options)) (*GetObjectTaggingResult, error)
}
