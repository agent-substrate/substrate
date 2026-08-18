package oss

import (
	"context"
	"fmt"
)

type PaginatorOptions struct {
	// The maximum number of items in the response.
	Limit int32
}

// ListObjectsPaginator is a paginator for ListObjects
type ListObjectsPaginator struct {
	options     PaginatorOptions
	client      *Client
	request     *ListObjectsRequest
	marker      *string
	firstPage   bool
	isTruncated bool
}

func (c *Client) NewListObjectsPaginator(request *ListObjectsRequest, optFns ...func(*PaginatorOptions)) *ListObjectsPaginator {
	if request == nil {
		request = &ListObjectsRequest{}
	}

	options := PaginatorOptions{}
	options.Limit = request.MaxKeys

	for _, fn := range optFns {
		fn(&options)
	}

	return &ListObjectsPaginator{
		options:     options,
		client:      c,
		request:     request,
		marker:      request.Marker,
		firstPage:   true,
		isTruncated: false,
	}
}

// HasNext Returns true if there’s a next page.
func (p *ListObjectsPaginator) HasNext() bool {
	return p.firstPage || p.isTruncated
}

// NextPage retrieves the next ListObjects page.
func (p *ListObjectsPaginator) NextPage(ctx context.Context, optFns ...func(*Options)) (*ListObjectsResult, error) {
	if !p.HasNext() {
		return nil, fmt.Errorf("no more pages available")
	}

	request := *p.request
	request.Marker = p.marker

	var limit int32
	if p.options.Limit > 0 {
		limit = p.options.Limit
	}
	request.MaxKeys = limit
	request.EncodingType = Ptr("url")

	result, err := p.client.ListObjects(ctx, &request, optFns...)
	if err != nil {
		return nil, err
	}

	p.firstPage = false
	p.isTruncated = result.IsTruncated
	p.marker = result.NextMarker

	return result, nil
}

// ListObjectsV2Paginator is a paginator for ListObjectsV2
type ListObjectsV2Paginator struct {
	options       PaginatorOptions
	client        *Client
	request       *ListObjectsV2Request
	continueToken *string
	firstPage     bool
	isTruncated   bool
}

func (c *Client) NewListObjectsV2Paginator(request *ListObjectsV2Request, optFns ...func(*PaginatorOptions)) *ListObjectsV2Paginator {
	if request == nil {
		request = &ListObjectsV2Request{}
	}

	options := PaginatorOptions{}
	options.Limit = request.MaxKeys

	for _, fn := range optFns {
		fn(&options)
	}

	return &ListObjectsV2Paginator{
		options:       options,
		client:        c,
		request:       request,
		continueToken: request.ContinuationToken,
		firstPage:     true,
		isTruncated:   false,
	}
}

// HasNext Returns true if there’s a next page.
func (p *ListObjectsV2Paginator) HasNext() bool {
	return p.firstPage || p.isTruncated
}

// NextPage retrieves the next ListObjectsV2 page.
func (p *ListObjectsV2Paginator) NextPage(ctx context.Context, optFns ...func(*Options)) (*ListObjectsV2Result, error) {
	if !p.HasNext() {
		return nil, fmt.Errorf("no more pages available")
	}

	request := *p.request
	request.ContinuationToken = p.continueToken

	var limit int32
	if p.options.Limit > 0 {
		limit = p.options.Limit
	}
	request.MaxKeys = limit
	request.EncodingType = Ptr("url")

	result, err := p.client.ListObjectsV2(ctx, &request, optFns...)
	if err != nil {
		return nil, err
	}

	p.firstPage = false
	p.isTruncated = result.IsTruncated
	p.continueToken = result.NextContinuationToken

	return result, nil
}

// ListObjectVersionsPaginator is a paginator for ListObjectVersions
type ListObjectVersionsPaginator struct {
	options         PaginatorOptions
	client          *Client
	request         *ListObjectVersionsRequest
	keyMarker       *string
	versionIdMarker *string
	firstPage       bool
	isTruncated     bool
}

func (c *Client) NewListObjectVersionsPaginator(request *ListObjectVersionsRequest, optFns ...func(*PaginatorOptions)) *ListObjectVersionsPaginator {
	if request == nil {
		request = &ListObjectVersionsRequest{}
	}

	options := PaginatorOptions{}
	options.Limit = request.MaxKeys

	for _, fn := range optFns {
		fn(&options)
	}

	return &ListObjectVersionsPaginator{
		options:         options,
		client:          c,
		request:         request,
		keyMarker:       request.KeyMarker,
		versionIdMarker: request.VersionIdMarker,
		firstPage:       true,
		isTruncated:     false,
	}
}

// HasNext Returns true if there’s a next page.
func (p *ListObjectVersionsPaginator) HasNext() bool {
	return p.firstPage || p.isTruncated
}

// NextPage retrieves the next ListObjectVersions page.
func (p *ListObjectVersionsPaginator) NextPage(ctx context.Context, optFns ...func(*Options)) (*ListObjectVersionsResult, error) {
	if !p.HasNext() {
		return nil, fmt.Errorf("no more pages available")
	}

	request := *p.request
	request.KeyMarker = p.keyMarker
	request.VersionIdMarker = p.versionIdMarker

	var limit int32
	if p.options.Limit > 0 {
		limit = p.options.Limit
	}
	request.MaxKeys = limit
	request.EncodingType = Ptr("url")

	result, err := p.client.ListObjectVersions(ctx, &request, optFns...)
	if err != nil {
		return nil, err
	}

	p.firstPage = false
	p.isTruncated = result.IsTruncated
	p.keyMarker = result.NextKeyMarker
	p.versionIdMarker = result.NextVersionIdMarker

	return result, nil
}

// ListBucketsPaginator is a paginator for ListBuckets
type ListBucketsPaginator struct {
	options     PaginatorOptions
	client      *Client
	request     *ListBucketsRequest
	marker      *string
	firstPage   bool
	isTruncated bool
}

func (c *Client) NewListBucketsPaginator(request *ListBucketsRequest, optFns ...func(*PaginatorOptions)) *ListBucketsPaginator {
	if request == nil {
		request = &ListBucketsRequest{}
	}

	options := PaginatorOptions{}
	options.Limit = request.MaxKeys

	for _, fn := range optFns {
		fn(&options)
	}

	return &ListBucketsPaginator{
		options:     options,
		client:      c,
		request:     request,
		marker:      request.Marker,
		firstPage:   true,
		isTruncated: false,
	}
}

// HasNext Returns true if there’s a next page.
func (p *ListBucketsPaginator) HasNext() bool {
	return p.firstPage || p.isTruncated
}

// NextPage retrieves the next ListBuckets page.
func (p *ListBucketsPaginator) NextPage(ctx context.Context, optFns ...func(*Options)) (*ListBucketsResult, error) {
	if !p.HasNext() {
		return nil, fmt.Errorf("no more pages available")
	}

	request := *p.request
	request.Marker = p.marker

	var limit int32
	if p.options.Limit > 0 {
		limit = p.options.Limit
	}
	request.MaxKeys = limit

	result, err := p.client.ListBuckets(ctx, &request, optFns...)
	if err != nil {
		return nil, err
	}

	p.firstPage = false
	p.isTruncated = result.IsTruncated
	p.marker = result.NextMarker

	return result, nil
}

type ListPartsAPIClient interface {
	ListParts(ctx context.Context, request *ListPartsRequest, optFns ...func(*Options)) (*ListPartsResult, error)
}

// ListPartsPaginator is a paginator for ListParts
type ListPartsPaginator struct {
	options     PaginatorOptions
	client      ListPartsAPIClient
	request     *ListPartsRequest
	marker      int32
	firstPage   bool
	isTruncated bool
}

func NewListPartsPaginator(c ListPartsAPIClient, request *ListPartsRequest, optFns ...func(*PaginatorOptions)) *ListPartsPaginator {
	if request == nil {
		request = &ListPartsRequest{}
	}

	options := PaginatorOptions{}
	options.Limit = request.MaxParts

	for _, fn := range optFns {
		fn(&options)
	}

	return &ListPartsPaginator{
		options:     options,
		client:      c,
		request:     request,
		marker:      request.PartNumberMarker,
		firstPage:   true,
		isTruncated: false,
	}
}

func (c *Client) NewListPartsPaginator(request *ListPartsRequest, optFns ...func(*PaginatorOptions)) *ListPartsPaginator {
	return NewListPartsPaginator(c, request, optFns...)
}

// HasNext Returns true if there’s a next page.
func (p *ListPartsPaginator) HasNext() bool {
	return p.firstPage || p.isTruncated
}

// NextPage retrieves the next ListParts page.
func (p *ListPartsPaginator) NextPage(ctx context.Context, optFns ...func(*Options)) (*ListPartsResult, error) {
	if !p.HasNext() {
		return nil, fmt.Errorf("no more pages available")
	}

	request := *p.request
	request.PartNumberMarker = p.marker
	var limit int32
	if p.options.Limit > 0 {
		limit = p.options.Limit
	}
	request.MaxParts = limit
	request.EncodingType = Ptr("url")
	result, err := p.client.ListParts(ctx, &request, optFns...)
	if err != nil {
		return nil, err
	}

	p.firstPage = false
	p.isTruncated = result.IsTruncated
	p.marker = result.NextPartNumberMarker

	return result, nil
}

// ListMultipartUploadsPaginator is a paginator for ListMultipartUploads
type ListMultipartUploadsPaginator struct {
	options        PaginatorOptions
	client         *Client
	request        *ListMultipartUploadsRequest
	keyMarker      *string
	uploadIdMarker *string
	firstPage      bool
	isTruncated    bool
}

func (c *Client) NewListMultipartUploadsPaginator(request *ListMultipartUploadsRequest, optFns ...func(*PaginatorOptions)) *ListMultipartUploadsPaginator {
	if request == nil {
		request = &ListMultipartUploadsRequest{}
	}
	options := PaginatorOptions{}
	options.Limit = request.MaxUploads
	for _, fn := range optFns {
		fn(&options)
	}
	return &ListMultipartUploadsPaginator{
		options:        options,
		client:         c,
		request:        request,
		keyMarker:      request.KeyMarker,
		uploadIdMarker: request.UploadIdMarker,
		firstPage:      true,
		isTruncated:    false,
	}
}

// HasNext Returns true if there’s a next page.
func (p *ListMultipartUploadsPaginator) HasNext() bool {
	return p.firstPage || p.isTruncated
}

// NextPage retrieves the next ListMultipartUploads page.
func (p *ListMultipartUploadsPaginator) NextPage(ctx context.Context, optFns ...func(*Options)) (*ListMultipartUploadsResult, error) {
	if !p.HasNext() {
		return nil, fmt.Errorf("no more pages available")
	}
	request := *p.request
	request.KeyMarker = p.keyMarker
	request.UploadIdMarker = p.uploadIdMarker
	var limit int32
	if p.options.Limit > 0 {
		limit = p.options.Limit
	}
	request.MaxUploads = limit
	request.EncodingType = Ptr("url")
	result, err := p.client.ListMultipartUploads(ctx, &request, optFns...)
	if err != nil {
		return nil, err
	}

	p.firstPage = false
	p.isTruncated = result.IsTruncated
	p.keyMarker = result.NextKeyMarker
	p.uploadIdMarker = result.NextUploadIdMarker
	return result, nil
}
