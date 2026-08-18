package oss

import "os"

const (
	MaxUploadParts int32 = 10000

	// MaxPartSize Max part size, 5GB, For UploadPart
	MaxPartSize int64 = 5 * 1024 * 1024 * 1024

	// MinPartSize Min part size, 100KB, For UploadPart
	MinPartSize int64 = 100 * 1024

	// DefaultPartSize Default part size, 6M
	DefaultPartSize int64 = 6 * 1024 * 1024

	// DefaultUploadPartSize Default part size for uploader uploads data
	DefaultUploadPartSize = DefaultPartSize

	// DefaultDownloadPartSize Default part size for downloader downloads object
	DefaultDownloadPartSize = DefaultPartSize

	// DefaultCopyPartSize Default part size for copier copys object, 64M
	DefaultCopyPartSize int64 = 64 * 1024 * 1024

	// DefaultParallel Default parallel
	DefaultParallel = 3

	// DefaultUploadParallel Default parallel for uploader uploads data
	DefaultUploadParallel = DefaultParallel

	// DefaultDownloadParallel Default parallel for downloader downloads object
	DefaultDownloadParallel = DefaultParallel

	// DefaultCopyParallel Default parallel for copier copys object
	DefaultCopyParallel = DefaultParallel

	// DefaultPrefetchThreshold Default prefetch threshold to swith to async read in ReadOnlyFile
	DefaultPrefetchThreshold int64 = 20 * 1024 * 1024

	// DefaultPrefetchNum Default prefetch number for async read in ReadOnlyFile
	DefaultPrefetchNum = DefaultParallel

	// DefaultPrefetchChunkSize Default prefetch chunk size for async read in ReadOnlyFile
	DefaultPrefetchChunkSize = DefaultPartSize

	// DefaultCopyThreshold Default threshold to use muitipart copy in Copier, 256M
	DefaultCopyThreshold int64 = 200 * 1024 * 1024

	// FilePermMode File permission
	FilePermMode = os.FileMode(0664)

	// TempFileSuffix Temp file suffix
	TempFileSuffix = ".temp"

	// CheckpointFileSuffixDownloader Checkpoint file suffix for Downloader
	CheckpointFileSuffixDownloader = ".dcp"

	// CheckpointFileSuffixUploader Checkpoint file suffix for Uploader
	CheckpointFileSuffixUploader = ".ucp"

	// CheckpointMagic Checkpoint file Magic
	CheckpointMagic = "92611BED-89E2-46B6-89E5-72F273D4B0A3"

	// DefaultProduct Product for signing
	DefaultProduct = "oss"

	// CloudBoxProduct Product of cloud box for signing
	CloudBoxProduct = "oss-cloudbox"

	// DefaultEndpointScheme The URL's scheme, default is https
	DefaultEndpointScheme = "https"

	// DefaultSignatureVersion Default signature version is v4
	DefaultSignatureVersion = SignatureVersionV4

	// DefaultOutOfOrderReadThreshold Default out of order read threshold is 3
	DefaultOutOfOrderReadThreshold int64 = 3
)
