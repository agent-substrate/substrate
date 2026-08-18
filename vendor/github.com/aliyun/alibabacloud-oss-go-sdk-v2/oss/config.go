package oss

import (
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/retry"
)

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type Config struct {
	// The region in which the bucket is located.
	Region *string

	// The domain names that other services can use to access OSS.
	Endpoint *string

	// RetryMaxAttempts specifies the maximum number attempts an API client will call
	// an operation that fails with a retryable error.
	RetryMaxAttempts *int

	// Retryer guides how HTTP requests should be retried in case of recoverable failures.
	Retryer retry.Retryer

	// The HTTP client to invoke API calls with. Defaults to client's default HTTP
	// implementation if nil.
	HttpClient HTTPClient

	// The credentials provider to use when signing requests.
	CredentialsProvider credentials.CredentialsProvider

	// Allows you to enable the client to use path-style addressing, i.e., https://oss-cn-hangzhou.aliyuncs.com/bucket/key.
	// By default, the oss client will use virtual hosted addressing i.e., https://bucket.oss-cn-hangzhou.aliyuncs.com/key.
	UsePathStyle *bool

	// If the endpoint is s CName, set this flag to true
	UseCName *bool

	// Connect timeout
	ConnectTimeout *time.Duration

	// read & write timeout
	ReadWriteTimeout *time.Duration

	// max connection
	MaxConnections *int

	// Skip server certificate verification
	InsecureSkipVerify *bool

	// Enable http redirect or not. Default is disable
	EnabledRedirect *bool

	// Flag of using proxy host.
	ProxyHost *string

	// Read the proxy setting from the environment variables.
	// HTTP_PROXY, HTTPS_PROXY and NO_PROXY (or the lowercase versions thereof).
	// HTTPS_PROXY takes precedence over HTTP_PROXY for https requests.
	ProxyFromEnvironment *bool

	// Upload bandwidth limit in kBytes/s for all request
	UploadBandwidthlimit *int64

	// Download bandwidth limit in kBytes/s for all request
	DownloadBandwidthlimit *int64

	// Authentication with OSS Signature Version
	SignatureVersion *SignatureVersionType

	// The level of the output log
	LogLevel *int

	// A interface for the SDK to log messages to.
	LogPrinter LogPrinter

	// DisableSSL forces the endpoint to be resolved as HTTP.
	DisableSSL *bool

	// Dual-stack endpoints are provided in some regions.
	// This allows an IPv4 client and an IPv6 client to access a bucket by using the same endpoint.
	// Set this to `true` to use a dual-stack endpoint for the requests.
	UseDualStackEndpoint *bool

	// OSS provides the transfer acceleration feature to accelerate date transfers of data
	// uploads and downloads across countries and regions.
	// Set this to `true` to use a accelerate endpoint for the requests.
	UseAccelerateEndpoint *bool

	// You can use an internal endpoint to communicate between Alibaba Cloud services located  within the same
	// region over the internal network. You are not charged for the traffic generated over the internal network.
	// Set this to `true` to use a accelerate endpoint for the requests.
	UseInternalEndpoint *bool

	// Check data integrity of uploads via the crc64 by default.
	// This feature takes effect for PutObject, AppendObject, UploadPart, Uploader.UploadFrom and Uploader.UploadFile
	// Set this to `true` to disable this feature.
	DisableUploadCRC64Check *bool

	// Check data integrity of download via the crc64 by default.
	// This feature only takes effect for Downloader.DownloadFile, GetObjectToFile
	// Set this to `true` to disable this feature.
	DisableDownloadCRC64Check *bool

	// Additional signable headers.
	AdditionalHeaders []string

	// The optional user specific identifier appended to the User-Agent header.
	UserAgent *string

	// The cloud box id
	CloudBoxId *string

	// The cloud box id is automatically extracted from endpoint.
	EnableAutoDetectCloudBoxId *bool

	// The account id
	AccountId *string

	// Local address to bind to for outgoing connections.
	BindAddress net.IP
}

func NewConfig() *Config {
	return &Config{}
}

func (c Config) Copy() Config {
	cp := c
	return cp
}

func LoadDefaultConfig() *Config {
	config := &Config{}

	// load from env
	str := os.Getenv("OSS_SDK_LOG_LEVEL")
	if str != "" {
		if level := ToLogLevel(str); level > LogOff {
			config.LogLevel = Ptr(level)
		}
	}

	return config
}

func (c *Config) WithRegion(region string) *Config {
	c.Region = Ptr(region)
	return c
}

func (c *Config) WithEndpoint(endpoint string) *Config {
	c.Endpoint = Ptr(endpoint)
	return c
}

func (c *Config) WithRetryMaxAttempts(value int) *Config {
	c.RetryMaxAttempts = Ptr(value)
	return c
}

func (c *Config) WithRetryer(retryer retry.Retryer) *Config {
	c.Retryer = retryer
	return c
}

func (c *Config) WithHttpClient(client *http.Client) *Config {
	c.HttpClient = client
	return c
}

func (c *Config) WithCredentialsProvider(provider credentials.CredentialsProvider) *Config {
	c.CredentialsProvider = provider
	return c
}

func (c *Config) WithUsePathStyle(enable bool) *Config {
	c.UsePathStyle = Ptr(enable)
	return c
}

func (c *Config) WithUseCName(enable bool) *Config {
	c.UseCName = Ptr(enable)
	return c
}

func (c *Config) WithConnectTimeout(value time.Duration) *Config {
	c.ConnectTimeout = Ptr(value)
	return c
}

func (c *Config) WithReadWriteTimeout(value time.Duration) *Config {
	c.ReadWriteTimeout = Ptr(value)
	return c
}

func (c *Config) WithMaxConnections(value int) *Config {
	c.MaxConnections = Ptr(value)
	return c
}

func (c *Config) WithInsecureSkipVerify(value bool) *Config {
	c.InsecureSkipVerify = Ptr(value)
	return c
}

func (c *Config) WithEnabledRedirect(value bool) *Config {
	c.EnabledRedirect = Ptr(value)
	return c
}

func (c *Config) WithProxyHost(value string) *Config {
	c.ProxyHost = Ptr(value)
	return c
}

func (c *Config) WithProxyFromEnvironment(value bool) *Config {
	c.ProxyFromEnvironment = Ptr(value)
	return c
}

func (c *Config) WithUploadBandwidthlimit(value int64) *Config {
	c.UploadBandwidthlimit = Ptr(value)
	return c
}

func (c *Config) WithDownloadBandwidthlimit(value int64) *Config {
	c.DownloadBandwidthlimit = Ptr(value)
	return c
}

func (c *Config) WithSignatureVersion(value SignatureVersionType) *Config {
	c.SignatureVersion = Ptr(value)
	return c
}

func (c *Config) WithLogLevel(level int) *Config {
	c.LogLevel = Ptr(level)
	return c
}

func (c *Config) WithLogPrinter(printer LogPrinter) *Config {
	c.LogPrinter = printer
	return c
}

func (c *Config) WithDisableSSL(value bool) *Config {
	c.DisableSSL = Ptr(value)
	return c
}

func (c *Config) WithUseDualStackEndpoint(value bool) *Config {
	c.UseDualStackEndpoint = Ptr(value)
	return c
}

func (c *Config) WithUseAccelerateEndpoint(value bool) *Config {
	c.UseAccelerateEndpoint = Ptr(value)
	return c
}

func (c *Config) WithUseInternalEndpoint(value bool) *Config {
	c.UseInternalEndpoint = Ptr(value)
	return c
}

func (c *Config) WithDisableUploadCRC64Check(value bool) *Config {
	c.DisableUploadCRC64Check = Ptr(value)
	return c
}

func (c *Config) WithDisableDownloadCRC64Check(value bool) *Config {
	c.DisableDownloadCRC64Check = Ptr(value)
	return c
}

func (c *Config) WithAdditionalHeaders(value []string) *Config {
	c.AdditionalHeaders = value
	return c
}

func (c *Config) WithUserAgent(value string) *Config {
	c.UserAgent = Ptr(value)
	return c
}

func (c *Config) WithCloudBoxId(value string) *Config {
	c.CloudBoxId = Ptr(value)
	return c
}

func (c *Config) WithEnableAutoDetectCloudBoxId(value bool) *Config {
	c.EnableAutoDetectCloudBoxId = Ptr(value)
	return c
}

func (c *Config) WithAccountId(value string) *Config {
	c.AccountId = Ptr(value)
	return c
}
  
func (c *Config) WithBindAddress(value net.IP) *Config {
	c.BindAddress = value
	return c
}
