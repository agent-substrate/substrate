package oss

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/retry"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/transport"
)

type Options struct {
	Product string

	Region string

	Endpoint *url.URL

	RetryMaxAttempts *int

	Retryer retry.Retryer

	Signer signer.Signer

	CredentialsProvider credentials.CredentialsProvider

	HttpClient HTTPClient

	ResponseHandlers []func(*http.Response) error

	UrlStyle UrlStyleType

	FeatureFlags FeatureFlagsType

	OpReadWriteTimeout *time.Duration

	AuthMethod *AuthMethodType

	AdditionalHeaders []string

	EndpointProvider EndpointProvider
}

func (c Options) Copy() Options {
	to := c
	to.ResponseHandlers = make([]func(*http.Response) error, len(c.ResponseHandlers))
	copy(to.ResponseHandlers, c.ResponseHandlers)
	return to
}

func OpReadWriteTimeout(value time.Duration) func(*Options) {
	return func(o *Options) {
		o.OpReadWriteTimeout = Ptr(value)
	}
}

type innerOptions struct {
	BwTokenBuckets BwTokenBuckets

	// A clock offset that how much client time is different from server time
	ClockOffset time.Duration

	// Logger
	Log Logger

	// UserAgent
	UserAgent string
}

type Client struct {
	options Options
	inner   innerOptions
}

func NewClient(cfg *Config, optFns ...func(*Options)) *Client {
	options := Options{
		Product:             DefaultProduct,
		Region:              ToString(cfg.Region),
		RetryMaxAttempts:    cfg.RetryMaxAttempts,
		Retryer:             cfg.Retryer,
		CredentialsProvider: cfg.CredentialsProvider,
		HttpClient:          cfg.HttpClient,
		FeatureFlags:        FeatureFlagsDefault,
		AdditionalHeaders:   cfg.AdditionalHeaders,
	}
	inner := innerOptions{
		Log:       NewLogger(ToInt(cfg.LogLevel), cfg.LogPrinter),
		UserAgent: buildUserAgent(cfg),
	}

	resolveEndpoint(cfg, &options)
	resolveRetryer(cfg, &options)
	resolveHTTPClient(cfg, &options, &inner)
	resolveSigner(cfg, &options)
	resolveUrlStyle(cfg, &options)
	resolveFeatureFlags(cfg, &options)
	resolveCloudBox(cfg, &options)

	for _, fn := range optFns {
		fn(&options)
	}

	client := &Client{
		options: options,
		inner:   inner,
	}

	return client
}

func resolveEndpoint(cfg *Config, o *Options) {
	disableSSL := ToBool(cfg.DisableSSL)
	endpoint := ToString(cfg.Endpoint)
	region := ToString(cfg.Region)
	if len(endpoint) > 0 {
		endpoint = addEndpointScheme(endpoint, disableSSL)
	} else if isValidRegion(region) {
		endpoint = endpointFromRegion(
			region,
			disableSSL,
			func() EndpointType {
				if ToBool(cfg.UseInternalEndpoint) {
					return EndpointInternal
				} else if ToBool(cfg.UseDualStackEndpoint) {
					return EndpointDualStack
				} else if ToBool(cfg.UseAccelerateEndpoint) {
					return EndpointAccelerate
				}
				return EndpointPublic
			}(),
		)
	}

	if endpoint == "" {
		return
	}

	o.Endpoint, _ = url.Parse(endpoint)
}

func resolveRetryer(_ *Config, o *Options) {
	if o.Retryer != nil {
		return
	}

	o.Retryer = retry.NewStandard()
}

func resolveHTTPClient(cfg *Config, o *Options, inner *innerOptions) {
	if o.HttpClient != nil {
		return
	}

	//config in http.Transport
	custom := []func(*http.Transport){}
	if cfg.InsecureSkipVerify != nil {
		custom = append(custom, transport.InsecureSkipVerify(*cfg.InsecureSkipVerify))
	}
	if cfg.ProxyFromEnvironment != nil && *cfg.ProxyFromEnvironment {
		custom = append(custom, transport.ProxyFromEnvironment())
	}
	if cfg.ProxyHost != nil {
		if url, err := url.Parse(*cfg.ProxyHost); err == nil {
			custom = append(custom, transport.HttpProxy(url))
		}
	}

	if cfg.MaxConnections != nil {
		custom = append(custom, transport.MaxConnections(*cfg.MaxConnections))
	}

	//config in transport  package
	tcfg := &transport.Config{}
	if cfg.ConnectTimeout != nil {
		tcfg.ConnectTimeout = cfg.ConnectTimeout
	}
	if cfg.ReadWriteTimeout != nil {
		tcfg.ReadWriteTimeout = cfg.ReadWriteTimeout
	}
	if cfg.EnabledRedirect != nil {
		tcfg.EnabledRedirect = cfg.EnabledRedirect
	}
	if cfg.UploadBandwidthlimit != nil {
		value := *cfg.UploadBandwidthlimit * 1024
		tb := newBwTokenBucket(value)
		tcfg.PostWrite = append(tcfg.PostWrite, func(n int, _ error) {
			tb.LimitBandwidth(n)
		})
		inner.BwTokenBuckets[BwTokenBucketSlotTx] = tb
	}
	if cfg.DownloadBandwidthlimit != nil {
		value := *cfg.DownloadBandwidthlimit * 1024
		tb := newBwTokenBucket(value)
		tcfg.PostRead = append(tcfg.PostRead, func(n int, _ error) {
			tb.LimitBandwidth(n)
		})
		inner.BwTokenBuckets[BwTokenBucketSlotRx] = tb
	}
	if cfg.BindAddress != nil {
		tcfg.BindAddr = cfg.BindAddress
	}

	o.HttpClient = transport.NewHttpClient(tcfg, custom...)
}

func resolveSigner(cfg *Config, o *Options) {
	if o.Signer != nil {
		return
	}

	ver := DefaultSignatureVersion
	if cfg.SignatureVersion != nil {
		ver = *cfg.SignatureVersion
	}

	switch ver {
	case SignatureVersionV1:
		o.Signer = &signer.SignerV1{}
	default:
		o.Signer = &signer.SignerV4{}
	}
}

func resolveUrlStyle(cfg *Config, o *Options) {
	if cfg.UseCName != nil && *cfg.UseCName {
		o.UrlStyle = UrlStyleCName
	} else if cfg.UsePathStyle != nil && *cfg.UsePathStyle {
		o.UrlStyle = UrlStylePath
	} else {
		o.UrlStyle = UrlStyleVirtualHosted
	}

	// if the endpoint is ip, set to path-style
	if o.Endpoint != nil {
		if ip := net.ParseIP(o.Endpoint.Hostname()); ip != nil {
			o.UrlStyle = UrlStylePath
		}
	}
}

func resolveFeatureFlags(cfg *Config, o *Options) {
	if ToBool(cfg.DisableDownloadCRC64Check) {
		o.FeatureFlags = o.FeatureFlags & ^FeatureEnableCRC64CheckDownload
	}

	if ToBool(cfg.DisableUploadCRC64Check) {
		o.FeatureFlags = o.FeatureFlags & ^FeatureEnableCRC64CheckUpload
	}
}

func resolveCloudBox(cfg *Config, o *Options) {
	if cfg.CloudBoxId != nil {
		o.Region = ToString(cfg.CloudBoxId)
		o.Product = CloudBoxProduct
		return
	}

	if !ToBool(cfg.EnableAutoDetectCloudBoxId) {
		return
	}

	if o.Endpoint == nil {
		return
	}

	//cb-***.{region}.oss-cloudbox-control.aliyuncs.com
	//cb-***.{region}.oss-cloudbox.aliyuncs.com
	host := o.Endpoint.Host
	if !(strings.HasSuffix(host, ".oss-cloudbox.aliyuncs.com") ||
		strings.HasSuffix(host, ".oss-cloudbox-control.aliyuncs.com")) {
		return
	}

	keys := strings.Split(host, ".")
	if keys == nil ||
		len(keys) != 5 ||
		!strings.HasPrefix(keys[0], "cb-") {
		return
	}
	o.Region = keys[0]
	o.Product = CloudBoxProduct
}

func buildUserAgent(cfg *Config) string {
	if cfg.UserAgent == nil {
		return defaultUserAgent
	}

	return fmt.Sprintf("%s/%s", defaultUserAgent, ToString(cfg.UserAgent))
}

func (c *Client) invokeOperation(ctx context.Context, input *OperationInput, optFns []func(*Options)) (output *OperationOutput, err error) {
	if c.getLogLevel() >= LogInfo {
		c.inner.Log.Infof("InvokeOperation Start: input[%p], OpName:%s, Bucket:%s, Key:%s",
			input, input.OpName,
			ToString(input.Bucket), ToString(input.Key))
		defer func() {
			c.inner.Log.Infof("InvokeOperation End: input[%p], OpName:%s, output:'%v', err:'%v'",
				input, input.OpName,
				c.dumpOperationOutput(output), err)
		}()
	}

	options := c.options.Copy()
	opOpt := Options{}

	for _, fn := range optFns {
		fn(&opOpt)
	}

	applyOperationOpt(&options, &opOpt)

	applyOperationMetadata(input, &options)

	ctx = applyOperationContext(ctx, &options)

	output, err = c.sendRequest(ctx, input, &options)

	if err != nil {
		return output, &OperationError{
			name: input.OpName,
			err:  err}
	}

	return output, err
}

func (c *Client) sendRequest(ctx context.Context, input *OperationInput, opts *Options) (output *OperationOutput, err error) {
	var request *http.Request
	var response *http.Response
	if c.getLogLevel() >= LogInfo {
		c.inner.Log.Infof("sendRequest Start: input[%p]", input)
		defer func() {
			c.inner.Log.Infof("sendRequest End: input[%p], http.Request[%p], http.Response[%p]", input, request, response)
		}()
	}

	// covert input into httpRequest
	if !isValidEndpoint(opts.Endpoint) {
		return output, NewErrParamInvalid("Endpoint")
	}

	var writers []io.Writer
	// tracker in OperationMetaData
	for _, w := range input.OpMetadata.Values(OpMetaKeyRequestBodyTracker) {
		if ww, ok := w.(io.Writer); ok {
			writers = append(writers, ww)
		}
	}
	// host & path
	var strUrl string
	if opts.EndpointProvider != nil {
		strUrl = opts.EndpointProvider.BuildURL(input)
	} else {
		host, path := buildURL(input, opts)
		strUrl = fmt.Sprintf("%s://%s%s", opts.Endpoint.Scheme, host, path)
	}

	// querys
	if len(input.Parameters) > 0 {
		var buf bytes.Buffer
		for k, v := range input.Parameters {
			if buf.Len() > 0 {
				buf.WriteByte('&')
			}
			buf.WriteString(url.QueryEscape(k))
			if len(v) > 0 {
				buf.WriteString("=" + strings.Replace(url.QueryEscape(v), "+", "%20", -1))
			}
		}
		strUrl += "?" + buf.String()
	}

	request, err = http.NewRequestWithContext(ctx, input.Method, strUrl, nil)
	if err != nil {
		return output, err
	}

	// headers
	for k, v := range input.Headers {
		if len(k) > 0 && len(v) > 0 {
			request.Header.Add(k, v)
		}
	}
	request.Header.Set("User-Agent", c.inner.UserAgent)

	// body
	var body io.Reader
	if input.Body == nil {
		body = strings.NewReader("")
	} else {
		body = input.Body
	}
	var length int64
	if clen := request.Header.Get("Content-Length"); clen != "" {
		length, _ = strconv.ParseInt(clen, 10, 64)
	} else {
		length = GetReaderLen(body)
	}
	if length >= 0 {
		request.ContentLength = length
	}
	request.Body = TeeReadNopCloser(body, writers...)

	//signing context
	subResource, _ := input.OpMetadata.Get(signer.SubResource).([]string)
	clockOffset := c.inner.ClockOffset
	signingCtx := &signer.SigningContext{
		Product:           Ptr(opts.Product),
		Region:            Ptr(opts.Region),
		Bucket:            input.Bucket,
		Key:               input.Key,
		Request:           request,
		SubResource:       subResource,
		AuthMethodQuery:   opts.AuthMethod != nil && *opts.AuthMethod == AuthMethodQuery,
		ClockOffset:       clockOffset,
		AdditionalHeaders: opts.AdditionalHeaders,
	}

	if date := request.Header.Get(HeaderOssDate); date != "" {
		signingCtx.Time, _ = http.ParseTime(date)
	} else if signTime, ok := input.OpMetadata.Get(signer.SignTime).(time.Time); ok {
		signingCtx.Time = signTime
	}

	// send http request
	response, err = c.sendHttpRequest(ctx, signingCtx, opts)

	if err != nil {
		return output, err
	}

	// covert http response into output context
	output = &OperationOutput{
		Input:       input,
		Status:      response.Status,
		StatusCode:  response.StatusCode,
		Body:        response.Body,
		Headers:     response.Header,
		httpRequest: request,
	}

	// save other info by Metadata filed, ex. retry detail info
	//output.OpMetadata.Set(...)
	if signingCtx.AuthMethodQuery {
		output.OpMetadata.Set(signer.SignTime, signingCtx.Time)
	}

	if signingCtx.ClockOffset != clockOffset {
		c.inner.ClockOffset = signingCtx.ClockOffset
	}

	return output, err
}

func (c *Client) sendHttpRequest(ctx context.Context, signingCtx *signer.SigningContext, opts *Options) (response *http.Response, err error) {
	request := signingCtx.Request
	retryer := opts.Retryer
	maxAttempts := c.retryMaxAttempts(opts)
	body, _ := request.Body.(*teeReadNopCloser)
	resetTime := signingCtx.Time.IsZero()
	body.Mark()
	for tries := 1; tries <= maxAttempts; tries++ {
		if tries > 1 {
			delay, err := retryer.RetryDelay(tries, err)
			if err != nil {
				break
			}

			if err = sleepWithContext(ctx, delay); err != nil {
				err = &CanceledError{Err: err}
				break
			}

			if err = body.Reset(); err != nil {
				break
			}

			if resetTime {
				signingCtx.Time = time.Time{}
			}

			c.inner.Log.Infof("Attempt retry, request[%p], tries:%v, retry delay:%v", request, tries, delay)
		}

		if response, err = c.sendHttpRequestOnce(ctx, signingCtx, opts); err == nil {
			break
		}

		c.postSendHttpRequestOnce(signingCtx, response, err)

		if isContextError(ctx, &err) {
			err = &CanceledError{Err: err}
			break
		}

		if !body.IsSeekable() {
			break
		}

		if !retryer.IsErrorRetryable(err) {
			break
		}
	}
	return response, err
}

func (c *Client) sendHttpRequestOnce(ctx context.Context, signingCtx *signer.SigningContext, opts *Options) (
	response *http.Response, err error,
) {
	if c.getLogLevel() > LogInfo {
		c.inner.Log.Infof("sendHttpRequestOnce Start, http.Request[%p]", signingCtx.Request)
		defer func() {
			c.inner.Log.Infof("sendHttpRequestOnce End, http.Request[%p], response[%p], err:%v", signingCtx.Request, response, err)
		}()
	}

	if _, anonymous := opts.CredentialsProvider.(*credentials.AnonymousCredentialsProvider); !anonymous {
		cred, err := opts.CredentialsProvider.GetCredentials(ctx)
		if err != nil {
			return response, err
		}

		signingCtx.Credentials = &cred
		if err = c.options.Signer.Sign(ctx, signingCtx); err != nil {
			return response, err
		}
		c.inner.Log.Debugf("sendHttpRequestOnce::Sign request[%p], StringToSign:%s", signingCtx.Request, signingCtx.StringToSign)
	}

	c.logHttpPRequet(signingCtx.Request)

	if response, err = opts.HttpClient.Do(signingCtx.Request); err != nil {
		return response, err
	}

	c.logHttpResponse(signingCtx.Request, response)

	for _, fn := range opts.ResponseHandlers {
		if err = fn(response); err != nil {
			return response, err
		}
	}

	return response, err
}

func (c *Client) postSendHttpRequestOnce(signingCtx *signer.SigningContext, _ *http.Response, err error) {
	if err != nil {
		switch e := err.(type) {
		case *ServiceError:
			if c.hasFeature(FeatureCorrectClockSkew) &&
				e.Code == "RequestTimeTooSkewed" &&
				!e.Timestamp.IsZero() {
				signingCtx.ClockOffset = e.Timestamp.Sub(signingCtx.Time)
				c.inner.Log.Warnf("Got RequestTimeTooSkewed error, correct clock request[%p], ClockOffset:%v, Server Time:%v, Client time:%v",
					signingCtx.Request, signingCtx.ClockOffset, e.Timestamp, signingCtx.Time)
			}
		}
	}
}

func buildURL(input *OperationInput, opts *Options) (host string, path string) {
	if input == nil || opts == nil || opts.Endpoint == nil {
		return host, path
	}

	var paths []string
	if input.Bucket == nil {
		host = opts.Endpoint.Host
	} else {
		switch opts.UrlStyle {
		default: // UrlStyleVirtualHosted
			host = fmt.Sprintf("%s.%s", *input.Bucket, opts.Endpoint.Host)
		case UrlStylePath:
			host = opts.Endpoint.Host
			paths = append(paths, *input.Bucket)
			if input.Key == nil {
				paths = append(paths, "")
			}
		case UrlStyleCName:
			host = opts.Endpoint.Host
		}
	}

	if input.Key != nil {
		paths = append(paths, escapePath(*input.Key, false))
	}

	return host, ("/" + strings.Join(paths, "/"))
}

func serviceErrorResponseHandler(response *http.Response) error {
	if response.StatusCode/100 == 2 {
		return nil
	}
	return tryConvertServiceError(response)
}

func callbackErrorResponseHandler(response *http.Response) error {
	if response.StatusCode == 203 &&
		response.Request.Header.Get(HeaderOssCallback) != "" {
		return tryConvertServiceError(response)
	}
	return nil
}

func tryConvertServiceError(response *http.Response) (err error) {
	var respBody []byte
	var body []byte
	timestamp, err := time.Parse(http.TimeFormat, response.Header.Get("Date"))
	if err != nil {
		timestamp = time.Now()
	}

	defer response.Body.Close()
	respBody, err = io.ReadAll(response.Body)
	body = respBody
	if len(respBody) == 0 && len(response.Header.Get(HeaderOssERR)) > 0 {
		body, err = base64.StdEncoding.DecodeString(response.Header.Get(HeaderOssERR))
		if err != nil {
			body = respBody
		}
	}
	se := &ServiceError{
		StatusCode:    response.StatusCode,
		Code:          "BadErrorResponse",
		RequestID:     response.Header.Get(HeaderOssRequestID),
		Timestamp:     timestamp,
		RequestTarget: fmt.Sprintf("%s %s", response.Request.Method, response.Request.URL),
		Snapshot:      body,
		Headers:       response.Header,
	}

	if err != nil {
		se.Message = fmt.Sprintf("The body of the response was not readable, due to :%s", err.Error())
		return se
	}
	var tag string
	if strings.EqualFold(contentTypeJSON, response.Header.Get(HTTPHeaderContentType)) {
		var rawMap map[string]json.RawMessage
		if err = json.Unmarshal(body, &rawMap); err == nil {
			if errorData, ok := rawMap["Error"]; ok && len(errorData) > 0 {
				type ErrorRoot struct {
					Root json.RawMessage `json:"Error"`
				}
				var root ErrorRoot
				if err = json.Unmarshal(body, &root); err == nil {
					err = json.Unmarshal(root.Root, &se)
				}
			} else {
				se.EC = response.Header.Get(HeaderOssEC)
				code := extractStatusCode(response.Status)
				if code != "" {
					se.Code = code
				}
				if err = json.Unmarshal(body, &se); err == nil {
					err = json.Unmarshal(body, &se)
				}
			}
		}

		tag = "json"
	} else {
		err = xml.Unmarshal(body, &se)
		tag = "xml"
	}
	if err != nil {
		len := len(body)
		if len > 256 {
			len = 256
		}
		se.Message = fmt.Sprintf("Failed to parse %s from response body due to: %s. With part response body %s.", tag, err.Error(), string(body[:len]))
		return se
	}
	return se
}

func nonStreamResponseHandler(response *http.Response) error {
	body := response.Body
	if body == nil {
		return nil
	}

	defer body.Close()
	val, err := io.ReadAll(body)

	if err == nil {
		response.Body = io.NopCloser(bytes.NewReader(val))
	}

	return err
}

func checkResponseHeaderCRC64(ccrc string, header http.Header) (err error) {
	if scrc := header.Get(HeaderOssCRC64); scrc != "" {
		if scrc != ccrc {
			return fmt.Errorf("crc is inconsistent, client %s, server %s", ccrc, scrc)
		}
	}
	return nil
}

func applyOperationOpt(c *Options, op *Options) {
	if c == nil || op == nil {
		return
	}

	if op.Endpoint != nil {
		c.Endpoint = op.Endpoint
	}

	if ToInt(op.RetryMaxAttempts) > 0 {
		c.RetryMaxAttempts = op.RetryMaxAttempts
	}

	if op.Retryer != nil {
		c.Retryer = op.Retryer
	}

	if c.Retryer == nil {
		c.Retryer = retry.NopRetryer{}
	}

	if op.OpReadWriteTimeout != nil {
		c.OpReadWriteTimeout = op.OpReadWriteTimeout
	}

	if op.HttpClient != nil {
		c.HttpClient = op.HttpClient
	}

	if op.AuthMethod != nil {
		c.AuthMethod = op.AuthMethod
	}

	//response handler
	handlers := []func(*http.Response) error{
		serviceErrorResponseHandler,
	}
	handlers = append(handlers, c.ResponseHandlers...)
	handlers = append(handlers, op.ResponseHandlers...)
	c.ResponseHandlers = handlers
}

func applyOperationContext(ctx context.Context, c *Options) context.Context {
	if ctx == nil || c.OpReadWriteTimeout == nil {
		return ctx
	}
	return context.WithValue(ctx, "OpReadWriteTimeout", c.OpReadWriteTimeout)
}

func applyOperationMetadata(input *OperationInput, c *Options) {
	for _, h := range input.OpMetadata.Values(OpMetaKeyResponsHandler) {
		if hh, ok := h.(func(*http.Response) error); ok {
			c.ResponseHandlers = append(c.ResponseHandlers, hh)
		}
	}
}

// fieldInfo holds details for the input/output of a single field.
type fieldInfo struct {
	idx   int
	flags int
}

const (
	fRequire int = 1 << iota

	fTypeUsermeta
	fTypeXml
	fTypeTime
)

func parseFiledFlags(tokens []string) int {
	var flags int = 0
	for _, token := range tokens {
		switch token {
		case "required":
			flags |= fRequire
		case "time":
			flags |= fTypeTime
		case "xml":
			flags |= fTypeXml
		case "usermeta":
			flags |= fTypeUsermeta
		}
	}
	return flags
}

func validateInput(input *OperationInput) error {
	if input == nil {
		return NewErrParamNull("OperationInput")
	}

	if input.Bucket != nil {
		if input.OpMetadata.Get(OpMetaKeyIsBucketArn) == true {
			return AssertValidateArnBucket(ToString(input.Bucket))
		} else {
			if !IsValidBucketName(input.Bucket) {
				return NewErrParamInvalid("OperationInput.Bucket")
			}
		}

	}

	if input.Key != nil && !isValidObjectName(input.Key) {
		return NewErrParamInvalid("OperationInput.Key")
	}

	if !isValidMethod(input.Method) {
		return NewErrParamInvalid("OperationInput.Method")
	}

	return nil
}

func (c *Client) marshalInput(request any, input *OperationInput, handlers ...func(any, *OperationInput) error) error {
	// merge common fields
	if cm, ok := request.(RequestCommonInterface); ok {
		h, p, b := cm.GetCommonFileds()
		// headers
		if len(h) > 0 {
			if input.Headers == nil {
				input.Headers = map[string]string{}
			}
			for k, v := range h {
				input.Headers[k] = v
			}
		}

		// parameters
		if len(p) > 0 {
			if input.Parameters == nil {
				input.Parameters = map[string]string{}
			}
			for k, v := range p {
				input.Parameters[k] = v
			}
		}

		// body
		input.Body = b
	}

	val := reflect.ValueOf(request)
	switch val.Kind() {
	case reflect.Pointer, reflect.Interface:
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct || input == nil {
		return nil
	}

	t := val.Type()
	for k := 0; k < t.NumField(); k++ {
		if tag, ok := t.Field(k).Tag.Lookup("input"); ok {
			// header|query|body,filed_name,[required,time,usermeta...]
			v := val.Field(k)
			var flags int = 0
			tokens := strings.Split(tag, ",")
			if len(tokens) < 2 {
				continue
			}

			// parse field flags
			if len(tokens) > 2 {
				flags = parseFiledFlags(tokens[2:])
			}
			// check required flag
			if isEmptyValue(v) {
				if flags&fRequire != 0 {
					return NewErrParamRequired(t.Field(k).Name)
				}
				continue
			}

			switch tokens[0] {
			case "query":
				if input.Parameters == nil {
					input.Parameters = map[string]string{}
				}
				if v.Kind() == reflect.Pointer {
					v = v.Elem()
				}
				input.Parameters[tokens[1]] = fmt.Sprintf("%v", v.Interface())
			case "header":
				if input.Headers == nil {
					input.Headers = map[string]string{}
				}
				if v.Kind() == reflect.Pointer {
					v = v.Elem()
				}
				if flags&fTypeUsermeta != 0 {
					if m, ok := v.Interface().(map[string]string); ok {
						for k, v := range m {
							input.Headers[tokens[1]+k] = v
						}
					}
				} else {
					input.Headers[tokens[1]] = fmt.Sprintf("%v", v.Interface())
				}
			case "body":
				if flags&fTypeXml != 0 {
					var b bytes.Buffer
					if err := xml.NewEncoder(&b).EncodeElement(
						v.Interface(),
						xml.StartElement{Name: xml.Name{Local: tokens[1]}}); err != nil {
						return &SerializationError{
							Err: err,
						}
					}
					input.Body = bytes.NewReader(b.Bytes())
				} else {
					if r, ok := v.Interface().(io.Reader); ok {
						input.Body = r
					} else {
						return NewErrParamTypeNotSupport(t.Field(k).Name)
					}
				}
			}
		}
	}

	if err := validateInput(input); err != nil {
		return err
	}

	for _, h := range handlers {
		if err := h(request, input); err != nil {
			return err
		}
	}

	return nil
}

func marshalRestoreObject(request any, input *OperationInput) error {
	var builder strings.Builder
	roRequest := request.(*RestoreObjectRequest)
	if roRequest.RestoreRequest == nil {
		return nil
	}

	restoreRequest := roRequest.RestoreRequest
	builder.WriteString("<RestoreRequest>")
	builder.WriteString("<Days>")
	builder.WriteString(strconv.Itoa(int(restoreRequest.Days)))
	builder.WriteString("</Days>")
	if restoreRequest.JobParameters != nil {
		builder.WriteString("<JobParameters>")
		if restoreRequest.JobParameters.Tier != nil {
			builder.WriteString("<Tier>")
			builder.WriteString(*restoreRequest.JobParameters.Tier)
			builder.WriteString("</Tier>")
		}
		builder.WriteString("</JobParameters>")
	} else if restoreRequest.Tier != nil {
		builder.WriteString("<JobParameters>")
		builder.WriteString("<Tier>")
		builder.WriteString(*restoreRequest.Tier)
		builder.WriteString("</Tier>")
		builder.WriteString("</JobParameters>")
	}
	builder.WriteString("</RestoreRequest>")
	input.Body = strings.NewReader(builder.String())
	return nil
}

func marshalDeleteObjects(request any, input *OperationInput) error {
	var builder strings.Builder
	delRequest := request.(*DeleteMultipleObjectsRequest)
	objects := delRequest.Objects
	quiet := delRequest.Quiet
	if delRequest.Delete != nil {
		objects = delRequest.Delete.Objects
		quiet = delRequest.Delete.Quiet
	}

	if len(objects) == 0 {
		return NewErrParamRequired("Objects")
	}

	builder.WriteString("<Delete>")
	builder.WriteString("<Quiet>")
	builder.WriteString(strconv.FormatBool(quiet))
	builder.WriteString("</Quiet>")
	if len(objects) > 0 {
		for _, object := range objects {
			builder.WriteString("<Object>")
			if object.Key != nil {
				builder.WriteString("<Key>")
				builder.WriteString(escapeXml(*object.Key))
				builder.WriteString("</Key>")
			}
			if object.VersionId != nil {
				builder.WriteString("<VersionId>")
				builder.WriteString(*object.VersionId)
				builder.WriteString("</VersionId>")
			}
			builder.WriteString("</Object>")
		}
	} else {
		return NewErrParamInvalid("Objects")
	}
	builder.WriteString("</Delete>")
	input.Body = strings.NewReader(builder.String())
	return nil
}

func discardBody(result any, output *OperationOutput) error {
	var err error
	if output.Body != nil {
		defer output.Body.Close()
		_, err = io.Copy(io.Discard, output.Body)
	}
	return err
}

func unmarshalBodyXml(result any, output *OperationOutput) error {
	var err error
	var body []byte
	if output.Body != nil {
		defer output.Body.Close()
		if body, err = io.ReadAll(output.Body); err != nil {
			return err
		}
	}

	if len(body) > 0 {
		if err = xml.Unmarshal(body, result); err != nil {
			err = &DeserializationError{
				Err:      err,
				Snapshot: body,
			}
		}
	}
	return err
}

func unmarshalBodyXmlMix(result any, output *OperationOutput) error {
	var err error
	var body []byte
	if output.Body != nil {
		defer output.Body.Close()
		if body, err = io.ReadAll(output.Body); err != nil {
			return err
		}
	}

	if len(body) == 0 {
		return nil
	}

	val := reflect.ValueOf(result)
	switch val.Kind() {
	case reflect.Pointer, reflect.Interface:
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct || output == nil {
		return nil
	}

	t := val.Type()
	idx := -1
	for k := 0; k < t.NumField(); k++ {
		if tag, ok := t.Field(k).Tag.Lookup("output"); ok {
			tokens := strings.Split(tag, ",")
			if len(tokens) < 2 {
				continue
			}
			// header|query|body,filed_name,[required,time,usermeta...]
			switch tokens[0] {
			case "body":
				idx = k
				break
			}
		}
	}

	if idx >= 0 {
		dst := val.Field(idx)
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		err = xml.Unmarshal(body, dst.Interface())
	} else {
		err = xml.Unmarshal(body, result)
	}

	if err != nil {
		err = &DeserializationError{
			Err:      err,
			Snapshot: body,
		}
	}

	return err
}

func unmarshalBodyXmlVersions(result any, output *OperationOutput) error {
	var err error
	var body []byte
	if output.Body != nil {
		defer output.Body.Close()
		if body, err = io.ReadAll(output.Body); err != nil {
			return err
		}
	}

	if len(body) > 0 {
		oldStrings := []string{"<Version>", "</Version>", "<DeleteMarker>", "</DeleteMarker>"}
		newStrings := []string{"<ObjectMix>", "</ObjectMix>", "<ObjectMix>", "</ObjectMix>"}

		replacedData := string(body)
		for i := range oldStrings {
			replacedData = strings.Replace(replacedData, oldStrings[i], newStrings[i], -1)
		}
		if err = xml.Unmarshal([]byte(replacedData), result); err != nil {
			err = &DeserializationError{
				Err:      err,
				Snapshot: body,
			}
		}
	}
	return err
}

func unmarshalBodyDefault(result any, output *OperationOutput) error {
	var err error
	var body []byte
	if output.Body != nil {
		defer output.Body.Close()
		if body, err = io.ReadAll(output.Body); err != nil {
			return err
		}
	}

	// extract body
	if len(body) > 0 {
		contentType := output.Headers.Get("Content-Type")
		switch contentType {
		case "application/xml":
			err = xml.Unmarshal(body, result)
		case "application/json":
			err = json.Unmarshal(body, result)
		case "application/json;charset=utf-8":
			err = json.Unmarshal(body, result)
		default:
			err = fmt.Errorf("unsupport contentType:%s", contentType)
		}

		if err != nil {
			err = &DeserializationError{
				Err:      err,
				Snapshot: body,
			}
		}
	}
	return err
}

func unmarshalCallbackBody(result any, output *OperationOutput) error {
	var err error
	var body []byte
	if output.Body != nil {
		defer output.Body.Close()
		if body, err = io.ReadAll(output.Body); err != nil {
			return err
		}
	}
	if len(body) > 0 {
		switch r := result.(type) {
		case *PutObjectResult:
			if err = json.Unmarshal(body, &r.CallbackResult); err != nil {
				return err
			}
		case *CompleteMultipartUploadResult:
			if err = json.Unmarshal(body, &r.CallbackResult); err != nil {
				return err
			}
		}
	}
	return err
}

func unmarshalHeader(result any, output *OperationOutput) error {
	val := reflect.ValueOf(result)
	switch val.Kind() {
	case reflect.Pointer, reflect.Interface:
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct || output == nil {
		return nil
	}

	filedInfos := map[string]fieldInfo{}

	t := val.Type()
	var usermetaKeys []string
	for k := 0; k < t.NumField(); k++ {
		if tag, ok := t.Field(k).Tag.Lookup("output"); ok {
			tokens := strings.Split(tag, ",")
			if len(tokens) < 2 {
				continue
			}
			// header|query|body,filed_name,[required,time,usermeta...]
			switch tokens[0] {
			case "header":
				lowkey := strings.ToLower(tokens[1])

				var flags int = 0
				if len(tokens) >= 3 {
					flags = parseFiledFlags(tokens[2:])
				}
				filedInfos[lowkey] = fieldInfo{idx: k, flags: flags}
				if flags&fTypeUsermeta != 0 {
					usermetaKeys = append(usermetaKeys, lowkey)
				}
			}
		}
	}
	var err error
	for key, vv := range output.Headers {
		lkey := strings.ToLower(key)
		for _, prefix := range usermetaKeys {
			if strings.HasPrefix(lkey, prefix) {
				if field, ok := filedInfos[prefix]; ok {
					if field.flags&fTypeUsermeta != 0 {
						mapKey := strings.TrimPrefix(lkey, prefix)
						err = setMapStringReflectValue(val.Field(field.idx), mapKey, vv[0])
					}
				}
			}
		}
		if field, ok := filedInfos[lkey]; ok {
			if field.flags&fTypeTime != 0 {
				if t, err := http.ParseTime(vv[0]); err == nil {
					err = setTimeReflectValue(val.Field(field.idx), t)
				}
			} else {
				err = setReflectValue(val.Field(field.idx), vv[0])
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func unmarshalHeaderLite(result any, output *OperationOutput) error {
	val := reflect.ValueOf(result)
	switch val.Kind() {
	case reflect.Pointer, reflect.Interface:
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct || output == nil {
		return nil
	}

	t := val.Type()
	for k := 0; k < t.NumField(); k++ {
		if tag := t.Field(k).Tag.Get("output"); tag != "" {
			tokens := strings.Split(tag, ",")
			if len(tokens) != 2 {
				continue
			}
			switch tokens[0] {
			case "header":
				if src := output.Headers.Get(tokens[1]); src != "" {
					if err := setReflectValue(val.Field(k), src); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (c *Client) unmarshalOutput(result any, output *OperationOutput, handlers ...func(any, *OperationOutput) error) error {
	// Common
	if cm, ok := result.(ResultCommonInterface); ok {
		cm.CopyIn(output.Status, output.StatusCode, output.Headers, output.OpMetadata)
	}

	var err error
	for _, h := range handlers {
		if err = h(result, output); err != nil {
			break
		}
	}
	return err
}

func updateContentMd5(_ any, input *OperationInput) error {
	var err error
	var contentMd5 string
	if input.Body != nil {
		var r io.ReadSeeker
		var ok bool
		if r, ok = input.Body.(io.ReadSeeker); !ok {
			buf, _ := io.ReadAll(input.Body)
			r = bytes.NewReader(buf)
			input.Body = r
		}
		h := md5.New()
		if _, err = copySeekableBody(h, r); err != nil {
			// error
		} else {
			contentMd5 = base64.StdEncoding.EncodeToString(h.Sum(nil))
		}
	} else {
		contentMd5 = "1B2M2Y8AsgTpgAmY7PhCfg=="
	}

	// set content-md5 and content-type
	if err == nil {
		if input.Headers == nil {
			input.Headers = map[string]string{}
		}
		input.Headers["Content-MD5"] = contentMd5
	}

	return err
}

func updateContentType(_ any, input *OperationInput) error {
	if input.Headers == nil {
		input.Headers = map[string]string{}
	}
	if _, ok := input.Headers[HTTPHeaderContentType]; !ok {
		value := TypeByExtension(ToString(input.Key))
		if value == "" {
			value = contentTypeDefault
		}
		input.Headers[HTTPHeaderContentType] = value
	}
	return nil
}

func addProgress(request any, input *OperationInput) error {
	var w io.Writer
	switch req := request.(type) {
	case *PutObjectRequest:
		if req.ProgressFn == nil {
			return nil
		}
		w = NewProgress(req.ProgressFn, GetReaderLen(input.Body))
	case *AppendObjectRequest:
		if req.ProgressFn == nil {
			return nil
		}
		w = NewProgress(req.ProgressFn, GetReaderLen(input.Body))
	case *UploadPartRequest:
		if req.ProgressFn == nil {
			return nil
		}
		w = NewProgress(req.ProgressFn, GetReaderLen(input.Body))
	default:
		return nil
	}
	input.OpMetadata.Add(OpMetaKeyRequestBodyTracker, w)
	return nil
}

func addProcess(request any, input *OperationInput) error {
	switch req := request.(type) {
	case *ProcessObjectRequest:
		if req.Process == nil {
			return nil
		}
		processData := fmt.Sprintf("%v=%v", "x-oss-process", ToString(req.Process))
		input.Body = strings.NewReader(processData)
	case *AsyncProcessObjectRequest:
		if req.AsyncProcess == nil {
			return nil
		}
		processData := fmt.Sprintf("%v=%v", "x-oss-async-process", ToString(req.AsyncProcess))
		input.Body = strings.NewReader(processData)
	default:
		return nil
	}

	return nil
}

func addCrcCheck(_ any, input *OperationInput) error {
	var w io.Writer = NewCRC64(0)
	input.OpMetadata.Add(OpMetaKeyRequestBodyTracker, w)
	input.OpMetadata.Add(OpMetaKeyResponsHandler, func(response *http.Response) error {
		return checkResponseHeaderCRC64(fmt.Sprint(w.(hash.Hash64).Sum64()), response.Header)
	})
	return nil
}

func addCallback(_ any, input *OperationInput) error {
	input.OpMetadata.Add(OpMetaKeyResponsHandler, callbackErrorResponseHandler)
	return nil
}

func enableNonStream(_ any, input *OperationInput) error {
	input.OpMetadata.Add(OpMetaKeyResponsHandler, func(response *http.Response) error {
		return nonStreamResponseHandler(response)
	})
	return nil
}

func (c *Client) updateContentType(request any, input *OperationInput) error {
	if !c.hasFeature(FeatureAutoDetectMimeType) {
		return nil
	}
	return updateContentType(request, input)
}

func (c *Client) addCrcCheck(request any, input *OperationInput) error {
	if !c.hasFeature(FeatureEnableCRC64CheckUpload) {
		return nil
	}
	return addCrcCheck(request, input)
}

func encodeSourceObject(request any) string {
	var bucket, key, versionId string
	switch req := request.(type) {
	case *CopyObjectRequest:
		key = ToString(req.SourceKey)
		if req.SourceBucket != nil {
			bucket = *req.SourceBucket
		} else {
			bucket = ToString(req.Bucket)
		}
		versionId = ToString(req.SourceVersionId)
	case *UploadPartCopyRequest:
		key = ToString(req.SourceKey)
		if req.SourceBucket != nil {
			bucket = *req.SourceBucket
		} else {
			bucket = ToString(req.Bucket)
		}
		versionId = ToString(req.SourceVersionId)
	}

	source := fmt.Sprintf("/%s/%s", bucket, escapePath(key, false))
	if versionId != "" {
		source += "?versionId=" + versionId
	}

	return source
}

func (c *Client) toClientError(err error, code string, output *OperationOutput) error {
	if err == nil {
		return nil
	}

	return &ClientError{
		Code: code,
		Message: fmt.Sprintf("execute %s fail, error code is %s, request id:%s",
			output.Input.OpName,
			code,
			output.Headers.Get(HeaderOssRequestID),
		),
		Err: err}
}

func (c *Client) hasFeature(flag FeatureFlagsType) bool {
	return (c.options.FeatureFlags & flag) > 0
}

func (c *Client) retryMaxAttempts(opts *Options) int {
	if opts == nil {
		opts = &c.options
	}

	if opts.RetryMaxAttempts != nil {
		return ToInt(opts.RetryMaxAttempts)
	}

	if opts.Retryer != nil {
		return opts.Retryer.MaxAttempts()
	}

	return retry.DefaultMaxAttempts
}

func (c *Client) dumpOperationOutput(output *OperationOutput) string {
	if output == nil {
		return ""
	}
	return fmt.Sprintf("http.Request[%p] Status:%v, StatusCode%v, RequestId:%v",
		output.httpRequest, output.Status, output.StatusCode,
		output.Headers.Get(HeaderOssRequestID),
	)
}

// LoggerHTTPReq Print the header information of the http request
func (c *Client) logHttpPRequet(request *http.Request) {
	if c.getLogLevel() < LogDebug {
		return
	}
	var logBuffer bytes.Buffer
	logBuffer.WriteString(fmt.Sprintf("http.request[%p]", request))
	if request != nil {
		logBuffer.WriteString(fmt.Sprintf("Method:%s\t", request.Method))
		logBuffer.WriteString(fmt.Sprintf("Host:%s\t", request.URL.Host))
		logBuffer.WriteString(fmt.Sprintf("Path:%s\t", request.URL.Path))
		logBuffer.WriteString(fmt.Sprintf("Query:%s\t", request.URL.RawQuery))
		logBuffer.WriteString(fmt.Sprintf("Header info:"))

		for k, v := range request.Header {
			var valueBuffer bytes.Buffer
			for j := 0; j < len(v); j++ {
				if j > 0 {
					valueBuffer.WriteString(" ")
				}
				valueBuffer.WriteString(v[j])
			}
			logBuffer.WriteString(fmt.Sprintf("\t%s:%s", k, valueBuffer.String()))
		}
	}

	c.inner.Log.Debugf("%s", logBuffer.String())
}

// LoggerHTTPResp Print Response to http request
func (c *Client) logHttpResponse(request *http.Request, response *http.Response) {
	if c.getLogLevel() < LogDebug {
		return
	}
	var logBuffer bytes.Buffer
	logBuffer.WriteString(fmt.Sprintf("http.request[%p]|http.response[%p]", request, response))
	if response != nil {
		logBuffer.WriteString(fmt.Sprintf("StatusCode:%d\t", response.StatusCode))
		logBuffer.WriteString(fmt.Sprintf("Header info:"))
		for k, v := range response.Header {
			var valueBuffer bytes.Buffer
			for j := 0; j < len(v); j++ {
				if j > 0 {
					valueBuffer.WriteString(" ")
				}
				valueBuffer.WriteString(v[j])
			}
			logBuffer.WriteString(fmt.Sprintf("\t%s:%s", k, valueBuffer.String()))
		}
	}
	c.inner.Log.Debugf("%s", logBuffer.String())
}

func (c *Client) getLogLevel() int {
	if c.inner.Log != nil {
		return c.inner.Log.Level()
	}
	return LogOff
}

// Content-Type
const (
	contentTypeDefault = "application/octet-stream"
	contentTypeXML     = "application/xml"
	contentTypeJSON    = "application/json"
)

// Exposed to external modules
func MarshalUpdateContentMd5(request any, input *OperationInput) error {
	return updateContentMd5(request, input)
}

func UnmarshalDiscardBody(result any, output *OperationOutput) error {
	return discardBody(result, output)
}

// MarshalInput marshals the request into operation input (exposed to external modules)
func (c *Client) MarshalInput(request any, input *OperationInput, handlers ...func(any, *OperationInput) error) error {
	return c.marshalInput(request, input, handlers...)
}

// UnmarshalOutput unmarshals the operation output into the result (exposed to external modules)
func (c *Client) UnmarshalOutput(result any, output *OperationOutput, handlers ...func(any, *OperationOutput) error) error {
	return c.unmarshalOutput(result, output, handlers...)
}

// ToClientError converts an error to a client error (exposed to external modules)
func (c *Client) ToClientError(err error, opName string, output *OperationOutput) error {
	return c.toClientError(err, opName, output)
}
