package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type MirrorHeaders struct {
	// Specifies whether to pass through all request headers other than the following headers to the origin. This parameter takes effect only when the value of RedirectType is Mirror.*   Headers such as content-length, authorization2, authorization, range, and date*   Headers that start with oss-, x-oss-, and x-drs-Default value: false.Valid values:*   true            *   false
	PassAll *bool `xml:"PassAll"`

	// The headers to pass through to the origin. This parameter takes effect only when the value of RedirectType is Mirror. Each specified header can be up to 1,024 bytes in length and can contain only letters, digits, and hyphens (-). You can specify up to 10 headers.
	Passes []string `xml:"Pass"`

	// The headers that are not allowed to pass through to the origin. This parameter takes effect only when the value of RedirectType is Mirror. Each header can be up to 1,024 bytes in length and can contain only letters, digits, and hyphens (-). You can specify up to 10 headers. This parameter is used together with PassAll.
	Removes []string `xml:"Remove"`

	// The headers that are sent to the origin. The specified headers are configured in the data returned by the origin regardless of whether the headers are contained in the request. This parameter takes effect only when the value of RedirectType is Mirror. You can specify up to 10 headers.
	Sets []MirrorHeadersSet `xml:"Set"`
}

type RoutingRule struct {
	// The sequence number that is used to match and run the redirection rules. OSS matches redirection rules based on this parameter. If a match succeeds, only the rule is run and the subsequent rules are not run.  This parameter must be specified if RoutingRule is specified.
	RuleNumber *int64 `xml:"RuleNumber"`

	// The matching condition. If all of the specified conditions are met, the rule is run. A rule is considered matched only when the rule meets the conditions that are specified by all nodes in Condition.  This parameter must be specified if RoutingRule is specified.
	Condition *RoutingRuleCondition `xml:"Condition"`

	// The operation to perform after the rule is matched.  This parameter must be specified if RoutingRule is specified.
	Redirect *RoutingRuleRedirect `xml:"Redirect"`

	// The Lua script config of this rule.
	LuaConfig *RoutingRuleLuaConfig `xml:"LuaConfig"`
}

type RoutingRuleLuaConfig struct {
	// The name of the Lua script.
	Script *string `xml:"Script"`
}

type WebsiteConfiguration struct {
	// The container that stores the default homepage.  You must specify at least one of the following containers: IndexDocument, ErrorDocument, and RoutingRules.
	IndexDocument *IndexDocument `xml:"IndexDocument"`

	// The container that stores the default 404 page.  You must specify at least one of the following containers: IndexDocument, ErrorDocument, and RoutingRules.
	ErrorDocument *ErrorDocument `xml:"ErrorDocument"`

	// The container that stores the redirection rules.  You must specify at least one of the following containers: IndexDocument, ErrorDocument, and RoutingRules.
	RoutingRules *RoutingRules `xml:"RoutingRules"`
}

type IndexDocument struct {
	// The default homepage.
	Suffix *string `xml:"Suffix"`

	// Specifies whether to redirect the access to the default homepage of the subdirectory when the subdirectory is accessed. Valid values:*   **true**: The access is redirected to the default homepage of the subdirectory.*   **false** (default): The access is redirected to the default homepage of the root directory.For example, the default homepage is set to index.html, and `bucket.oss-cn-hangzhou.aliyuncs.com/subdir/` is the site that you want to access. If **SupportSubDir** is set to false, the access is redirected to `bucket.oss-cn-hangzhou.aliyuncs.com/index.html`. If **SupportSubDir** is set to true, the access is redirected to `bucket.oss-cn-hangzhou.aliyuncs.com/subdir/index.html`.
	SupportSubDir *bool `xml:"SupportSubDir"`

	// The operation to perform when the default homepage is set, the name of the accessed object does not end with a forward slash (/), and the object does not exist. This parameter takes effect only when **SupportSubDir** is set to true. It takes effect after RoutingRule but before ErrorFile. For example, the default homepage is set to index.html, `bucket.oss-cn-hangzhou.aliyuncs.com/abc` is the site that you want to access, and the abc object does not exist. In this case, different operations are performed based on the value of **Type**.*   **0** (default): OSS checks whether the object named abc/index.html, which is in the `Object + Forward slash (/) + Homepage` format, exists. If the object exists, OSS returns HTTP status code 302 and the Location header value that contains URL-encoded `/abc/`. The URL-encoded /abc/ is in the `Forward slash (/) + Object + Forward slash (/)` format. If the object does not exist, OSS returns HTTP status code 404 and continues to check ErrorFile.*   **1**: OSS returns HTTP status code 404 and the NoSuchKey error code and continues to check ErrorFile.*   **2**: OSS checks whether abc/index.html exists. If abc/index.html exists, the content of the object is returned. If abc/index.html does not exist, OSS returns HTTP status code 404 and continues to check ErrorFile.
	Type *int64 `xml:"Type"`
}

type ErrorDocument struct {
	// The error page.
	Key *string `xml:"Key"`

	// The HTTP status code returned with the error page.
	HttpStatus *int64 `xml:"HttpStatus"`
}

type RoutingRuleIncludeHeader struct {
	// The key of the header. The rule is matched only when the specified header is included in the request and the header value equals the value specified by Equals.
	Key *string `xml:"Key"`

	// The value of the header. The rule is matched only when the header specified by Key is included in the request and the header value equals the specified value.
	Equals *string `xml:"Equals"`
}

type RoutingRuleCondition struct {
	// The prefix of object names. Only objects whose names contain the specified prefix match the rule.
	KeyPrefixEquals *string `xml:"KeyPrefixEquals"`

	// Only objects that match this suffix can match this rule.
	KeySuffixEquals *string `xml:"KeySuffixEquals"`

	// The HTTP status code. The rule is matched only when the specified object is accessed and the specified HTTP status code is returned. If the redirection rule is the mirroring-based back-to-origin rule, the value of this parameter is 404.
	HttpErrorCodeReturnedEquals *int64 `xml:"HttpErrorCodeReturnedEquals"`

	// This rule can only be matched if the request contains the specified header and the value is the specified value. This container can specify up to 10.
	IncludeHeaders []RoutingRuleIncludeHeader `xml:"IncludeHeader"`
}

type MirrorHeadersSet struct {
	// The key of the header. The key can be up to 1,024 bytes in length and can contain only letters, digits, and hyphens (-). This parameter takes effect only when the value of RedirectType is Mirror.  This parameter must be specified if Set is specified.
	Key *string `xml:"Key"`

	// The value of the header. The value can be up to 1,024 bytes in length and cannot contain `\r\n`. This parameter takes effect only when the value of RedirectType is Mirror.  This parameter must be specified if Set is specified.
	Value *string `xml:"Value"`
}

type RoutingRuleRedirect struct {
	// The origin URL for mirroring-based back-to-origin. This parameter takes effect only when the value of RedirectType is Mirror. The origin URL must start with \*\*http://** or **https://\*\* and end with a forward slash (/). OSS adds an object name to the end of the URL to generate a back-to-origin URL. For example, the name of the object to access is myobject. If MirrorURL is set to `http://example.com/`, the back-to-origin URL is `http://example.com/myobject`. If MirrorURL is set to `http://example.com/dir1/`, the back-to-origin URL is `http://example.com/dir1/myobject`.  This parameter must be specified if RedirectType is set to Mirror.Valid values:*   true            *   false
	MirrorURL *string `xml:"MirrorURL"`

	// Specifies whether to redirect the access to the address specified by Location if the origin returns an HTTP 3xx status code. This parameter takes effect only when the value of RedirectType is Mirror. For example, when a mirroring-based back-to-origin request is initiated, the origin returns 302 and Location is specified.*   If you set MirrorFollowRedirect to true, OSS continues requesting the resource at the address specified by Location. The access can be redirected up to 10 times. If the access is redirected more than 10 times, the mirroring-based back-to-origin request fails.*   If you set MirrorFollowRedirect to false, OSS returns 302 and passes through Location.Default value: true.
	MirrorFollowRedirect *bool `xml:"MirrorFollowRedirect"`

	// If this parameter is set to true, the prefix of the object names is replaced with the value specified by ReplaceKeyPrefixWith. If this parameter is not specified or empty, the prefix of object names is truncated.  When the ReplaceKeyWith parameter is not empty, the EnableReplacePrefix parameter cannot be set to true.Default value: false.
	EnableReplacePrefix *bool `xml:"EnableReplacePrefix"`

	// The string that is used to replace the requested object name when the request is redirected. This parameter can be set to the ${key} variable, which indicates the object name in the request. For example, if ReplaceKeyWith is set to `prefix/${key}.suffix` and the object to access is test, the value of the Location header is `http://example.com/prefix/test.suffix`.
	ReplaceKeyWith *string `xml:"ReplaceKeyWith"`

	// The domain name used for redirection. The domain name must comply with the domain naming rules. For example, if you access an object named test, Protocol is set to https, and Hostname is set to `example.com`, the value of the Location header is `https://example.com/test`.
	HostName *string `xml:"HostName"`

	// Specifies whether to include parameters of the original request in the redirection request when the system runs the redirection rule or mirroring-based back-to-origin rule. For example, if the **PassQueryString** parameter is set to true, the `?a=b&c=d` parameter string is included in a request sent to OSS, and the redirection mode is 302, this parameter is added to the Location header. For example, if the request is `Location:example.com?a=b&c=d` and the redirection type is mirroring-based back-to-origin, the ?a=b\&c=d parameter string is also included in the back-to-origin request. Valid values: true and false (default).
	PassQueryString *bool `xml:"PassQueryString"`

	// The headers contained in the response that is returned when you use mirroring-based back-to-origin. This parameter takes effect only when the value of RedirectType is Mirror.
	MirrorHeaders *MirrorHeaders `xml:"MirrorHeaders"`

	// The string that is used to replace the prefix of the object name during redirection. If the prefix of an object name is empty, the string precedes the object name.  You can specify only one of the ReplaceKeyWith and ReplaceKeyPrefixWith parameters in a rule. For example, if you access an object named abc/test.txt, KeyPrefixEquals is set to abc/, ReplaceKeyPrefixWith is set to def/, the value of the Location header is `http://example.com/def/test.txt`.
	ReplaceKeyPrefixWith *string `xml:"ReplaceKeyPrefixWith"`

	// The redirection type. Valid values:*   **Mirror**: mirroring-based back-to-origin.*   **External**: external redirection. OSS returns an HTTP 3xx status code and returns an address for you to redirect to.*   **AliCDN**: redirection based on Alibaba Cloud CDN. Compared with external redirection, OSS adds an additional header to the request. After Alibaba Cloud CDN identifies the header, Alibaba Cloud CDN redirects the access to the specified address and returns the obtained data instead of the HTTP 3xx status code that redirects the access to another address.  This parameter must be specified if Redirect is specified.
	RedirectType *string `xml:"RedirectType"`

	// Is SNI transparent.
	MirrorSNI *bool `xml:"MirrorSNI"`

	// The protocol used for redirection. This parameter takes effect only when RedirectType is set to External or AliCDN. For example, if you access an object named test, Protocol is set to https, and Hostname is set to `example.com`, the value of the Location header is `https://example.com/test`. Valid values: **http** and **https**.
	Protocol *string `xml:"Protocol"`

	// Specifies whether to check the MD5 hash of the body of the response returned by the origin. This parameter takes effect only when the value of RedirectType is Mirror. When **MirrorCheckMd5** is set to true and the response returned by the origin includes the Content-Md5 header, OSS checks whether the MD5 hash of the obtained data matches the header value. If the MD5 hash of the obtained data does not match the header value, the obtained data is not stored in OSS. Default value: false.
	MirrorCheckMd5 *bool `xml:"MirrorCheckMd5"`

	// The HTTP redirect code in the response. This parameter takes effect only when RedirectType is set to External or AliCDN. Valid values: 301, 302, and 307.
	HttpRedirectCode *int64 `xml:"HttpRedirectCode"`

	// Is it transmitted transparently '/' to the source site
	MirrorPassOriginalSlashes *bool `xml:"MirrorPassOriginalSlashes"`

	// This parameter plays the same role as PassQueryString and has a higher priority than PassQueryString. This parameter takes effect only when the value of RedirectType is Mirror. Default value: false.Valid values:*   true            *   false
	MirrorPassQueryString *bool `xml:"MirrorPassQueryString"`

	// The HTTP status codes that trigger the asynchronous pull mode in mirror-based back-to-origin.
	MirrorAsyncStatus *int64 `xml:"MirrorAsyncStatus"`

	// The authentication information for the origin server in mirror-based back-to-origin.
	MirrorAuth *MirrorAuth `xml:"MirrorAuth"`

	// The probe URL for mirror-based back-to-origin.
	MirrorURLProbe *string `xml:"MirrorURLProbe"`

	// Whether to allow take video snapshot in mirror-based back-to-origin.
	MirrorAllowVideoSnapshot *bool `xml:"MirrorAllowVideoSnapshot"`

	// The slave URL for mirror-based back-to-origin.
	MirrorURLSlave *string `xml:"MirrorURLSlave"`

	// The VPC ID for mirror-based back-to-origin express tunnel.
	MirrorDstVpcId *string `xml:"MirrorDstVpcId"`

	// Use LastModifiedTime of the file from origin.
	MirrorUserLastModified *bool `xml:"MirrorUserLastModified"`

	// Whether to use role for mirror-based back-to-origin.
	MirrorUsingRole *bool `xml:"MirrorUsingRole"`

	// Mirror-based back-to-origin with express tunnel.
	MirrorIsExpressTunnel *bool `xml:"MirrorIsExpressTunnel"`

	// Not save data in web-based back-to-origin.
	MirrorProxyPass *bool `xml:"MirrorProxyPass"`

	// The rules for setting tags when saving files during mirror-based back-to-origin.
	MirrorTaggings *MirrorTaggings `xml:"MirrorTaggings"`

	// The slave VPC ID for mirror-based back-to-origin express tunnel.
	MirrorDstSlaveVpcId *string `xml:"MirrorDstSlaveVpcId"`

	// The VPC region for mirror-based back-to-origin express tunnel.
	MirrorDstRegion *string `xml:"MirrorDstRegion"`

	// Used for determining the state of primary-secondary switching. The logic for primary-secondary switching is based on the error code returned by the origin server. If MirrorSwitchAllErrors is set to true, all status codes except the following are considered failures: 200, 206, 301, 302, 303, 307, 404. If it is set to false, only status codes in the 5xx range or timeouts are considered failures.
	MirrorSwitchAllErrors *bool `xml:"MirrorSwitchAllErrors"`

	// The tunnel ID for mirror-based back-to-origin.
	MirrorTunnelId *string `xml:"MirrorTunnelId"`

	// The role name used for mirror-based back-to-origin.
	MirrorRole *string `xml:"MirrorRole"`

	// Whether to allow get image information in mirror-based back-to-origin.
	MirrorAllowGetImageInfo *bool `xml:"MirrorAllowGetImageInfo"`

	// Whether to store the user defined metadata in mirror-based back-to-origin.
	MirrorSaveOssMeta *bool `xml:"MirrorSaveOssMeta"`

	// Whether to allow take HeadObject in mirror-based back-to-origin.
	MirrorAllowHeadObject *bool `xml:"MirrorAllowHeadObject"`

	// The container to store the configuration for multiple origins in mirror-based back-to-origin.
	MirrorMultiAlternates *MirrorMultiAlternates `xml:"MirrorMultiAlternates"`

	// Specify which status codes returned by the origin server should be passed through to the client along with the body. The value should be HTTP status codes such as 4xx, 5xx, etc., separated by commas (,), for example, 400,404. This setting takes effect only when RedirectType is set to Mirror. When OSS requests content from the origin server, if the origin server returns one of the status codes specified in this parameter, OSS will pass through the status code and body returned by the origin server to the client. If the 404 status code is specified in this parameter, the configured ErrorDocument will be ineffective.
	TransparentMirrorResponseCodes *string `xml:"TransparentMirrorResponseCodes"`

	// Container to store the rules for setting response headers in mirror-based back-to-origin.
	MirrorReturnHeaders *MirrorReturnHeaders `xml:"MirrorReturnHeaders"`
}

type MirrorReturnHeaders struct {
	// The rule list for setting response headers in mirror-based back-to-origin.
	ReturnHeaders []ReturnHeader `xml:"ReturnHeader"`
}

type ReturnHeader struct {
	// The response header.
	Key *string `xml:"Key"`

	// The rule for setting response header value for a specific header.
	Value *string `xml:"Value"`
}

type MirrorMultiAlternates struct {
	// The configuration list for multiple origins.
	MirrorMultiAlternates []MirrorMultiAlternate `xml:"MirrorMultiAlternate"`
}

type MirrorMultiAlternate struct {
	// The region for a specific origin.
	MirrorMultiAlternateDstRegion *string `xml:"MirrorMultiAlternateDstRegion"`

	// The distinct number of a specific origin.
	MirrorMultiAlternateNumber *int64 `xml:"MirrorMultiAlternateNumber"`

	// The URL for a specific origin.
	MirrorMultiAlternateURL *string `xml:"MirrorMultiAlternateURL"`

	// The VPC ID for a specific origin.
	MirrorMultiAlternateVpcId *string `xml:"MirrorMultiAlternateVpcId"`
}

type MirrorAuth struct {
	// The authentication type.
	AuthType *string `xml:"AuthType"`

	// The sign region for signature.
	Region *string `xml:"Region"`

	// The access key id for signature.
	AccessKeyId *string `xml:"AccessKeyId"`

	// The access key secret for signature.
	AccessKeySecret *string `xml:"AccessKeySecret"`
}

type MirrorTaggings struct {
	// The rule list for setting tags.
	Taggings []MirrorTagging `xml:"Taggings"`
}

type MirrorTagging struct {
	// The tag key.
	Key *string `xml:"Key"`

	// The rule for setting tag value for a specific tag key.
	Value *string `xml:"Value"`
}

type RoutingRules struct {
	// The specified redirection rule or mirroring-based back-to-origin rule. You can specify up to 20 rules.
	RoutingRules []RoutingRule `xml:"RoutingRule"`
}

type GetBucketWebsiteRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type GetBucketWebsiteResult struct {
	// The containers of the website configuration.
	WebsiteConfiguration *WebsiteConfiguration `output:"body,WebsiteConfiguration,xml"`

	ResultCommon
}

// GetBucketWebsite Queries the static website hosting status and redirection rules configured for a bucket.
func (c *Client) GetBucketWebsite(ctx context.Context, request *GetBucketWebsiteRequest, optFns ...func(*Options)) (*GetBucketWebsiteResult, error) {
	var err error
	if request == nil {
		request = &GetBucketWebsiteRequest{}
	}
	input := &OperationInput{
		OpName: "GetBucketWebsite",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"website": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"website"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &GetBucketWebsiteResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type PutBucketWebsiteRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	WebsiteConfiguration *WebsiteConfiguration `input:"body,WebsiteConfiguration,xml,required"`

	RequestCommon
}

type PutBucketWebsiteResult struct {
	ResultCommon
}

// PutBucketWebsite Enables the static website hosting mode for a bucket and configures redirection rules for the bucket.
func (c *Client) PutBucketWebsite(ctx context.Context, request *PutBucketWebsiteRequest, optFns ...func(*Options)) (*PutBucketWebsiteResult, error) {
	var err error
	if request == nil {
		request = &PutBucketWebsiteRequest{}
	}
	input := &OperationInput{
		OpName: "PutBucketWebsite",
		Method: "PUT",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"website": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"website"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &PutBucketWebsiteResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}

type DeleteBucketWebsiteRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type DeleteBucketWebsiteResult struct {
	ResultCommon
}

// DeleteBucketWebsite Disables the static website hosting mode and deletes the redirection rules for a bucket.
func (c *Client) DeleteBucketWebsite(ctx context.Context, request *DeleteBucketWebsiteRequest, optFns ...func(*Options)) (*DeleteBucketWebsiteResult, error) {
	var err error
	if request == nil {
		request = &DeleteBucketWebsiteRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteBucketWebsite",
		Method: "DELETE",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"website": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"website"})
	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}
	result := &DeleteBucketWebsiteResult{}
	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}
	return result, err
}
