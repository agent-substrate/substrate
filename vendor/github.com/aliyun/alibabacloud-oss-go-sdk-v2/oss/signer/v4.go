package signer

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	// headers
	contentSha256Header   = "x-oss-content-sha256"
	iso8601DatetimeFormat = "20060102T150405Z"
	iso8601DateFormat     = "20060102"
	algorithmV4           = "OSS4-HMAC-SHA256"

	unsignedPayload = "UNSIGNED-PAYLOAD"
)

var noEscape [256]bool

func init() {
	for i := 0; i < len(noEscape); i++ {
		noEscape[i] = (i >= 'A' && i <= 'Z') ||
			(i >= 'a' && i <= 'z') ||
			(i >= '0' && i <= '9') ||
			i == '-' ||
			i == '.' ||
			i == '_' ||
			i == '~'
	}
}

func toString(p *string) (v string) {
	if p == nil {
		return v
	}
	return *p
}

func escapePath(path string, encodeSep bool) string {
	var buf bytes.Buffer
	for i := 0; i < len(path); i++ {
		c := path[i]
		if noEscape[c] || (c == '/' && !encodeSep) {
			buf.WriteByte(c)
		} else {
			fmt.Fprintf(&buf, "%%%02X", c)
		}
	}
	return buf.String()
}

func isDefaultSignedHeader(low string) bool {
	if strings.HasPrefix(low, ossHeaderPreifx) ||
		low == "content-type" ||
		low == "content-md5" {
		return true
	}
	return false
}

func getCommonAdditionalHeaders(header http.Header, additionalHeaders []string) []string {
	var keys []string
	for _, k := range additionalHeaders {
		lowK := strings.ToLower(k)
		if isDefaultSignedHeader(lowK) {
			//default signed header, skip
			continue
		} else if header.Get(lowK) != "" {
			keys = append(keys, lowK)
		}
	}
	sort.Strings(keys)
	return keys
}

type SignerV4 struct {
}

func (s *SignerV4) calcStringToSign(datetime, scope, canonicalRequest string) string {
	/**
	StringToSign
	"OSS4-HMAC-SHA256" + "\n" +
	TimeStamp + "\n" +
	Scope + "\n" +
	Hex(SHA256Hash(Canonical Request))
	*/
	hash256 := sha256.New()
	hash256.Write([]byte(canonicalRequest))
	hashValue := hash256.Sum(nil)
	canonicalHash := hex.EncodeToString(hashValue)

	return "OSS4-HMAC-SHA256" + "\n" +
		datetime + "\n" +
		scope + "\n" +
		canonicalHash
}

func (s *SignerV4) calcCanonicalRequest(signingCtx *SigningContext, additionalHeaders []string) string {
	request := signingCtx.Request
	/*
		Canonical Request
		HTTP Verb + "\n" +
		Canonical URI + "\n" +
		Canonical Query String + "\n" +
		Canonical Headers + "\n" +
		Additional Headers + "\n" +
		Hashed PayLoad
	*/

	//Canonical Uri
	uri := "/"
	if signingCtx.Bucket != nil {
		uri += *signingCtx.Bucket + "/"
	}
	if signingCtx.Key != nil {
		uri += *signingCtx.Key
	}
	canonicalUri := escapePath(uri, false)

	//Canonical Query
	query := strings.Replace(request.URL.RawQuery, "+", "%20", -1)
	values := make(map[string]string)
	var params []string
	for query != "" {
		var key string
		key, query, _ = strings.Cut(query, "&")
		if key == "" {
			continue
		}
		key, value, _ := strings.Cut(key, "=")
		values[key] = value
		params = append(params, key)
	}
	sort.Strings(params)
	var buf strings.Builder
	for _, k := range params {
		if buf.Len() > 0 {
			buf.WriteByte('&')
		}
		buf.WriteString(k)
		if len(values[k]) > 0 {
			buf.WriteByte('=')
			buf.WriteString(values[k])
		}
	}
	canonicalQuery := buf.String()

	//Canonical Headers
	var headers []string
	buf.Reset()
	addHeadersMap := make(map[string]bool)
	for _, k := range additionalHeaders {
		addHeadersMap[strings.ToLower(k)] = true
	}
	for k := range request.Header {
		lowk := strings.ToLower(k)
		if isDefaultSignedHeader(lowk) {
			headers = append(headers, lowk)
		} else if _, ok := addHeadersMap[lowk]; ok {
			headers = append(headers, lowk)
		}
	}
	sort.Strings(headers)
	for _, k := range headers {
		headerValues := make([]string, len(request.Header.Values(k)))
		for i, v := range request.Header.Values(k) {
			headerValues[i] = strings.TrimSpace(v)
		}
		buf.WriteString(k)
		buf.WriteString(":")
		buf.WriteString(strings.Join(headerValues, ","))
		buf.WriteString("\n")
	}
	canonicalHeaders := buf.String()

	//Additional Headers
	canonicalAdditionalHeaders := strings.Join(additionalHeaders, ";")

	hashPayload := unsignedPayload
	if val := request.Header.Get(contentSha256Header); val != "" {
		hashPayload = val
	}

	buf.Reset()
	buf.WriteString(request.Method)
	buf.WriteString("\n")
	buf.WriteString(canonicalUri)
	buf.WriteString("\n")
	buf.WriteString(canonicalQuery)
	buf.WriteString("\n")
	buf.WriteString(canonicalHeaders)
	buf.WriteString("\n")
	buf.WriteString(canonicalAdditionalHeaders)
	buf.WriteString("\n")
	buf.WriteString(hashPayload)

	return buf.String()
}

func buildScope(date, region, product string) string {
	return fmt.Sprintf("%s/%s/%s/aliyun_v4_request", date, region, product)
}

func (s *SignerV4) calcSignature(sk, date, region, product, stringToSign string) string {
	hmacHash := func() hash.Hash { return sha256.New() }

	signingKey := "aliyun_v4" + sk

	h1 := hmac.New(func() hash.Hash { return sha256.New() }, []byte(signingKey))
	io.WriteString(h1, date)
	h1Key := h1.Sum(nil)

	h2 := hmac.New(hmacHash, h1Key)
	io.WriteString(h2, region)
	h2Key := h2.Sum(nil)

	h3 := hmac.New(hmacHash, h2Key)
	io.WriteString(h3, product)
	h3Key := h3.Sum(nil)

	h4 := hmac.New(hmacHash, h3Key)
	io.WriteString(h4, "aliyun_v4_request")
	h4Key := h4.Sum(nil)

	h := hmac.New(hmacHash, h4Key)
	io.WriteString(h, stringToSign)
	signature := hex.EncodeToString(h.Sum(nil))

	return signature
}

func (s *SignerV4) authHeader(ctx context.Context, signingCtx *SigningContext) error {
	request := signingCtx.Request
	cred := signingCtx.Credentials

	// Date
	if signingCtx.Time.IsZero() {
		signingCtx.Time = time.Now().Add(signingCtx.ClockOffset)
	}
	utcTime := signingCtx.Time.UTC()
	datetime := utcTime.Format(iso8601DatetimeFormat)
	date := utcTime.Format(iso8601DateFormat)
	request.Header.Set(ossDateHeader, datetime)
	request.Header.Set(dateHeader, utcTime.Format(http.TimeFormat))

	// Credentials information
	if cred.SecurityToken != "" {
		request.Header.Set(securityTokenHeader, cred.SecurityToken)
	}

	// Other Headers
	request.Header.Set(contentSha256Header, unsignedPayload)

	// Scope
	region := toString(signingCtx.Region)
	product := toString(signingCtx.Product)
	scope := buildScope(date, region, product)

	additionalHeaders := getCommonAdditionalHeaders(request.Header, signingCtx.AdditionalHeaders)

	// CanonicalRequest
	canonicalRequest := s.calcCanonicalRequest(signingCtx, additionalHeaders)

	// StringToSign
	stringToSign := s.calcStringToSign(datetime, scope, canonicalRequest)
	signingCtx.StringToSign = stringToSign

	// Signature
	signature := s.calcSignature(cred.AccessKeySecret, date, region, product, stringToSign)

	// credential
	var buf strings.Builder
	buf.WriteString("OSS4-HMAC-SHA256 Credential=")
	buf.WriteString(cred.AccessKeyID + "/" + scope)
	if len(additionalHeaders) > 0 {
		buf.WriteString(",AdditionalHeaders=")
		buf.WriteString(strings.Join(additionalHeaders, ";"))
	}
	buf.WriteString(",Signature=")
	buf.WriteString(signature)

	request.Header.Set(authorizationHeader, buf.String())

	//fmt.Printf("canonicalRequest:\n%s\n", canonicalRequest)

	//fmt.Printf("stringToSign:\n%s\n", stringToSign)

	return nil
}

func (s *SignerV4) authQuery(ctx context.Context, signingCtx *SigningContext) error {
	request := signingCtx.Request
	cred := signingCtx.Credentials

	// Date
	now := time.Now().UTC()
	if signingCtx.Time.IsZero() {
		signingCtx.Time = now.Add(defaultExpiresDuration)
	}
	if signingCtx.signTime != nil {
		now = signingCtx.signTime.UTC()
	}
	datetime := now.Format(iso8601DatetimeFormat)
	date := now.Format(iso8601DateFormat)
	expires := signingCtx.Time.Unix() - now.Unix()

	// Scope
	region := toString(signingCtx.Region)
	product := toString(signingCtx.Product)
	scope := buildScope(date, region, product)

	additionalHeaders := getCommonAdditionalHeaders(request.Header, signingCtx.AdditionalHeaders)

	// Credentials information
	query, _ := url.ParseQuery(request.URL.RawQuery)
	if cred.SecurityToken != "" {
		query.Add("x-oss-security-token", cred.SecurityToken)
	}
	query.Add("x-oss-signature-version", algorithmV4)
	query.Add("x-oss-date", datetime)
	query.Add("x-oss-expires", fmt.Sprintf("%v", expires))
	query.Add("x-oss-credential", fmt.Sprintf("%s/%s", cred.AccessKeyID, scope))
	if len(additionalHeaders) > 0 {
		query.Add("x-oss-additional-headers", strings.Join(additionalHeaders, ";"))
	}
	request.URL.RawQuery = query.Encode()

	// CanonicalRequest
	canonicalRequest := s.calcCanonicalRequest(signingCtx, additionalHeaders)

	// StringToSign
	stringToSign := s.calcStringToSign(datetime, scope, canonicalRequest)
	signingCtx.StringToSign = stringToSign

	//fmt.Printf("canonicalRequest:\n%s\n", canonicalRequest)

	//fmt.Printf("stringToSign:\n%s\n", stringToSign)

	// Signature
	signature := s.calcSignature(cred.AccessKeySecret, date, region, product, stringToSign)

	// Authorization query
	query.Add("x-oss-signature", signature)
	request.URL.RawQuery = strings.Replace(query.Encode(), "+", "%20", -1)

	return nil
}

func (s *SignerV4) Sign(ctx context.Context, signingCtx *SigningContext) error {
	if signingCtx == nil {
		return fmt.Errorf("SigningContext is null.")
	}

	if signingCtx.Credentials == nil || !signingCtx.Credentials.HasKeys() {
		return fmt.Errorf("SigningContext.Credentials is null or empty.")
	}

	if signingCtx.Request == nil {
		return fmt.Errorf("SigningContext.Request is null.")
	}
	if signingCtx.AuthMethodQuery {
		return s.authQuery(ctx, signingCtx)
	}
	return s.authHeader(ctx, signingCtx)
}

func (s *SignerV4) IsSignedHeader(additionalHeaders []string, h string) bool {
	return isDefaultSignedHeader(strings.ToLower(h)) || ContainsStr(additionalHeaders, h)
}

// ContainsStr Used to check if the string is in the slice
func ContainsStr(slice []string, str string) bool {
	for _, item := range slice {
		if strings.ToLower(str) == strings.ToLower(item) {
			return true
		}
	}
	return false
}
