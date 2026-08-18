package oss

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/signer"
)

type CertificateConfiguration struct {
	// The ID of the certificate.
	CertId *string `xml:"CertId"`

	// The public key of the certificate.
	Certificate *string `xml:"Certificate"`

	// The private key of the certificate.
	PrivateKey *string `xml:"PrivateKey"`

	// The ID of the certificate. If the Force parameter is not set to true, the OSS server checks whether the value of the Force parameter matches the current certificate ID. If the value does not match the certificate ID, an error is returned.noticeIf you do not specify the PreviousCertId parameter when you bind a certificate, you must set the Force parameter to true./notice
	PreviousCertId *string `xml:"PreviousCertId"`

	// Specifies whether to overwrite the certificate. Valid values:- true: overwrites the certificate.- false: does not overwrite the certificate.
	Force *bool `xml:"Force"`

	// Specifies whether to delete the certificate. Valid values:- true: deletes the certificate.- false: does not delete the certificate.
	DeleteCertificate *bool `xml:"DeleteCertificate"`
}

type Cname struct {
	// The custom domain name.
	Domain *string `xml:"Domain"`

	// The container for which the certificate is configured.
	CertificateConfiguration *CertificateConfiguration `xml:"CertificateConfiguration"`
}

type BucketCnameConfiguration struct {
	// The custom domain name.
	// Deprecated: Domain is deprecated, and will be removed in the future. Use Cname.Domain instead.
	// If both exist simultaneously, the value of Cname will take precedence.
	Domain *string

	// The container for which the certificate is configured.
	// Deprecated: CertificateConfiguration is deprecated, , and will be removed in the future. Use Cname.CertificateConfiguration instead.
	// If both exist simultaneously, the value of Cname will take precedence.
	CertificateConfiguration *CertificateConfiguration

	// The container for the custom domain name.
	Cname *Cname `xml:"Cname"`
}

type CnameCertificate struct {
	// The time when the certificate was bound.
	CreationDate *string `xml:"CreationDate"`

	// The signature of the certificate.
	Fingerprint *string `xml:"Fingerprint"`

	// The time when the certificate takes effect.
	ValidStartDate *string `xml:"ValidStartDate"`

	// The time when the certificate expires.
	ValidEndDate *string `xml:"ValidEndDate"`

	// The source of the certificate.Valid values:*   CAS            *   Upload
	Type *string `xml:"Type"`

	// The ID of the certificate.
	CertId *string `xml:"CertId"`

	// The status of the certificate.Valid values:*   Enabled            *   Disabled
	Status *string `xml:"Status"`
}

type CnameInfo struct {
	// The custom domain name.
	Domain *string `xml:"Domain"`

	// The time when the custom domain name was mapped.
	LastModified *string `xml:"LastModified"`

	// The status of the domain name. Valid values:*   Enabled*   Disabled
	Status *string `xml:"Status"`

	// The container in which the certificate information is stored.
	Certificate *CnameCertificate `xml:"Certificate"`
}

type CnameToken struct {
	// The name of the bucket to which the CNAME record is mapped.
	Bucket *string `xml:"Bucket"`

	// The name of the CNAME record that is mapped to the bucket.
	Cname *string `xml:"Cname"`

	// The CNAME token that is returned by OSS.
	Token *string `xml:"Token"`

	// The time when the CNAME token expires.
	ExpireTime *string `xml:"ExpireTime"`
}

type PutCnameRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	BucketCnameConfiguration *BucketCnameConfiguration `input:"body,BucketCnameConfiguration,xml,required"`

	RequestCommon
}

type PutCnameResult struct {
	ResultCommon
}

// PutCname Maps a CNAME record to a bucket.
func (c *Client) PutCname(ctx context.Context, request *PutCnameRequest, optFns ...func(*Options)) (*PutCnameResult, error) {
	var err error
	if request == nil {
		request = &PutCnameRequest{}
	}
	input := &OperationInput{
		OpName: "PutCname",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"cname": "",
			"comp":  "add",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"comp", "cname"})

	configuration := request.BucketCnameConfiguration
	defer func() {
		request.BucketCnameConfiguration = configuration
	}()
	if configuration != nil && configuration.Cname == nil {
		request.BucketCnameConfiguration = &BucketCnameConfiguration{
			Cname: &Cname{
				Domain:                   configuration.Domain,
				CertificateConfiguration: configuration.CertificateConfiguration,
			},
		}
	}

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &PutCnameResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type ListCnameRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	RequestCommon
}

type ListCnameResult struct {
	// The container that is used to store the information about all CNAME records.
	Cnames []CnameInfo `xml:"Cname"`

	// The name of the bucket to which the CNAME records you want to query are mapped.
	Bucket *string `xml:"Bucket"`

	// The name of the bucket owner.
	Owner *string `xml:"Owner"`

	ResultCommon
}

// ListCname Queries all CNAME records that are mapped to a bucket.
func (c *Client) ListCname(ctx context.Context, request *ListCnameRequest, optFns ...func(*Options)) (*ListCnameResult, error) {
	var err error
	if request == nil {
		request = &ListCnameRequest{}
	}
	input := &OperationInput{
		OpName: "ListCname",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"cname": "",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"cname"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &ListCnameResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type DeleteCnameRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	BucketCnameConfiguration *BucketCnameConfiguration `input:"body,BucketCnameConfiguration,xml,required"`

	RequestCommon
}

type DeleteCnameResult struct {
	ResultCommon
}

// DeleteCname Deletes a CNAME record that is mapped to a bucket.
func (c *Client) DeleteCname(ctx context.Context, request *DeleteCnameRequest, optFns ...func(*Options)) (*DeleteCnameResult, error) {
	var err error
	if request == nil {
		request = &DeleteCnameRequest{}
	}
	input := &OperationInput{
		OpName: "DeleteCname",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"cname": "",
			"comp":  "delete",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"cname", "comp"})

	configuration := request.BucketCnameConfiguration
	defer func() {
		request.BucketCnameConfiguration = configuration
	}()
	if configuration != nil && configuration.Cname == nil {
		request.BucketCnameConfiguration = &BucketCnameConfiguration{
			Cname: &Cname{
				Domain:                   configuration.Domain,
				CertificateConfiguration: configuration.CertificateConfiguration,
			},
		}
	}

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &DeleteCnameResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type GetCnameTokenRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The name of the CNAME record that is mapped to the bucket.
	Cname *string `input:"query,cname,required"`

	RequestCommon
}

type GetCnameTokenResult struct {
	// The container in which the CNAME token is stored.
	CnameToken *CnameToken `output:"body,CnameToken,xml"`

	ResultCommon
}

// GetCnameToken Queries the created CNAME tokens.
func (c *Client) GetCnameToken(ctx context.Context, request *GetCnameTokenRequest, optFns ...func(*Options)) (*GetCnameTokenResult, error) {
	var err error
	if request == nil {
		request = &GetCnameTokenRequest{}
	}
	input := &OperationInput{
		OpName: "GetCnameToken",
		Method: "GET",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"comp": "token",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"comp", "cname"})

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &GetCnameTokenResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}

type CreateCnameTokenRequest struct {
	// The name of the bucket.
	Bucket *string `input:"host,bucket,required"`

	// The request body schema.
	BucketCnameConfiguration *BucketCnameConfiguration `input:"body,BucketCnameConfiguration,xml,required"`

	RequestCommon
}

type CreateCnameTokenResult struct {
	// The container in which the CNAME token is stored.
	CnameToken *CnameToken `output:"body,CnameToken,xml"`

	ResultCommon
}

// CreateCnameToken Creates a CNAME token to verify the ownership of a domain name.
func (c *Client) CreateCnameToken(ctx context.Context, request *CreateCnameTokenRequest, optFns ...func(*Options)) (*CreateCnameTokenResult, error) {
	var err error
	if request == nil {
		request = &CreateCnameTokenRequest{}
	}
	input := &OperationInput{
		OpName: "CreateCnameToken",
		Method: "POST",
		Headers: map[string]string{
			HTTPHeaderContentType: contentTypeXML,
		},
		Parameters: map[string]string{
			"cname": "",
			"comp":  "token",
		},
		Bucket: request.Bucket,
	}
	input.OpMetadata.Set(signer.SubResource, []string{"cname", "comp"})

	configuration := request.BucketCnameConfiguration
	defer func() {
		request.BucketCnameConfiguration = configuration
	}()
	if configuration != nil && configuration.Cname == nil {
		request.BucketCnameConfiguration = &BucketCnameConfiguration{
			Cname: &Cname{
				Domain:                   configuration.Domain,
				CertificateConfiguration: configuration.CertificateConfiguration,
			},
		}
	}

	if err = c.marshalInput(request, input, updateContentMd5); err != nil {
		return nil, err
	}
	output, err := c.invokeOperation(ctx, input, optFns)
	if err != nil {
		return nil, err
	}

	result := &CreateCnameTokenResult{}

	if err = c.unmarshalOutput(result, output, unmarshalBodyXmlMix); err != nil {
		return nil, c.toClientError(err, "UnmarshalOutputFail", output)
	}

	return result, err
}
