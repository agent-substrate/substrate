package crypto

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/asn1"
	"encoding/json"
	"encoding/pem"
	"fmt"
)

// CreateMasterRsa Create master key interface implemented by rsa
// matDesc will be converted to json string
func CreateMasterRsa(matDesc map[string]string, publicKey string, privateKey string) (MasterCipher, error) {
	var masterCipher MasterRsaCipher
	var jsonDesc string
	if len(matDesc) > 0 {
		b, err := json.Marshal(matDesc)
		if err != nil {
			return masterCipher, err
		}
		jsonDesc = string(b)
	}
	masterCipher.MatDesc = jsonDesc
	masterCipher.PublicKey = publicKey
	masterCipher.PrivateKey = privateKey
	return masterCipher, nil
}

// MasterRsaCipher rsa master key interface
type MasterRsaCipher struct {
	MatDesc    string
	PublicKey  string
	PrivateKey string
}

// GetWrapAlgorithm get master key wrap algorithm
func (mrc MasterRsaCipher) GetWrapAlgorithm() string {
	return RsaCryptoWrap
}

// GetMatDesc get master key describe
func (mrc MasterRsaCipher) GetMatDesc() string {
	return mrc.MatDesc
}

// Encrypt encrypt data by rsa public key
// Mainly used to encrypt object's symmetric secret key and iv
func (mrc MasterRsaCipher) Encrypt(plainData []byte) ([]byte, error) {
	block, _ := pem.Decode([]byte(mrc.PublicKey))
	if block == nil {
		return nil, fmt.Errorf("pem.Decode public key error")
	}

	var pub *rsa.PublicKey
	if block.Type == "PUBLIC KEY" {
		// pks8 format
		pubInterface, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		pub = pubInterface.(*rsa.PublicKey)
	} else if block.Type == "RSA PUBLIC KEY" {
		// pks1 format
		pub = &rsa.PublicKey{}
		_, err := asn1.Unmarshal(block.Bytes, pub)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("not supported public key,type:%s", block.Type)
	}
	return rsa.EncryptPKCS1v15(rand.Reader, pub, plainData)
}

// Decrypt Decrypt data by rsa private key
// Mainly used to decrypt object's symmetric secret key and iv
func (mrc MasterRsaCipher) Decrypt(cryptoData []byte) ([]byte, error) {
	block, _ := pem.Decode([]byte(mrc.PrivateKey))
	if block == nil {
		return nil, fmt.Errorf("pem.Decode private key error")
	}

	if block.Type == "PRIVATE KEY" {
		// pks8 format
		privInterface, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		return rsa.DecryptPKCS1v15(rand.Reader, privInterface.(*rsa.PrivateKey), cryptoData)
	} else if block.Type == "RSA PRIVATE KEY" {
		// pks1 format
		priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		return rsa.DecryptPKCS1v15(rand.Reader, priv, cryptoData)
	} else {
		return nil, fmt.Errorf("not supported private key,type:%s", block.Type)
	}
}
