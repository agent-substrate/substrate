package crypto

import (
	"fmt"
	"io"
)

const (
	aesKeySize = 32
	ivSize     = 16
)

// aesCtrCipherBuilder for building ContentCipher
type aesCtrCipherBuilder struct {
	MasterCipher MasterCipher
}

// aesCtrCipher will use aes ctr algorithm
type aesCtrCipher struct {
	CipherData CipherData
	Cipher     Cipher
}

// CreateAesCtrCipher creates ContentCipherBuilder
func CreateAesCtrCipher(cipher MasterCipher) ContentCipherBuilder {
	return aesCtrCipherBuilder{MasterCipher: cipher}
}

// createCipherData create CipherData for encrypt object data
func (builder aesCtrCipherBuilder) createCipherData() (CipherData, error) {
	var cd CipherData
	var err error
	err = cd.RandomKeyIv(aesKeySize, ivSize)
	if err != nil {
		return cd, err
	}

	cd.WrapAlgorithm = builder.MasterCipher.GetWrapAlgorithm()
	cd.CEKAlgorithm = AesCtrAlgorithm
	cd.MatDesc = builder.MasterCipher.GetMatDesc()

	// EncryptedKey
	cd.EncryptedKey, err = builder.MasterCipher.Encrypt(cd.Key)
	if err != nil {
		return cd, err
	}

	// EncryptedIV
	cd.EncryptedIV, err = builder.MasterCipher.Encrypt(cd.IV)
	if err != nil {
		return cd, err
	}

	return cd, nil
}

// contentCipherCD is used to create ContentCipher with CipherData
func (builder aesCtrCipherBuilder) contentCipherCD(cd CipherData) (ContentCipher, error) {
	cipher, err := newAesCtr(cd)
	if err != nil {
		return nil, err
	}

	return &aesCtrCipher{
		CipherData: cd,
		Cipher:     cipher,
	}, nil
}

// ContentCipher is used to create ContentCipher interface
func (builder aesCtrCipherBuilder) ContentCipher() (ContentCipher, error) {
	cd, err := builder.createCipherData()
	if err != nil {
		return nil, err
	}
	return builder.contentCipherCD(cd)
}

// ContentCipherEnv is used to create a decrption ContentCipher from Envelope
func (builder aesCtrCipherBuilder) ContentCipherEnv(envelope Envelope) (ContentCipher, error) {
	var cd CipherData
	cd.EncryptedKey = make([]byte, len(envelope.CipherKey))
	copy(cd.EncryptedKey, []byte(envelope.CipherKey))

	plainKey, err := builder.MasterCipher.Decrypt([]byte(envelope.CipherKey))
	if err != nil {
		return nil, err
	}
	cd.Key = make([]byte, len(plainKey))
	copy(cd.Key, plainKey)

	cd.EncryptedIV = make([]byte, len(envelope.IV))
	copy(cd.EncryptedIV, []byte(envelope.IV))

	plainIV, err := builder.MasterCipher.Decrypt([]byte(envelope.IV))
	if err != nil {
		return nil, err
	}

	cd.IV = make([]byte, len(plainIV))
	copy(cd.IV, plainIV)

	cd.MatDesc = envelope.MatDesc
	cd.WrapAlgorithm = envelope.WrapAlg
	cd.CEKAlgorithm = envelope.CEKAlg

	return builder.contentCipherCD(cd)
}

// GetMatDesc is used to get MasterCipher's MatDesc
func (builder aesCtrCipherBuilder) GetMatDesc() string {
	return builder.MasterCipher.GetMatDesc()
}

// EncryptContents will generate a random key and iv and encrypt the data using ctr
func (cc *aesCtrCipher) EncryptContent(src io.Reader) (io.ReadCloser, error) {
	if sr, ok := src.(io.ReadSeeker); ok {
		if curr, err := sr.Seek(0, io.SeekCurrent); err == nil {
			return &aesSeekEncrypter{
				Body:      sr,
				Encrypter: nil,
				Start:     curr,
				Offset:    curr,
				cc:        cc,
			}, nil
		}
	}
	reader := cc.Cipher.Encrypt(src)
	return &CryptoEncrypter{Body: src, Encrypter: reader}, nil
}

// DecryptContent is used to decrypt object using ctr
func (cc *aesCtrCipher) DecryptContent(src io.Reader) (io.ReadCloser, error) {
	reader := cc.Cipher.Decrypt(src)
	return &CryptoDecrypter{Body: src, Decrypter: reader}, nil
}

// GetCipherData is used to get cipher data information
func (cc *aesCtrCipher) GetCipherData() *CipherData {
	return &(cc.CipherData)
}

// GetCipherData returns cipher data
func (cc *aesCtrCipher) GetEncryptedLen(plainTextLen int64) int64 {
	// AES CTR encryption mode does not change content length
	return plainTextLen
}

// GetAlignLen is used to get align length
func (cc *aesCtrCipher) GetAlignLen() int {
	return len(cc.CipherData.IV)
}

// Clone is used to create a new aesCtrCipher from itself
func (cc *aesCtrCipher) Clone(cd CipherData) (ContentCipher, error) {
	cipher, err := newAesCtr(cd)
	if err != nil {
		return nil, err
	}

	return &aesCtrCipher{
		CipherData: cd,
		Cipher:     cipher,
	}, nil
}

// CryptoSeekEncrypter provides close and seek method for Encrypter
type aesSeekEncrypter struct {
	Body      io.ReadSeeker
	Encrypter io.Reader
	isClosed  bool
	Start     int64
	Offset    int64
	cc        *aesCtrCipher
}

// Close lets the CryptoSeekEncrypter satisfy io.ReadCloser interface
func (rc *aesSeekEncrypter) Close() error {
	rc.isClosed = true
	if closer, ok := rc.Body.(io.ReadCloser); ok {
		return closer.Close()
	}
	return nil
}

// Read lets the CryptoSeekEncrypter satisfy io.ReadCloser interface
func (rc *aesSeekEncrypter) Read(b []byte) (int, error) {
	if rc.isClosed {
		return 0, io.EOF
	}
	if rc.Encrypter == nil {
		if rc.Start != rc.Offset {
			return 0, fmt.Errorf("Cant not encrypt from offset %v, must start from %v", rc.Offset, rc.Start)
		}
		rc.Encrypter = rc.cc.Cipher.Encrypt(rc.Body)
	}
	return rc.Encrypter.Read(b)
}

// Seek lets the CryptoSeekEncrypter satisfy io.Seeker interface
func (rc *aesSeekEncrypter) Seek(offset int64, whence int) (int64, error) {
	off, err := rc.Body.Seek(offset, whence)
	//Reset Encrypter Reader
	rc.Encrypter = nil
	rc.Offset = off

	return off, err
}
