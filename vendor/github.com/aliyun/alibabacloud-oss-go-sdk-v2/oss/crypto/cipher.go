package crypto

import (
	"io"
)

// Cipher is interface for encryption or decryption of an object
type Cipher interface {
	Encrypter
	Decrypter
}

// Encrypter is interface with only encrypt method
type Encrypter interface {
	Encrypt(io.Reader) io.Reader
}

// Decrypter is interface with only decrypt method
type Decrypter interface {
	Decrypt(io.Reader) io.Reader
}

// CryptoEncrypter provides close method for Encrypter
type CryptoEncrypter struct {
	Body      io.Reader
	Encrypter io.Reader
	isClosed  bool
}

// Close lets the CryptoEncrypter satisfy io.ReadCloser interface
func (rc *CryptoEncrypter) Close() error {
	rc.isClosed = true
	if closer, ok := rc.Body.(io.ReadCloser); ok {
		return closer.Close()
	}
	return nil
}

// Read lets the CryptoEncrypter satisfy io.ReadCloser interface
func (rc *CryptoEncrypter) Read(b []byte) (int, error) {
	if rc.isClosed {
		return 0, io.EOF
	}
	return rc.Encrypter.Read(b)
}

// CryptoDecrypter provides close method for Decrypter
type CryptoDecrypter struct {
	Body      io.Reader
	Decrypter io.Reader
	isClosed  bool
}

// Close lets the CryptoDecrypter satisfy io.ReadCloser interface
func (rc *CryptoDecrypter) Close() error {
	rc.isClosed = true
	if closer, ok := rc.Body.(io.ReadCloser); ok {
		return closer.Close()
	}
	return nil
}

// Read lets the CryptoDecrypter satisfy io.ReadCloser interface
func (rc *CryptoDecrypter) Read(b []byte) (int, error) {
	if rc.isClosed {
		return 0, io.EOF
	}
	return rc.Decrypter.Read(b)
}
