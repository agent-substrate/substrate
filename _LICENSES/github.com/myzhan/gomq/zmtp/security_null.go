package zmtp

// SecurityNull implements the NullSecurityMechanismType
type SecurityNull struct{}

// NewSecurityNull returns a SecurityNull mechanism
func NewSecurityNull() *SecurityNull {
	return &SecurityNull{}
}

// Type returns the security mechanisms type
func (s *SecurityNull) Type() SecurityMechanismType {
	return NullSecurityMechanismType
}

// Handshake performs the ZMTP handshake for this
// security mechanism
func (s *SecurityNull) Handshake() error {
	return nil
}

// Encrypt encrypts a []byte
func (s *SecurityNull) Encrypt(data []byte) []byte {
	return data
}
