package zmtp

// SecurityMechanismType denotes types of ZMTP security mechanisms
type SecurityMechanismType string

const (
	// NullSecurityMechanismType is an empty security mechanism
	// that does no authentication nor encryption.
	NullSecurityMechanismType SecurityMechanismType = "NULL"

	// PlainSecurityMechanismType is a security mechanism that uses
	// plaintext passwords. It is a reference implementation and
	// should not be used to anything important.
	PlainSecurityMechanismType SecurityMechanismType = "PLAIN"

	// CurveSecurityMechanismType uses ZMQ_CURVE for authentication
	// and encryption.
	CurveSecurityMechanismType SecurityMechanismType = "CURVE"
)

// SecurityMechanism is an interface for ZMTP security mechanisms
type SecurityMechanism interface {
	Type() SecurityMechanismType
	Handshake() error
	Encrypt([]byte) []byte
}
