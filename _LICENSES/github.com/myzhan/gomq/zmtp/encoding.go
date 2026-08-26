package zmtp

import "fmt"

func toNullPaddedString(str string, dst []byte) error {
	if len(str) > len(dst) {
		return fmt.Errorf("dst []byte too short for string")
	}

	copy(dst, str)
	return nil
}

func fromNullPaddedString(slice []byte) string {
	str := ""
	for _, b := range slice {
		if b == 0 {
			break
		}

		str += string(b)
	}

	return str
}

func toByteBool(b bool) byte {
	if b {
		return byte(0x01)
	}
	return byte(0x00)
}

func fromByteBool(b byte) (bool, error) {
	if b == byte(0x00) {
		return false, nil
	}

	if b == byte(0x01) {
		return true, nil
	}

	return false, fmt.Errorf("Invalid boolean byte")
}
