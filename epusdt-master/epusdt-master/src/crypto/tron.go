package crypto

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/shengdoushi/base58"
)

const AddressLength = 20
const PrefixMainnet = 0x41

// Encode returns the Base58 encoding of input using the Bitcoin alphabet.
func Encode(input []byte) string {
	return base58.Encode(input, base58.BitcoinAlphabet)
}

// EncodeCheck returns the Base58Check encoding of input (Base58 with a 4-byte SHA-256d checksum).
func EncodeCheck(input []byte) string {
	h256h0 := sha256.New()
	h256h0.Write(input)
	h0 := h256h0.Sum(nil)

	h256h1 := sha256.New()
	h256h1.Write(h0)
	h1 := h256h1.Sum(nil)

	inputCheck := append(append([]byte(nil), input...), h1[:4]...)

	return Encode(inputCheck)
}

// DecodeCheck decodes a Base58Check string and validates its 4-byte checksum.
func DecodeCheck(input string) ([]byte, error) {
	raw, err := base58.Decode(input, base58.BitcoinAlphabet)
	if err != nil {
		return nil, err
	}
	if len(raw) < 4 {
		return nil, fmt.Errorf("base58check payload too short")
	}

	payload := raw[:len(raw)-4]
	checksum := raw[len(raw)-4:]

	h0 := sha256.Sum256(payload)
	h1 := sha256.Sum256(h0[:])
	if !bytes.Equal(checksum, h1[:4]) {
		return nil, fmt.Errorf("base58check checksum mismatch")
	}
	return payload, nil
}
