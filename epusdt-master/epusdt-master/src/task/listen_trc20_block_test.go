package task

import "testing"

func TestTronAddressToHexConvertsBase58Contract(t *testing.T) {
	got, err := TronAddressToHex("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
	if err != nil {
		t.Fatalf("TronAddressToHex(): %v", err)
	}

	const want = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
	if got != want {
		t.Fatalf("TronAddressToHex() = %q, want %q", got, want)
	}
}

func TestNormalizeTronHexAddress(t *testing.T) {
	got := normalizeTronHexAddress("0X41A614F803B6FD780986A42C78EC9C7F77E6DED13C")
	const want = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
	if got != want {
		t.Fatalf("normalizeTronHexAddress() = %q, want %q", got, want)
	}
}
