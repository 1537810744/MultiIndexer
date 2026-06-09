package data

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/GMWalletApp/epusdt/internal/testutil"
	"github.com/GMWalletApp/epusdt/model/mdb"
)

func TestEvmTransactionLockAddressIsCaseInsensitive(t *testing.T) {
	cleanup := testutil.SetupTestDatabases(t)
	defer cleanup()

	tradeID := "trade-evm-case"
	address := "0xA1B2c3D4e5F60718293aBcDeF001122334455667"

	if err := LockTransaction("Ethereum", address, "usdt", tradeID, 1.23, time.Hour); err != nil {
		t.Fatalf("lock transaction: %v", err)
	}

	gotTradeID, err := GetTradeIdByWalletAddressAndAmountAndToken(mdb.NetworkEthereum, strings.ToLower(address), "USDT", 1.23)
	if err != nil {
		t.Fatalf("lookup transaction lock: %v", err)
	}
	if gotTradeID != tradeID {
		t.Fatalf("trade id = %q, want %q", gotTradeID, tradeID)
	}

	if err := UnLockTransaction(mdb.NetworkEthereum, strings.ToUpper(address), "USDT", 1.23); err != nil {
		t.Fatalf("unlock transaction: %v", err)
	}

	gotTradeID, err = GetTradeIdByWalletAddressAndAmountAndToken(mdb.NetworkEthereum, address, "USDT", 1.23)
	if err != nil {
		t.Fatalf("lookup after unlock: %v", err)
	}
	if gotTradeID != "" {
		t.Fatalf("expected lock to be released, got trade id %q", gotTradeID)
	}
}

func TestTransactionLockPrecisionPreventsEquivalentAmountsOnly(t *testing.T) {
	cleanup := testutil.SetupTestDatabases(t)
	defer cleanup()

	if err := SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "2", mdb.SettingTypeInt); err != nil {
		t.Fatalf("set precision 2: %v", err)
	}
	if err := LockTransaction(mdb.NetworkTron, "TPrecisionAddress001", "USDT", "trade-old", 1.23, time.Hour); err != nil {
		t.Fatalf("lock old transaction: %v", err)
	}

	if err := SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "4", mdb.SettingTypeInt); err != nil {
		t.Fatalf("set precision 4: %v", err)
	}
	if err := LockTransaction(mdb.NetworkTron, "TPrecisionAddress001", "USDT", "trade-equivalent", 1.2300, time.Hour); !errors.Is(err, ErrTransactionLocked) {
		t.Fatalf("equivalent lock error = %v, want %v", err, ErrTransactionLocked)
	}
	if err := LockTransaction(mdb.NetworkTron, "TPrecisionAddress001", "USDT", "trade-new", 1.2301, time.Hour); err != nil {
		t.Fatalf("distinct precision lock: %v", err)
	}
}

func TestTransactionLockLookupUsesStoredPrecision(t *testing.T) {
	cleanup := testutil.SetupTestDatabases(t)
	defer cleanup()

	if err := SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "4", mdb.SettingTypeInt); err != nil {
		t.Fatalf("set precision 4: %v", err)
	}
	if err := LockTransaction(mdb.NetworkTron, "TPrecisionAddress002", "USDT", "trade-precise", 1.2345, time.Hour); err != nil {
		t.Fatalf("lock precise transaction: %v", err)
	}
	if err := SetSetting(mdb.SettingGroupSystem, mdb.SettingKeyAmountPrecision, "2", mdb.SettingTypeInt); err != nil {
		t.Fatalf("set precision 2: %v", err)
	}

	gotTradeID, err := GetTradeIdByWalletAddressAndAmountAndToken(mdb.NetworkTron, "TPrecisionAddress002", "USDT", 1.2345)
	if err != nil {
		t.Fatalf("lookup transaction lock: %v", err)
	}
	if gotTradeID != "trade-precise" {
		t.Fatalf("trade id = %q, want trade-precise", gotTradeID)
	}
}

func TestNonEvmTransactionLockAddressRemainsCaseSensitive(t *testing.T) {
	cleanup := testutil.SetupTestDatabases(t)
	defer cleanup()

	tradeID := "trade-tron-case"
	address := "TCaseSensitiveAddress001"

	if err := LockTransaction(mdb.NetworkTron, address, "USDT", tradeID, 1.00, time.Hour); err != nil {
		t.Fatalf("lock transaction: %v", err)
	}

	gotTradeID, err := GetTradeIdByWalletAddressAndAmountAndToken(mdb.NetworkTron, strings.ToLower(address), "USDT", 1.00)
	if err != nil {
		t.Fatalf("lookup transaction lock: %v", err)
	}
	if gotTradeID != "" {
		t.Fatalf("tron address lookup should remain case-sensitive, got trade id %q", gotTradeID)
	}
}
