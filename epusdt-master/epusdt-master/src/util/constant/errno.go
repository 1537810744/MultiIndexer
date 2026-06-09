package constant

var Errno = map[int]string{
	400:   "system error",
	401:   "signature verification failed",
	10001: "wallet address already exists",
	10002: "order already exists",
	10003: "no available wallet address",
	10004: "invalid payment amount",
	10005: "no available amount channel",
	10006: "rate calculation failed",
	10007: "block transaction already processed",
	10008: "order does not exist",
	10009: "failed to parse request params",
	10010: "order status already changed",
	10011: "exceeded maximum sub-order limit",
	10012: "cannot switch network on a sub-order",
	10013: "order is not awaiting payment",
	10014: "chain is not enabled",
	10016: "supported asset not found",
	10017: "payment provider is not enabled",
	10018: "payment provider configuration is incomplete",
	10019: "payment provider does not support this token or network",
}

var (
	SystemErr                  = Err(400)
	SignatureErr               = Err(401)
	WalletAddressAlreadyExists = Err(10001)
	OrderAlreadyExists         = Err(10002)
	NotAvailableWalletAddress  = Err(10003)
	PayAmountErr               = Err(10004)
	NotAvailableAmountErr      = Err(10005)
	RateAmountErr              = Err(10006)
	OrderBlockAlreadyProcess   = Err(10007)
	OrderNotExists             = Err(10008)
	ParamsMarshalErr           = Err(10009)
	OrderStatusConflict        = Err(10010)
	SubOrderLimitExceeded      = Err(10011)
	CannotSwitchSubOrder       = Err(10012)
	OrderNotWaitPay            = Err(10013)
	ChainNotEnabled            = Err(10014)
	SupportedAssetNotFound     = Err(10016)
	PaymentProviderNotEnabled  = Err(10017)
	PaymentProviderConfigErr   = Err(10018)
	PaymentProviderNotSupport  = Err(10019)
)

type RspError struct {
	Code int
	Msg  string
}

func (re *RspError) Error() string {
	return re.Msg
}

func Err(code int) (err error) {
	err = &RspError{
		Code: code,
		Msg:  Errno[code],
	}
	return err
}

func (re *RspError) Render() (code int, msg string) {
	return re.Code, re.Msg
}

// HttpStatus maps a RspError code to the HTTP status the handler
// should use on the wire. Small codes (< 1000) are treated as real
// HTTP status codes (e.g. 400 system error, 401 signature failure) so
// clients see the right status. Business codes (>= 1000) are all
// client-side problems that map to HTTP 400; the specific code still
// lives in the body's `status_code` field for the frontend to branch on.
func (re *RspError) HttpStatus() int {
	if re == nil {
		return 500
	}
	if re.Code >= 400 && re.Code < 600 {
		return re.Code
	}
	return 400
}
