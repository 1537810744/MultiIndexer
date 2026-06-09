package comm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/GMWalletApp/epusdt/model/service"
	"github.com/GMWalletApp/epusdt/util/log"
	"github.com/labstack/echo/v4"
)

// OkPayNotify receives the server-to-server OkPay payment callback.
// The handler accepts JSON or form-urlencoded payloads and returns plain-text
// acknowledgements expected by the upstream provider.
// @Summary      OkPay notify callback
// @Description  Receives OkPay deposit callbacks for hosted checkout child orders.
// @Description  The callback is verified against the configured OkPay shop token and, on success, marks the matching child order as paid.
// @Tags         Payment
// @Accept       json
// @Accept       x-www-form-urlencoded
// @Produce      plain
// @Success      200 {string} string "success"
// @Failure      400 {string} string "fail"
// @Router       /payments/okpay/v1/notify [post]
func (c *BaseCommController) OkPayNotify(ctx echo.Context) error {
	req := ctx.Request()
	rawBody, err := io.ReadAll(req.Body)
	if err != nil {
		log.Sugar.Warnf("[okpay] read notify body failed err=%v", err)
		return ctx.String(http.StatusBadRequest, "fail")
	}
	req.Body = io.NopCloser(bytes.NewReader(rawBody))

	form := make(map[string]string)
	copyForm := func(values url.Values) {
		for key, items := range values {
			if len(items) == 0 {
				continue
			}
			form[key] = items[0]
		}
	}

	copyForm(req.URL.Query())

	contentType := strings.ToLower(req.Header.Get(echo.HeaderContentType))

	if strings.Contains(contentType, echo.MIMEApplicationJSON) && len(rawBody) > 0 {
		if jsonForm, jsonErr := flattenOkPayJSONBody(rawBody); jsonErr == nil {
			for key, value := range jsonForm {
				form[key] = value
			}
		}
	}

	// Match the PHP example's $_POST semantics: let Echo/net/http parse both
	// application/x-www-form-urlencoded and multipart/form-data.
	if len(form) == 0 {
		req.Body = io.NopCloser(bytes.NewReader(rawBody))
		if parsedForm, formErr := ctx.FormParams(); formErr == nil {
			copyForm(parsedForm)
		}
	}

	// Final fallback for raw query-string style bodies.
	if len(form) == 0 && len(rawBody) > 0 && looksLikeQueryPayload(string(rawBody)) {
		if parsed, parseErr := url.ParseQuery(string(rawBody)); parseErr == nil && len(parsed) > 0 {
			copyForm(parsed)
		}
	}

	rawFormData := string(rawBody)
	if rawFormData == "" {
		rawFormData = req.URL.RawQuery
	}

	if len(form) == 0 {
		log.Sugar.Warnf("[okpay] empty notify payload method=%s content_type=%s remote=%s raw_query=%q raw_body=%q", req.Method, req.Header.Get(echo.HeaderContentType), ctx.RealIP(), req.URL.RawQuery, rawFormData)
		return ctx.String(http.StatusBadRequest, "fail")
	}

	if err = service.HandleOkPayNotify(form, rawFormData); err != nil {
		log.Sugar.Warnf("[okpay] notify handle failed method=%s content_type=%s remote=%s form=%v raw_query=%q raw_body=%q err=%v", req.Method, req.Header.Get(echo.HeaderContentType), ctx.RealIP(), form, req.URL.RawQuery, rawFormData, err)
		return ctx.String(http.StatusBadRequest, "fail")
	}
	return ctx.String(http.StatusOK, "success")
}

func flattenOkPayJSONBody(rawBody []byte) (map[string]string, error) {
	decoder := json.NewDecoder(bytes.NewReader(rawBody))
	decoder.UseNumber()

	var payload map[string]interface{}
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}

	flat := make(map[string]string)
	for key, value := range payload {
		if key == "data" {
			dataMap, ok := value.(map[string]interface{})
			if !ok {
				continue
			}
			for dataKey, dataValue := range dataMap {
				flat["data["+dataKey+"]"] = stringifyOkPayJSONValue(dataValue)
			}
			continue
		}
		flat[key] = stringifyOkPayJSONValue(value)
	}
	return flat, nil
}

func stringifyOkPayJSONValue(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	case json.Number:
		return v.String()
	case float64:
		if v == math.Trunc(v) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func looksLikeQueryPayload(raw string) bool {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return false
	}
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		return false
	}
	return strings.Contains(trimmed, "=")
}
