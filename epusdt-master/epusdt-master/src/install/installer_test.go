package install

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
)

func TestInstallDefaults(t *testing.T) {
	d := InstallDefaults()
	if d.AppName != "epusdt" {
		t.Errorf("AppName = %q, want epusdt", d.AppName)
	}
	if d.HttpBindAddr != "127.0.0.1" {
		t.Errorf("HttpBindAddr = %q, want 127.0.0.1", d.HttpBindAddr)
	}
	if d.HttpBindPort != 8000 {
		t.Errorf("HttpBindPort = %d, want 8000", d.HttpBindPort)
	}
	if d.OrderExpirationTime != 10 {
		t.Errorf("OrderExpirationTime = %d, want 10", d.OrderExpirationTime)
	}
}

func TestWriteEnvFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")

	req := &InstallRequest{
		AppName:             "myapp",
		AppURI:              "http://1.2.3.4:8000",
		HttpBindAddr:        "0.0.0.0",
		HttpBindPort:        9000,
		RuntimeRootPath:     "./runtime",
		LogSavePath:         "./logs",
		OrderExpirationTime: 15,
		OrderNoticeMaxRetry: 3,
	}
	if err := writeEnvFile(path, req); err != nil {
		t.Fatalf("writeEnvFile: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read env: %v", err)
	}
	content := string(data)

	for _, want := range []string{
		"app_name=myapp",
		"app_uri=http://1.2.3.4:8000",
		"http_listen=0.0.0.0:9000",
		"order_expiration_time=15",
		"order_notice_max_retry=3",
		"db_type=sqlite",
		"install=false",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("env file missing %q\ncontent:\n%s", want, content)
		}
	}
}

func TestInstallAPIDefaults(t *testing.T) {
	h := &installHandler{done: make(chan struct{})}
	e := echo.New()
	e.GET("/install/defaults", h.GetDefaults)

	req := httptest.NewRequest(http.MethodGet, "/install/defaults", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["app_name"] != "epusdt" {
		t.Errorf("app_name = %v, want epusdt", body["app_name"])
	}
	if body["http_bind_addr"] != "127.0.0.1" {
		t.Errorf("http_bind_addr = %v, want 127.0.0.1", body["http_bind_addr"])
	}
	if body["http_bind_port"] != float64(8000) {
		t.Errorf("http_bind_port = %v, want 8000", body["http_bind_port"])
	}
}

func TestInstallServerRootRedirectsToInstall(t *testing.T) {
	dir := t.TempDir()
	wwwRoot := filepath.Join(dir, "www")
	if err := os.MkdirAll(wwwRoot, 0o755); err != nil {
		t.Fatalf("mkdir www root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wwwRoot, "index.html"), []byte("install-ui"), 0o644); err != nil {
		t.Fatalf("write index.html: %v", err)
	}

	e, _ := newInstallServer(filepath.Join(dir, ".env"), wwwRoot)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302; body: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/install" {
		t.Fatalf("Location = %q, want /install", got)
	}
}

func TestInstallServerServesSPAOnInstallRoute(t *testing.T) {
	dir := t.TempDir()
	wwwRoot := filepath.Join(dir, "www")
	if err := os.MkdirAll(wwwRoot, 0o755); err != nil {
		t.Fatalf("mkdir www root: %v", err)
	}
	const wantBody = "install-ui"
	if err := os.WriteFile(filepath.Join(wwwRoot, "index.html"), []byte(wantBody), 0o644); err != nil {
		t.Fatalf("write index.html: %v", err)
	}

	e, _ := newInstallServer(filepath.Join(dir, ".env"), wwwRoot)

	req := httptest.NewRequest(http.MethodGet, "/install", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); body != wantBody {
		t.Fatalf("body = %q, want %q", body, wantBody)
	}
}

func TestInstallAPISubmit(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")

	h := &installHandler{envFilePath: envPath, done: make(chan struct{})}
	e := echo.New()
	e.POST("/install", h.Submit)

	payload := `{"app_name":"testapp","app_uri":"http://10.0.0.1:8000","http_bind_addr":"0.0.0.0","http_bind_port":8000,"order_expiration_time":10,"order_notice_max_retry":1}`
	req := httptest.NewRequest(http.MethodPost, "/install", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	// done channel should be closed after successful submit
	select {
	case <-h.done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handler did not close done channel within timeout")
	}
	data, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatalf("env file not written: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "app_uri=http://10.0.0.1:8000") {
		t.Errorf("env file missing app_uri; content:\n%s", content)
	}
	if !strings.Contains(content, "http_listen=0.0.0.0:8000") {
		t.Errorf("env file missing http_listen; content:\n%s", content)
	}
}

func TestInstallAPISubmitMissingURI(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")

	h := &installHandler{envFilePath: envPath, done: make(chan struct{})}
	e := echo.New()
	e.POST("/install", h.Submit)

	req := httptest.NewRequest(http.MethodPost, "/install", strings.NewReader(`{"app_name":"x"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if _, err := os.Stat(envPath); err == nil {
		t.Error("env file should not have been written for invalid request")
	}
}

func TestInstallAPISubmitInvalidPort(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")

	h := &installHandler{envFilePath: envPath, done: make(chan struct{})}
	e := echo.New()
	e.POST("/install", h.Submit)

	payload := `{"app_uri":"http://example.com","http_bind_port":99999}`
	req := httptest.NewRequest(http.MethodPost, "/install", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(envPath); err == nil {
		t.Error("env file should not have been written for invalid port")
	}
}
