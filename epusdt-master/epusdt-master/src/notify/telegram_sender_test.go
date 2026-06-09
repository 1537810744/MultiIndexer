package notify

import "testing"

func TestParseTelegramConfigAcceptsNumericChatID(t *testing.T) {
	raw := `{"bot_token":"123:ABC","chat_id":123456789}`
	cfg, err := ParseTelegramConfig(raw)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.BotToken != "123:ABC" {
		t.Fatalf("bot token = %q, want %q", cfg.BotToken, "123:ABC")
	}
	if cfg.ChatID != 123456789 {
		t.Fatalf("chat id = %d, want %d", cfg.ChatID, 123456789)
	}
}

func TestParseTelegramConfigAcceptsStringChatID(t *testing.T) {
	raw := `{"bot_token":"123:ABC","chat_id":"-1001234567890"}`
	cfg, err := ParseTelegramConfig(raw)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.ChatID != -1001234567890 {
		t.Fatalf("chat id = %d, want %d", cfg.ChatID, int64(-1001234567890))
	}
}

func TestParseTelegramConfigRejectsInvalidChatID(t *testing.T) {
	raw := `{"bot_token":"123:ABC","chat_id":"not-a-number"}`
	_, err := ParseTelegramConfig(raw)
	if err == nil {
		t.Fatal("expected parse error for invalid chat_id")
	}
}

func TestParseTelegramConfigAcceptsCamelCaseKeys(t *testing.T) {
	raw := `{"botToken":"123:ABC","chatId":"-1001234567890","proxyUrl":"http://127.0.0.1:7890"}`
	cfg, err := ParseTelegramConfig(raw)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.BotToken != "123:ABC" {
		t.Fatalf("bot token = %q, want %q", cfg.BotToken, "123:ABC")
	}
	if cfg.ChatID != -1001234567890 {
		t.Fatalf("chat id = %d, want %d", cfg.ChatID, int64(-1001234567890))
	}
	if cfg.Proxy != "http://127.0.0.1:7890" {
		t.Fatalf("proxy = %q, want %q", cfg.Proxy, "http://127.0.0.1:7890")
	}
}
