package config

import "testing"

func TestNormalizeDefaultsStreamFormatToSSE(t *testing.T) {
	got := Normalize(Config{})
	if got.StreamFormat != DefaultStreamFormat {
		t.Fatalf("expected default stream format %q, got %q", DefaultStreamFormat, got.StreamFormat)
	}
}

func TestNormalizeStreamFormatFallsBackToDefaultOnInvalidValue(t *testing.T) {
	got := Normalize(Config{StreamFormat: "invalid"})
	if got.StreamFormat != DefaultStreamFormat {
		t.Fatalf("expected fallback stream format %q, got %q", DefaultStreamFormat, got.StreamFormat)
	}
}
