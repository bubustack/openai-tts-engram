package engram

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	sdk "github.com/bubustack/bubu-sdk-go"
)

func (e *OpenAITTS) debugEnabled(ctx context.Context, logger *slog.Logger) bool {
	if sdk.DebugModeEnabled() {
		return true
	}
	if logger == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return logger.Enabled(ctx, slog.LevelDebug)
}

func (e *OpenAITTS) logTTSRequest(ctx context.Context, logger *slog.Logger, req *TTSRequest) {
	if !e.debugEnabled(ctx, logger) || req == nil {
		return
	}
	sampleRate := ptrValue(req.SampleRate, 0)
	channels := ptrValue(req.Channels, 0)
	logger.Debug("openai tts request",
		slog.String("model", strings.TrimSpace(req.Model)),
		slog.String("voice", strings.TrimSpace(req.Voice)),
		slog.String("format", strings.TrimSpace(req.Format)),
		slog.String("streamFormat", strings.TrimSpace(req.StreamFormat)),
		slog.Int("sampleRate", sampleRate),
		slog.Int("channels", channels),
		slog.Float64("speed", ptrValue(req.Speed, 0)),
		slog.String("textPreview", previewText(req.Text, 120)),
	)
}

func (e *OpenAITTS) logTTSResult(ctx context.Context, logger *slog.Logger, payload map[string]any) {
	if !e.debugEnabled(ctx, logger) || payload == nil {
		return
	}
	audioSummary := map[string]any{}
	if audioRaw, ok := payload["audio"].(map[string]any); ok {
		if enc, ok := audioRaw["encoding"].(string); ok {
			audioSummary["encoding"] = enc
		}
		if sr, ok := audioRaw["sampleRate"].(int); ok {
			audioSummary["sampleRate"] = sr
		}
		if ch, ok := audioRaw["channels"].(int); ok {
			audioSummary["channels"] = ch
		}
		if _, ok := audioRaw["storage"]; ok {
			audioSummary["storage"] = "ref"
		} else if data, ok := audioRaw["data"].(string); ok {
			audioSummary["dataBytes"] = len(data)
		}
	}
	logger.Debug("openai tts response",
		slog.String("model", fmt.Sprint(payload["model"])),
		slog.String("voice", fmt.Sprint(payload["voice"])),
		slog.String("responseFormat", fmt.Sprint(payload["responseFormat"])),
		slog.Any("audio", audioSummary),
	)
}

func (e *OpenAITTS) logDebug(ctx context.Context, logger *slog.Logger, msg string, attrs ...any) {
	if !e.debugEnabled(ctx, logger) || logger == nil {
		return
	}
	logger.Debug(msg, attrs...)
}

func ptrValue[T any](ptr *T, zero T) T {
	if ptr == nil {
		return zero
	}
	return *ptr
}
