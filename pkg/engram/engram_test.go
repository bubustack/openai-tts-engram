package engram

import (
	"bytes"
	"context"
	"testing"
	"time"

	sdkengram "github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/tractatus/transport"
)

func TestEmitSynthesisSignal(t *testing.T) {
	ctx := context.Background()
	var capturedKey string
	var capturedPayload map[string]any
	orig := emitSignalFunc
	emitSignalFunc = func(_ context.Context, key string, payload any) error {
		capturedKey = key
		if m, ok := payload.(map[string]any); ok {
			capturedPayload = m
		}
		return nil
	}
	t.Cleanup(func() { emitSignalFunc = orig })

	summary := map[string]any{
		"type":           "speech.audio.v1",
		"model":          "gpt-4o-mini-tts",
		"voice":          "alloy",
		"responseFormat": "pcm",
		"chunks":         3,
		"audio": map[string]any{
			"sampleRate": 48000,
			"channels":   1,
			"encoding":   "pcm",
		},
	}

	engine := &OpenAITTS{}
	engine.emitSynthesisSignal(ctx, nil, summary)

	if capturedKey != "speech.audio.summary" {
		t.Fatalf("expected speech.audio.summary, got %s", capturedKey)
	}
	if capturedPayload["model"] != "gpt-4o-mini-tts" {
		t.Fatalf("expected model gpt-4o-mini-tts, got %v", capturedPayload["model"])
	}
	if capturedPayload["voice"] != "alloy" {
		t.Fatalf("expected voice alloy, got %v", capturedPayload["voice"])
	}
	if capturedPayload["sampleRate"] != 48000 {
		t.Fatalf("expected sampleRate 48000, got %v", capturedPayload["sampleRate"])
	}
	if capturedPayload["channels"] != 1 {
		t.Fatalf("expected channels 1, got %v", capturedPayload["channels"])
	}
}

func TestJSONBinaryStreamMessageMirrorsPayload(t *testing.T) {
	payload := []byte(`{"type":"speech.audio.v1","chunks":3}`)
	msg := jsonBinaryStreamMessage(map[string]string{"type": "speech.audio.done.v1"}, payload)
	if msg.Binary == nil {
		t.Fatal("expected binary frame")
	}
	if !bytes.Equal(msg.Payload, msg.Binary.Payload) {
		t.Fatalf("expected mirrored payload, payload=%q binary=%q", string(msg.Payload), string(msg.Binary.Payload))
	}
}

func TestStreamContinuesAfterMalformedPacket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	engine := &OpenAITTS{}
	in := make(chan sdkengram.InboundMessage, 1)
	out := make(chan sdkengram.StreamMessage, 1)

	in <- sdkengram.NewInboundMessage(sdkengram.StreamMessage{
		Payload: []byte("{invalid json"),
	})
	close(in)

	if err := engine.Stream(ctx, in, out); err != nil {
		t.Fatalf("expected malformed packet to be skipped, got err=%v", err)
	}
}

func TestNewStreamDoneSummaryUsesDoneType(t *testing.T) {
	engine := &OpenAITTS{}
	summary := engine.newStreamDoneSummary(
		speechRequest{
			model:               "gpt-4o-mini-tts",
			voice:               "alloy",
			format:              "pcm",
			streamFormat:        "sse",
			speed:               1.0,
			instructions:        "",
			instructionsApplied: false,
		},
		"audio/L16",
		3,
		48000,
		1,
		"hello",
	)
	if got := summary["type"]; got != transport.StreamTypeSpeechAudioDone {
		t.Fatalf("expected done summary type %q, got %v", transport.StreamTypeSpeechAudioDone, got)
	}
}
