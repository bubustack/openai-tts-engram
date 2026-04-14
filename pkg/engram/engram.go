package engram

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bubustack/bobrapet/pkg/storage"
	sdk "github.com/bubustack/bubu-sdk-go"
	sdkengram "github.com/bubustack/bubu-sdk-go/engram"
	"github.com/bubustack/bubu-sdk-go/media"
	ttscfg "github.com/bubustack/openai-tts-engram/pkg/config"
	"github.com/bubustack/tractatus/transport"
	openai "github.com/openai/openai-go/v2"
	"github.com/openai/openai-go/v2/azure"
	"github.com/openai/openai-go/v2/option"
)

const (
	defaultBaseURL    = "https://api.openai.com/v1"
	defaultAzureAPIV  = "2024-06-01"
	componentName     = "openai-tts-engram"
	streamFormatSSE   = "sse"
	streamFormatAudio = "audio"
	audioEncodingPCM  = "pcm"
	summaryFieldAudio = streamFormatAudio
)

var emitSignalFunc = sdk.EmitSignal

type TTSRequest struct {
	Text         string   `json:"text"`
	Voice        string   `json:"voice"`
	Model        string   `json:"model"`
	Format       string   `json:"format"`
	StreamFormat string   `json:"streamFormat"`
	SampleRate   *int     `json:"sampleRate"`
	Channels     *int     `json:"channels"`
	Speed        *float64 `json:"speed"`
	Instructions string   `json:"instructions"`
}

type Input = TTSRequest

type speechRequest struct {
	params                openai.AudioSpeechNewParams
	requestOpts           []option.RequestOption
	format                string
	streamFormat          string
	model                 string
	voice                 string
	speed                 float64
	instructions          string
	instructionsApplied   bool
	instructionsRequested bool
	requestSampleRate     int
	requestChannels       int
}

type OpenAITTS struct {
	cfg             ttscfg.Config
	secrets         *sdkengram.Secrets
	client          *openai.Client
	isAzure         bool
	azureDeployment string
}

func New() *OpenAITTS { return &OpenAITTS{} }

func (e *OpenAITTS) Init(_ context.Context, cfg ttscfg.Config, secrets *sdkengram.Secrets) error {
	cfg = ttscfg.Normalize(cfg)
	client, isAzure, deployment, err := newOpenAIClient(secrets)
	if err != nil {
		return err
	}
	e.cfg = cfg
	e.secrets = secrets
	e.client = client
	e.isAzure = isAzure
	e.azureDeployment = deployment
	return nil
}

func (e *OpenAITTS) Process(
	ctx context.Context,
	execCtx *sdkengram.ExecutionContext,
	req Input,
) (*sdkengram.Result, error) {
	logger := execCtx.Logger().With(
		"component", componentName,
		"mode", "batch",
	)
	if strings.TrimSpace(req.Text) == "" {
		return nil, fmt.Errorf("text is required for TTS")
	}
	e.normalizeRequestedModel(logger, &req)
	e.logTTSRequest(ctx, logger, &req)
	e.logDebug(ctx, logger, "processing batch tts request",
		"textPreview", previewText(req.Text, 160),
		"model", firstNonEmpty(req.Model, e.cfg.Model),
	)
	result, err := e.synthesize(ctx, req, logger, storageContextFromStoryInfo(execCtx.StoryInfo()))
	if err != nil {
		return nil, err
	}
	return sdkengram.NewResultFrom(result), nil
}

func (e *OpenAITTS) Stream(
	ctx context.Context,
	in <-chan sdkengram.InboundMessage,
	out chan<- sdkengram.StreamMessage,
) error {
	logger := sdk.LoggerFromContext(ctx).With(
		"component", componentName,
		"mode", "stream",
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-in:
			if !ok {
				return nil
			}
			if err := e.handleStreamMessage(ctx, logger, msg, out); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				logger.Warn("Failed to process TTS stream packet", "error", err)
				msg.Done()
				continue
			}
		}
	}
}

func (e *OpenAITTS) handleStreamMessage(
	ctx context.Context,
	logger *slog.Logger,
	msg sdkengram.InboundMessage,
	out chan<- sdkengram.StreamMessage,
) error {
	logStreamEnvelope(logger, msg)
	if shouldSkipHeartbeatMessage(logger, msg) {
		msg.Done()
		return nil
	}
	stripHeartbeatMetadata(msg.Metadata)

	req, source, skip, err := decodeStreamTTSRequest(logger, msg)
	if err != nil {
		return err
	}
	if skip {
		msg.Done()
		return nil
	}
	logParsedStreamRequest(logger, req, source, len(msg.Metadata))

	if strings.TrimSpace(req.Text) == "" {
		logger.Warn("Skipping TTS request without text",
			"source", source,
			"metadataKeys", len(msg.Metadata),
		)
		msg.Done()
		return nil
	}

	e.normalizeRequestedModel(logger, &req)
	e.logTTSRequest(ctx, logger, &req)
	e.logDebug(ctx, logger, "received streaming tts payload",
		"textPreview", previewText(req.Text, 160),
		"metadataKeys", len(msg.Metadata),
	)

	prepared, err := e.prepareSpeechRequest(req)
	if err != nil {
		return err
	}
	if err := e.streamPreparedSpeech(ctx, prepared, logger, msg.Metadata, out); err != nil {
		return err
	}
	msg.Done()
	return nil
}

func logStreamEnvelope(logger *slog.Logger, msg sdkengram.InboundMessage) {
	hbValue := ""
	if len(msg.Metadata) > 0 {
		hbValue = msg.Metadata["bubu-heartbeat"]
	}
	binaryLen := 0
	binaryMime := ""
	if msg.Binary != nil {
		binaryLen = len(msg.Binary.Payload)
		binaryMime = msg.Binary.MimeType
	}
	logger.Info("TTS stream message received",
		"metadataKeys", len(msg.Metadata),
		"heartbeat", hbValue,
		"payloadLen", len(msg.Payload),
		"inputsLen", len(msg.Inputs),
		"binaryLen", binaryLen,
		"binaryMime", binaryMime,
		"hasContent", streamMessageHasContent(msg),
	)
}

func shouldSkipHeartbeatMessage(logger *slog.Logger, msg sdkengram.InboundMessage) bool {
	if !isHeartbeat(msg) {
		return false
	}
	if streamMessageHasContent(msg) {
		logger.Warn("TTS heartbeat flag set on message with content; ignoring flag",
			"payloadLen", len(msg.Payload),
			"inputsLen", len(msg.Inputs),
			"binaryLen", binaryPayloadLength(msg),
		)
		return false
	}
	logger.Info("TTS ignoring heartbeat")
	return true
}

func binaryPayloadLength(msg sdkengram.InboundMessage) int {
	if msg.Binary == nil {
		return 0
	}
	return len(msg.Binary.Payload)
}

func decodeStreamTTSRequest(
	logger *slog.Logger,
	msg sdkengram.InboundMessage,
) (TTSRequest, string, bool, error) {
	source, raw := streamRequestBytes(msg)
	if len(raw) == 0 {
		logger.Debug("Skipping stream message without textual payload",
			"metadataKeys", len(msg.Metadata),
			"messageID", msg.MessageID,
		)
		return TTSRequest{}, "", true, nil
	}

	var req TTSRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		logStreamDecodeError(logger, msg, source, err)
		return TTSRequest{}, source, false, fmt.Errorf("failed to parse %s: %w", source, err)
	}
	return req, source, false, nil
}

func streamRequestBytes(msg sdkengram.InboundMessage) (string, []byte) {
	switch {
	case len(msg.Inputs) > 0:
		return "inputs", msg.Inputs
	case len(msg.Payload) > 0:
		return "payload", msg.Payload
	case msg.Binary != nil && len(msg.Binary.Payload) > 0:
		return "binary", msg.Binary.Payload
	default:
		return "", nil
	}
}

func logStreamDecodeError(
	logger *slog.Logger,
	msg sdkengram.InboundMessage,
	source string,
	err error,
) {
	switch source {
	case "inputs":
		logger.Warn("Failed to parse TTS inputs payload", "error", err, "len", len(msg.Inputs))
	case "payload":
		logger.Warn("Failed to parse TTS payload", "error", err, "len", len(msg.Payload))
	case "binary":
		logger.Warn("Failed to parse TTS binary payload",
			"error", err,
			"len", binaryPayloadLength(msg),
			"mime", binaryMimeType(msg),
		)
	}
}

func binaryMimeType(msg sdkengram.InboundMessage) string {
	if msg.Binary == nil {
		return ""
	}
	return msg.Binary.MimeType
}

func logParsedStreamRequest(
	logger *slog.Logger,
	req TTSRequest,
	source string,
	metadataKeys int,
) {
	logger.Info("TTS stream message parsed",
		"source", source,
		"textLen", len(strings.TrimSpace(req.Text)),
		"voice", req.Voice,
		"model", req.Model,
		"format", req.Format,
		"streamFormat", req.StreamFormat,
		"sampleRate", req.SampleRate,
		"channels", req.Channels,
		"metadataKeys", metadataKeys,
	)
}

func (e *OpenAITTS) normalizeRequestedModel(logger *slog.Logger, req *TTSRequest) {
	if req == nil || e.isAzure {
		return
	}
	if strings.TrimSpace(req.Model) == "" || isTTSModel(req.Model) {
		return
	}
	logger.Warn("Ignoring non-TTS model for speech synthesis",
		"model", req.Model,
		"fallback", e.cfg.Model,
	)
	req.Model = ""
}

func (e *OpenAITTS) streamPreparedSpeech(
	ctx context.Context,
	prepared speechRequest,
	logger *slog.Logger,
	metadata map[string]string,
	out chan<- sdkengram.StreamMessage,
) error {
	if prepared.streamFormat == streamFormatSSE {
		return e.streamSpeechSSE(ctx, prepared, logger, metadata, out)
	}
	return e.streamSpeechBuffered(ctx, prepared, logger, metadata, out)
}

func (e *OpenAITTS) providerName() string {
	if e.isAzure {
		return "azure-openai"
	}
	return "openai"
}

func isHeartbeat(msg sdkengram.InboundMessage) bool {
	if len(msg.Metadata) == 0 {
		return false
	}
	value, ok := msg.Metadata["bubu-heartbeat"]
	if !ok {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(value), "true")
}

func streamMessageHasContent(msg sdkengram.InboundMessage) bool {
	if msg.Audio != nil && len(msg.Audio.PCM) > 0 {
		return true
	}
	if msg.Video != nil && len(msg.Video.Payload) > 0 {
		return true
	}
	if msg.Binary != nil && len(msg.Binary.Payload) > 0 {
		return true
	}
	if len(msg.Payload) > 0 {
		return true
	}
	if len(msg.Inputs) > 0 {
		return true
	}
	return false
}

func stripHeartbeatMetadata(metadata map[string]string) {
	if len(metadata) == 0 {
		return
	}
	delete(metadata, "bubu-heartbeat")
}

func (e *OpenAITTS) synthesize(
	ctx context.Context,
	req TTSRequest,
	logger *slog.Logger,
	storeCtx storageContext,
) (map[string]any, error) {
	prepared, err := e.prepareSpeechRequest(req)
	if err != nil {
		return nil, err
	}
	result, _, err := e.executeSpeechRequest(ctx, prepared, logger, storeCtx)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *OpenAITTS) prepareSpeechRequest(req TTSRequest) (speechRequest, error) {
	var prepared speechRequest
	voice := firstNonEmpty(req.Voice, e.cfg.Voice)
	format, formatEnum, err := resolveSpeechFormat(req.Format, e.cfg.Format)
	if err != nil {
		return prepared, err
	}
	streamFormat := sanitizeStreamFormat(firstNonEmpty(req.StreamFormat, e.cfg.StreamFormat))
	streamEnum, err := resolveStreamFormat(streamFormat)
	if err != nil {
		return prepared, err
	}
	model := firstNonEmpty(req.Model, e.cfg.Model)
	if e.isAzure {
		model = firstNonEmpty(req.Model, e.azureDeployment)
	}
	if strings.TrimSpace(model) == "" {
		return prepared, fmt.Errorf("model or azure deployment is required")
	}

	speed := e.cfg.Speed
	if req.Speed != nil {
		speed = clampSpeed(*req.Speed)
	}
	instructions := strings.TrimSpace(req.Instructions)
	if instructions == "" {
		instructions = strings.TrimSpace(e.cfg.Instructions)
	}

	params := openai.AudioSpeechNewParams{
		Input:          req.Text,
		Model:          model,
		Voice:          openai.AudioSpeechNewParamsVoice(voice),
		ResponseFormat: formatEnum,
	}

	if streamEnum != nil {
		params.StreamFormat = *streamEnum
	}
	if speed != ttscfg.DefaultSpeed {
		params.Speed = openai.Float(speed)
	}

	instructionsAllowed := supportsTTSInstructions(model)
	instructionsApplied := false
	instructionsRequested := strings.TrimSpace(instructions) != ""
	if instructionsRequested && instructionsAllowed {
		params.Instructions = openai.String(instructions)
		instructionsApplied = true
	}

	prepared = speechRequest{
		params:                params,
		format:                format,
		streamFormat:          streamFormat,
		model:                 model,
		voice:                 voice,
		speed:                 speed,
		instructions:          instructions,
		instructionsApplied:   instructionsApplied,
		instructionsRequested: instructionsRequested,
		requestSampleRate:     derefPositiveInt(req.SampleRate),
		requestChannels:       derefPositiveInt(req.Channels),
	}

	if streamFormat == streamFormatSSE {
		prepared.requestOpts = append(prepared.requestOpts, option.WithHeader("Accept", "text/event-stream"))
	}

	return prepared, nil
}

func (e *OpenAITTS) executeSpeechRequest(
	ctx context.Context,
	prepared speechRequest,
	logger *slog.Logger,
	storeCtx storageContext,
) (map[string]any, []byte, error) {
	e.logDebug(ctx, logger, "dispatching TTS request",
		"model", prepared.model,
		"voice", prepared.voice,
		"format", prepared.format,
		"streamFormat", prepared.streamFormat,
		"azure", e.isAzure,
		"speed", prepared.speed,
	)

	if prepared.instructionsRequested && !prepared.instructionsApplied && prepared.instructions != "" {
		logger.Info("voice instructions ignored for model that does not support them", "model", prepared.model)
	}

	resp, err := e.requestSpeech(ctx, prepared)
	if err != nil {
		return nil, nil, err
	}
	defer closeResponseBody(resp.Body)

	audioBytes, transcript, contentType, err := e.readSpeechResponse(prepared, resp, logger)
	if err != nil {
		return nil, nil, err
	}

	e.logDebug(ctx, logger, "openai tts response received",
		"bytes", len(audioBytes),
		"contentType", contentType,
	)

	processedAudio, outputSampleRate, effectiveChannels := e.resolveSpeechOutputAudio(
		prepared,
		contentType,
		audioBytes,
		logger,
	)
	result := e.buildSpeechResult(
		ctx,
		logger,
		prepared,
		processedAudio,
		contentType,
		transcript,
		outputSampleRate,
		effectiveChannels,
		storeCtx,
	)
	e.logTTSResult(ctx, logger, result)
	e.emitSynthesisSignal(ctx, logger, result)

	return result, processedAudio, nil
}

func (e *OpenAITTS) requestSpeech(
	ctx context.Context,
	prepared speechRequest,
) (*http.Response, error) {
	resp, err := e.client.Audio.Speech.New(ctx, prepared.params, prepared.requestOpts...)
	if err != nil {
		return nil, fmt.Errorf("openai tts request failed: %w", err)
	}
	if resp.StatusCode < 300 {
		return resp, nil
	}
	defer closeResponseBody(resp.Body)
	bodyBytes, _ := io.ReadAll(resp.Body)
	return nil, fmt.Errorf("openai tts returned %s: %s", resp.Status, string(bodyBytes))
}

func closeResponseBody(body io.ReadCloser) {
	if body == nil {
		return
	}
	_ = body.Close()
}

func (e *OpenAITTS) readSpeechResponse(
	prepared speechRequest,
	resp *http.Response,
	logger *slog.Logger,
) ([]byte, string, string, error) {
	contentType := resp.Header.Get("Content-Type")
	if prepared.streamFormat == streamFormatSSE {
		audioBytes, transcript, err := e.consumeSpeechSSE(resp.Body, logger, speechSSEConsumer{
			CollectAudio:      true,
			CollectTranscript: true,
		})
		if err != nil {
			return nil, "", "", err
		}
		return audioBytes, transcript, contentTypeForFormat(prepared.format), nil
	}

	audioBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to read tts response: %w", err)
	}
	return audioBytes, "", contentType, nil
}

func (e *OpenAITTS) resolveSpeechOutputAudio(
	prepared speechRequest,
	contentType string,
	audioBytes []byte,
	logger *slog.Logger,
) ([]byte, int, int) {
	detectedSampleRate := parseSampleRateFromContentType(contentType)
	detectedChannels := parseChannelsFromContentType(contentType)
	effectiveSampleRate := firstPositiveInt(
		detectedSampleRate,
		prepared.requestSampleRate,
		e.cfg.SampleRate,
		ttscfg.DefaultSampleRate,
	)
	effectiveChannels := firstPositiveInt(
		detectedChannels,
		prepared.requestChannels,
		e.cfg.Channels,
		ttscfg.DefaultChannels,
	)

	processedAudio := audioBytes
	outputSampleRate := effectiveSampleRate
	if e.cfg.TargetSampleRate > 0 && effectiveSampleRate > 0 && e.cfg.TargetSampleRate != effectiveSampleRate {
		resampled, err := resamplePCM16(processedAudio, effectiveSampleRate, e.cfg.TargetSampleRate)
		if err != nil {
			logger.Warn("failed to resample TTS audio",
				"from", effectiveSampleRate,
				"to", e.cfg.TargetSampleRate,
				"error", err,
			)
		} else {
			processedAudio = resampled
			outputSampleRate = e.cfg.TargetSampleRate
		}
	}
	return processedAudio, outputSampleRate, effectiveChannels
}

func (e *OpenAITTS) buildSpeechResult(
	ctx context.Context,
	logger *slog.Logger,
	prepared speechRequest,
	audio []byte,
	contentType string,
	transcript string,
	sampleRate int,
	channels int,
	storeCtx storageContext,
) map[string]any {
	result := map[string]any{
		"type":           transport.StreamTypeSpeechAudio,
		"model":          prepared.model,
		"voice":          prepared.voice,
		"responseFormat": prepared.format,
	}
	if prepared.streamFormat != "" {
		result["streamFormat"] = prepared.streamFormat
	}
	if contentType != "" {
		result["contentType"] = contentType
	}
	if transcript != "" {
		result["transcript"] = transcript
	}

	result[summaryFieldAudio] = e.buildAudioPayload(ctx, logger, audio, audioPayloadOptions{
		format:      prepared.format,
		contentType: contentType,
		sampleRate:  sampleRate,
		channels:    channels,
		storeCtx:    storeCtx,
	})
	if prepared.speed != ttscfg.DefaultSpeed {
		result["speed"] = prepared.speed
	}
	if prepared.instructionsApplied && prepared.instructions != "" {
		result["instructions"] = prepared.instructions
	}
	return result
}

func (e *OpenAITTS) streamSpeechSSE(
	ctx context.Context,
	prepared speechRequest,
	logger *slog.Logger,
	baseMetadata map[string]string,
	out chan<- sdkengram.StreamMessage,
) error {
	if prepared.streamFormat != streamFormatSSE {
		// Fallback to regular execution if stream format changed during normalization.
		result, _, err := e.executeSpeechRequest(ctx, prepared, logger, storageContextFromMetadata(baseMetadata))
		if err != nil {
			return err
		}
		payloadBytes, err := json.Marshal(result)
		if err != nil {
			return err
		}
		metadata := cloneMetadata(baseMetadata)
		metadata["provider"] = e.providerName()
		metadata["type"] = transport.StreamTypeSpeechAudio
		metadata["model"] = prepared.model
		select {
		case out <- jsonBinaryStreamMessage(metadata, payloadBytes):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	resp, err := e.requestSpeech(ctx, prepared)
	if err != nil {
		return err
	}
	defer closeResponseBody(resp.Body)

	targetContentType := contentTypeForFormat(prepared.format)
	sampleRate := firstPositiveInt(
		prepared.requestSampleRate,
		e.cfg.SampleRate,
		ttscfg.DefaultSampleRate,
	)
	if sampleRate == 0 {
		sampleRate = ttscfg.DefaultSampleRate
	}
	channels := firstPositiveInt(
		prepared.requestChannels,
		e.cfg.Channels,
		ttscfg.DefaultChannels,
	)
	if channels == 0 {
		channels = ttscfg.DefaultChannels
	}

	baseMeta := cloneMetadata(baseMetadata)
	baseMeta["provider"] = e.providerName()
	baseMeta["model"] = prepared.model
	baseMeta["responseFormat"] = prepared.format
	baseMeta["streamFormat"] = prepared.streamFormat

	var chunkCount int
	var transcriptFinal string
	consumer := speechSSEConsumer{
		CollectAudio:      false,
		CollectTranscript: true,
		OnAudioDelta: func(chunk []byte) error {
			chunkCount++
			return e.emitAudioChunk(ctx, baseMeta, prepared, chunk, sampleRate, channels, chunkCount, out, logger)
		},
		OnTranscriptDone: func(text string) error {
			transcriptFinal = text
			return nil
		},
	}

	_, transcript, err := e.consumeSpeechSSE(resp.Body, logger, consumer)
	if err != nil {
		return err
	}
	if transcriptFinal == "" {
		transcriptFinal = transcript
	}

	metadata := cloneMetadata(baseMeta)
	metadata["type"] = transport.StreamTypeSpeechAudioDone
	metadata["stream"] = "complete"

	summary := e.newStreamDoneSummary(prepared, targetContentType, chunkCount, sampleRate, channels, transcriptFinal)
	e.logTTSResult(ctx, logger, summary)
	payloadBytes, err := json.Marshal(summary)
	if err != nil {
		return err
	}

	select {
	case out <- jsonBinaryStreamMessage(metadata, payloadBytes):
		logger.Info("openai tts stream completed",
			"model", prepared.model,
			"voice", prepared.voice,
			"chunks", chunkCount,
		)
		e.emitSynthesisSignal(ctx, logger, summary)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *OpenAITTS) streamSpeechBuffered(
	ctx context.Context,
	prepared speechRequest,
	logger *slog.Logger,
	baseMetadata map[string]string,
	out chan<- sdkengram.StreamMessage,
) error {
	storeCtx := storageContextFromMetadata(baseMetadata)
	result, audioBytes, err := e.executeSpeechRequest(ctx, prepared, logger, storeCtx)
	if err != nil {
		return err
	}
	if len(audioBytes) == 0 {
		return fmt.Errorf("openai tts returned empty audio payload")
	}

	baseMeta := cloneMetadata(baseMetadata)
	baseMeta["provider"] = e.providerName()
	baseMeta["model"] = prepared.model
	baseMeta["responseFormat"] = prepared.format
	if prepared.streamFormat != "" {
		baseMeta["streamFormat"] = prepared.streamFormat
	}

	audioMap, _ := result[summaryFieldAudio].(map[string]any)
	sampleRate := firstPositiveInt(
		anyToInt(audioMap["sampleRate"]),
		prepared.requestSampleRate,
		e.cfg.SampleRate,
		ttscfg.DefaultSampleRate,
	)
	channels := firstPositiveInt(
		anyToInt(audioMap["channels"]),
		prepared.requestChannels,
		e.cfg.Channels,
		ttscfg.DefaultChannels,
	)
	if sampleRate <= 0 {
		sampleRate = ttscfg.DefaultSampleRate
	}
	if channels <= 0 {
		channels = ttscfg.DefaultChannels
	}

	chunkBytes := pcmChunkSize(sampleRate, channels)
	if chunkBytes <= 0 || chunkBytes > len(audioBytes) {
		chunkBytes = len(audioBytes)
	}

	chunkCount := 0
	for offset := 0; offset < len(audioBytes); offset += chunkBytes {
		end := offset + chunkBytes
		if end > len(audioBytes) {
			end = len(audioBytes)
		}
		chunk := audioBytes[offset:end]
		chunkCount++
		if err := e.emitAudioChunk(
			ctx,
			baseMeta,
			prepared,
			chunk,
			sampleRate,
			channels,
			chunkCount,
			out,
			logger,
		); err != nil {
			return err
		}
	}

	targetContentType := fmt.Sprint(result["contentType"])
	if targetContentType == "" {
		targetContentType = contentTypeForFormat(prepared.format)
	}

	summary := e.newStreamDoneSummary(prepared, targetContentType, chunkCount, sampleRate, channels, "")

	metadata := cloneMetadata(baseMeta)
	metadata["type"] = transport.StreamTypeSpeechAudioDone
	metadata["stream"] = "complete"

	payloadBytes, err := json.Marshal(summary)
	if err != nil {
		return err
	}

	select {
	case out <- jsonBinaryStreamMessage(metadata, payloadBytes):
		logger.Info("openai tts stream completed",
			"model", prepared.model,
			"voice", prepared.voice,
			"chunks", chunkCount,
		)
		e.emitSynthesisSignal(ctx, logger, summary)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *OpenAITTS) newStreamDoneSummary(
	prepared speechRequest,
	targetContentType string,
	chunkCount int,
	sampleRate int,
	channels int,
	transcript string,
) map[string]any {
	summary := map[string]any{
		"type":           transport.StreamTypeSpeechAudioDone,
		"model":          prepared.model,
		"voice":          prepared.voice,
		"responseFormat": prepared.format,
		"streamFormat":   prepared.streamFormat,
		"contentType":    targetContentType,
		"chunks":         chunkCount,
	}
	if sampleRate > 0 {
		summary["sampleRate"] = sampleRate
	}
	if channels > 0 {
		summary["channels"] = channels
	}
	if prepared.speed != ttscfg.DefaultSpeed {
		summary["speed"] = prepared.speed
	}
	if prepared.instructionsApplied && prepared.instructions != "" {
		summary["instructions"] = prepared.instructions
	}
	if strings.TrimSpace(transcript) != "" {
		summary["transcript"] = transcript
	}
	return summary
}

func (e *OpenAITTS) emitAudioChunk(
	ctx context.Context,
	baseMeta map[string]string,
	prepared speechRequest,
	chunk []byte,
	sampleRate int,
	channels int,
	sequence int,
	out chan<- sdkengram.StreamMessage,
	logger *slog.Logger,
) error {
	if len(chunk) == 0 {
		return nil
	}
	metadata := cloneMetadata(baseMeta)
	metadata["type"] = transport.StreamTypeSpeechAudioDelta
	metadata["chunkIndex"] = strconv.Itoa(sequence)

	msg := sdkengram.StreamMessage{Metadata: metadata}
	if strings.EqualFold(prepared.format, audioEncodingPCM) {
		msg.Audio = &sdkengram.AudioFrame{
			PCM:          append([]byte(nil), chunk...),
			SampleRateHz: int32(sampleRate),
			Channels:     int32(channels),
			Codec:        "pcm16",
		}
	} else {
		msg.Binary = &sdkengram.BinaryFrame{
			Payload:  append([]byte(nil), chunk...),
			MimeType: contentTypeForFormat(prepared.format),
		}
	}

	select {
	case out <- msg:
		e.logDebug(ctx, logger, "emitted openai tts audio chunk",
			"chunk", sequence,
			"bytes", len(chunk),
			"format", prepared.format,
		)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func pcmChunkSize(sampleRate, channels int) int {
	if sampleRate <= 0 {
		return 0
	}
	if channels <= 0 {
		channels = 1
	}
	samplesPerChunk := sampleRate / 50 // ~20ms windows
	if samplesPerChunk <= 0 {
		samplesPerChunk = sampleRate
	}
	return samplesPerChunk * channels * 2
}

func anyToInt(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float32:
		return int(v)
	case float64:
		return int(v)
	case json.Number:
		i, _ := v.Int64()
		return int(i)
	default:
		return 0
	}
}

type audioPayloadOptions struct {
	format      string
	contentType string
	sampleRate  int
	channels    int
	storeCtx    storageContext
}

type storageContext struct {
	namespace string
	storyRun  string
	step      string
	scope     []string
}

func (c storageContext) writeOptions(contentType string) media.WriteOptions {
	scope := append([]string(nil), c.scope...)
	return media.WriteOptions{
		Namespace:   c.namespace,
		StoryRun:    c.storyRun,
		Step:        c.step,
		Scope:       scope,
		ContentType: contentType,
	}
}

func storageContextFromMetadata(meta map[string]string) storageContext {
	return storageContext{
		namespace: meta["storyrun-namespace"],
		storyRun:  meta["storyrun-name"],
		step:      meta["current-step-id"],
		scope:     scopeFromMetadata(meta),
	}
}

func storageContextFromStoryInfo(info sdkengram.StoryInfo) storageContext {
	return storageContext{
		namespace: info.StepRunNamespace,
		storyRun:  info.StoryRunID,
		step:      info.StepName,
		scope:     scopeFromStoryInfo(info),
	}
}

func (e *OpenAITTS) emitSynthesisSignal(ctx context.Context, logger *slog.Logger, summary map[string]any) {
	if summary == nil {
		return
	}
	payload := buildSynthesisSignalPayload(summary)
	if len(payload) == 0 {
		return
	}
	if logger == nil {
		logger = sdk.LoggerFromContext(ctx)
	}
	if err := emitSignalFunc(ctx, "speech.audio.summary", payload); err != nil &&
		!errors.Is(err, sdk.ErrSignalsUnavailable) {
		logger.Warn("Failed to emit TTS signal", "error", err)
	}
}

func buildSynthesisSignalPayload(summary map[string]any) map[string]any {
	payload := make(map[string]any)
	for _, key := range []string{
		"type",
		"model",
		"voice",
		"responseFormat",
		"streamFormat",
		"contentType",
		"chunks",
		"speed",
		"instructions",
		"transcript",
	} {
		copySummaryField(payload, summary, key)
	}
	ensureSummaryField(payload, summary, "sampleRate")
	ensureSummaryField(payload, summary, "channels")
	mergeSignalAudioFields(payload, summary)
	ensureSummaryField(payload, summary, "encoding")
	if _, ok := payload["type"]; !ok {
		payload["type"] = transport.StreamTypeSpeechAudio
	}
	return payload
}

func copySummaryField(dst, src map[string]any, key string) {
	if v, ok := src[key]; ok {
		dst[key] = v
	}
}

func ensureSummaryField(dst, src map[string]any, key string) {
	if _, exists := dst[key]; exists {
		return
	}
	copySummaryField(dst, src, key)
}

func mergeSignalAudioFields(payload, summary map[string]any) {
	audioRaw, ok := summary[summaryFieldAudio].(map[string]any)
	if !ok {
		return
	}
	ensureSummaryField(payload, audioRaw, "sampleRate")
	ensureSummaryField(payload, audioRaw, "channels")
	copySummaryField(payload, audioRaw, "encoding")
	copySummaryField(payload, audioRaw, "storage")
}

func jsonBinaryStreamMessage(metadata map[string]string, payload []byte) sdkengram.StreamMessage {
	body := append([]byte(nil), payload...)
	return sdkengram.StreamMessage{
		Metadata: metadata,
		Payload:  body,
		Binary: &sdkengram.BinaryFrame{
			Payload:  body,
			MimeType: "application/json",
		},
	}
}

type speechSSEConsumer struct {
	CollectAudio      bool
	CollectTranscript bool
	OnAudioDelta      func([]byte) error
	OnTranscriptDelta func(string) error
	OnTranscriptDone  func(string) error
	OnCompleted       func() error
}

type speechSSEState struct {
	eventName       string
	dataBuilder     strings.Builder
	audioBuffer     []byte
	transcript      strings.Builder
	finalTranscript string
	streamComplete  bool
}

func (e *OpenAITTS) consumeSpeechSSE(
	body io.Reader,
	logger *slog.Logger,
	consumer speechSSEConsumer,
) ([]byte, string, error) {
	reader := bufio.NewReader(body)
	debugEnabled := e.debugEnabled(context.Background(), logger)
	state := &speechSSEState{}

	for !state.streamComplete {
		line, isEOF, err := readSpeechSSELine(reader)
		if err != nil {
			return nil, "", err
		}
		if err := state.consumeLine(line, logger, debugEnabled, consumer); err != nil {
			return nil, "", err
		}
		if !isEOF {
			continue
		}
		if err := state.flush(logger, debugEnabled, consumer); err != nil {
			return nil, "", err
		}
		break
	}

	if consumer.CollectAudio && len(state.audioBuffer) == 0 {
		return nil, "", fmt.Errorf("speech stream completed without audio payload")
	}
	return state.audioBuffer, state.transcriptText(), nil
}

func readSpeechSSELine(reader *bufio.Reader) (string, bool, error) {
	line, err := reader.ReadString('\n')
	isEOF := errors.Is(err, io.EOF)
	if err != nil && !isEOF {
		return "", false, fmt.Errorf("read speech stream: %w", err)
	}
	return strings.TrimRight(line, "\r\n"), isEOF, nil
}

func (s *speechSSEState) consumeLine(
	line string,
	logger *slog.Logger,
	debugEnabled bool,
	consumer speechSSEConsumer,
) error {
	switch {
	case line == "":
		return s.flush(logger, debugEnabled, consumer)
	case strings.HasPrefix(line, "event:"):
		s.eventName = strings.TrimSpace(line[len("event:"):])
	case strings.HasPrefix(line, "data:"):
		s.appendData(strings.TrimSpace(line[len("data:"):]))
	case strings.HasPrefix(line, ":"):
		// comment, ignore
	default:
		s.appendData(line)
	}
	return nil
}

func (s *speechSSEState) appendData(line string) {
	if s.dataBuilder.Len() > 0 {
		s.dataBuilder.WriteByte('\n')
	}
	s.dataBuilder.WriteString(line)
}

func (s *speechSSEState) flush(
	logger *slog.Logger,
	debugEnabled bool,
	consumer speechSSEConsumer,
) error {
	if s.dataBuilder.Len() == 0 && strings.TrimSpace(s.eventName) == "" {
		return nil
	}

	payload := strings.TrimSpace(s.dataBuilder.String())
	eventName := strings.TrimSpace(s.eventName)
	s.dataBuilder.Reset()
	s.eventName = ""
	if payload == "" {
		return nil
	}

	done, err := handleSpeechSSEEvent(eventName, payload, s, logger, debugEnabled, consumer)
	if done {
		s.streamComplete = true
	}
	return err
}

func (s *speechSSEState) transcriptText() string {
	transcriptText := strings.TrimSpace(s.finalTranscript)
	if transcriptText == "" {
		transcriptText = strings.TrimSpace(s.transcript.String())
	}
	return transcriptText
}

func handleSpeechSSEEvent(
	eventName string,
	payload string,
	state *speechSSEState,
	logger *slog.Logger,
	debugEnabled bool,
	consumer speechSSEConsumer,
) (bool, error) {
	if payload == "[DONE]" {
		return true, nil
	}

	evt, eventType, err := decodeSpeechSSEEvent(eventName, payload)
	if err != nil {
		return false, err
	}
	if err := validateSpeechSSEError(evt); err != nil {
		return false, err
	}

	switch eventType {
	case "response.output_audio.delta":
		return false, handleSpeechAudioDelta(evt, state, consumer)
	case "response.output_audio.done":
		return false, nil
	case "response.output_audio_transcript.delta":
		return false, handleSpeechTranscriptDelta(evt, state, consumer)
	case "response.output_audio_transcript.done":
		return false, handleSpeechTranscriptDone(evt, state, consumer)
	case "response.completed":
		return handleSpeechCompletion(consumer)
	default:
		logIgnoredSpeechEvent(logger, debugEnabled, eventType)
		return false, nil
	}
}

func decodeSpeechSSEEvent(eventName, payload string) (speechSSEPayload, string, error) {
	var evt speechSSEPayload
	if err := json.Unmarshal([]byte(payload), &evt); err != nil {
		return speechSSEPayload{}, "", fmt.Errorf("decode speech stream payload: %w", err)
	}
	eventType := strings.TrimSpace(evt.Type)
	if eventType == "" {
		eventType = strings.TrimSpace(eventName)
	}
	return evt, eventType, nil
}

func validateSpeechSSEError(evt speechSSEPayload) error {
	if evt.Error == nil || strings.TrimSpace(evt.Error.Message) == "" {
		return nil
	}
	code := evt.Error.Code
	if code == "" {
		code = "unknown"
	}
	return fmt.Errorf("openai speech stream error (%s): %s", code, evt.Error.Message)
}

func handleSpeechAudioDelta(
	evt speechSSEPayload,
	state *speechSSEState,
	consumer speechSSEConsumer,
) error {
	if evt.Delta == "" {
		return nil
	}
	chunk, err := base64.StdEncoding.DecodeString(evt.Delta)
	if err != nil {
		return fmt.Errorf("decode audio delta: %w", err)
	}
	if consumer.CollectAudio {
		state.audioBuffer = append(state.audioBuffer, chunk...)
	}
	if consumer.OnAudioDelta != nil {
		return consumer.OnAudioDelta(chunk)
	}
	return nil
}

func handleSpeechTranscriptDelta(
	evt speechSSEPayload,
	state *speechSSEState,
	consumer speechSSEConsumer,
) error {
	if consumer.CollectTranscript {
		state.transcript.WriteString(evt.Delta)
	}
	if consumer.OnTranscriptDelta != nil && evt.Delta != "" {
		return consumer.OnTranscriptDelta(evt.Delta)
	}
	return nil
}

func handleSpeechTranscriptDone(
	evt speechSSEPayload,
	state *speechSSEState,
	consumer speechSSEConsumer,
) error {
	if evt.Transcript != "" {
		state.finalTranscript = evt.Transcript
	} else if consumer.CollectTranscript {
		state.finalTranscript = state.transcript.String()
	}
	if consumer.OnTranscriptDone == nil {
		return nil
	}
	return consumer.OnTranscriptDone(strings.TrimSpace(state.transcriptText()))
}

func handleSpeechCompletion(consumer speechSSEConsumer) (bool, error) {
	if consumer.OnCompleted == nil {
		return true, nil
	}
	return true, consumer.OnCompleted()
}

func logIgnoredSpeechEvent(logger *slog.Logger, debugEnabled bool, eventType string) {
	if eventType != "" && debugEnabled && logger != nil {
		logger.Debug("ignoring speech stream event", "type", eventType)
	}
}

type speechSSEPayload struct {
	Type       string `json:"type"`
	Delta      string `json:"delta"`
	Transcript string `json:"transcript"`
	Error      *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (e *OpenAITTS) buildAudioPayload(
	ctx context.Context,
	logger *slog.Logger,
	audio []byte,
	opts audioPayloadOptions,
) map[string]any {
	payload := map[string]any{
		"encoding":   opts.format,
		"sampleRate": opts.sampleRate,
		"channels":   opts.channels,
	}

	contentType := firstNonEmpty(opts.contentType, contentTypeForFormat(opts.format))
	writeOpts := opts.storeCtx.writeOptions(contentType)

	if sm, err := storage.SharedManager(ctx); err == nil && len(audio) > 0 {
		if ref, err := media.MaybeOffloadBlob(ctx, sm, audio, writeOpts); err != nil {
			logger.Warn("failed to offload tts audio", "error", err)
			payload["data"] = base64.StdEncoding.EncodeToString(audio)
		} else if ref != nil {
			payload["storage"] = ref
		} else {
			payload["data"] = base64.StdEncoding.EncodeToString(audio)
		}
	} else {
		payload["data"] = base64.StdEncoding.EncodeToString(audio)
	}

	return payload
}

func newOpenAIClient(secrets *sdkengram.Secrets) (*openai.Client, bool, string, error) {
	if secrets == nil {
		return nil, false, "", fmt.Errorf("secrets are required to initialize OpenAI client")
	}

	if endpoint := secretValue(secrets, "AZURE_ENDPOINT"); endpoint != "" {
		apiKey := secretValue(secrets, "AZURE_API_KEY")
		if apiKey == "" {
			return nil, false, "", fmt.Errorf("AZURE_API_KEY secret is required for Azure OpenAI")
		}
		apiVersion := secretValue(secrets, "AZURE_API_VERSION")
		if apiVersion == "" {
			apiVersion = defaultAzureAPIV
		}
		deployment := secretValue(secrets, "AZURE_DEPLOYMENT")
		if deployment == "" {
			return nil, false, "", fmt.Errorf("AZURE_DEPLOYMENT secret is required for Azure OpenAI")
		}
		httpClient := &http.Client{Timeout: 60 * time.Second}
		opts := []option.RequestOption{
			azure.WithEndpoint(endpoint, apiVersion),
			azure.WithAPIKey(apiKey),
			option.WithHTTPClient(httpClient),
		}
		client := openai.NewClient(opts...)
		return &client, true, deployment, nil
	}

	apiKey := secretValue(secrets, "OPENAI_API_KEY")
	if apiKey == "" {
		apiKey = secretValue(secrets, "API_KEY")
	}
	if apiKey == "" {
		return nil, false, "", fmt.Errorf("OPENAI_API_KEY secret is required")
	}
	httpClient := &http.Client{Timeout: 60 * time.Second}
	opts := []option.RequestOption{
		option.WithAPIKey(apiKey),
		option.WithHTTPClient(httpClient),
	}
	baseURL := secretValue(secrets, "OPENAI_BASE_URL")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	opts = append(opts, option.WithBaseURL(strings.TrimRight(baseURL, "/")))
	if org := secretValue(secrets, "OPENAI_ORG_ID"); org != "" {
		opts = append(opts, option.WithOrganization(org))
	}
	if project := secretValue(secrets, "OPENAI_PROJECT_ID"); project != "" {
		opts = append(opts, option.WithProject(project))
	}
	client := openai.NewClient(opts...)
	return &client, false, "", nil
}

func scopeFromMetadata(meta map[string]string) []string {
	if len(meta) == 0 {
		return nil
	}
	scopeKeys := []string{"room", "participant", "livekit-room", "livekit-participant"}
	scope := make([]string, 0, len(scopeKeys))
	for _, key := range scopeKeys {
		if val := strings.TrimSpace(meta[key]); val != "" {
			scope = append(scope, val)
		}
	}
	return scope
}

func scopeFromStoryInfo(info sdkengram.StoryInfo) []string {
	candidates := []string{info.StoryName, info.StepName, info.StepRunID}
	scope := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if val := strings.TrimSpace(candidate); val != "" {
			scope = append(scope, val)
		}
	}
	return scope
}

func cloneMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return make(map[string]string)
	}
	cloned := make(map[string]string, len(in))
	for k, v := range in {
		trimmed := strings.TrimSpace(k)
		if trimmed == "" {
			continue
		}
		cloned[trimmed] = v
	}
	return cloned
}

func contentTypeForFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "mp3":
		return "audio/mpeg"
	case "wav":
		return "audio/wav"
	case "opus":
		return "audio/ogg"
	case audioEncodingPCM:
		return "audio/L16"
	default:
		return "application/octet-stream"
	}
}

func secretValue(secrets *sdkengram.Secrets, key string) string {
	if secrets == nil {
		return ""
	}
	if val, ok := secrets.Get(key); ok {
		return strings.TrimSpace(val)
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return ""
}

func resolveSpeechFormat(
	requestFormat, configFormat string,
) (string, openai.AudioSpeechNewParamsResponseFormat, error) {
	desired := strings.TrimSpace(strings.ToLower(firstNonEmpty(requestFormat, configFormat, ttscfg.DefaultFormat)))
	switch desired {
	case "mp3":
		return desired, openai.AudioSpeechNewParamsResponseFormatMP3, nil
	case "opus":
		return desired, openai.AudioSpeechNewParamsResponseFormatOpus, nil
	case "aac":
		return desired, openai.AudioSpeechNewParamsResponseFormatAAC, nil
	case "flac":
		return desired, openai.AudioSpeechNewParamsResponseFormatFLAC, nil
	case "wav":
		return desired, openai.AudioSpeechNewParamsResponseFormatWAV, nil
	case audioEncodingPCM:
		fallthrough
	case "":
		return audioEncodingPCM, openai.AudioSpeechNewParamsResponseFormatPCM, nil
	default:
		return "", "", fmt.Errorf("unsupported audio response format %q", desired)
	}
}

func clampSpeed(v float64) float64 {
	if v < 0.25 {
		return 0.25
	}
	if v > 4.0 {
		return 4.0
	}
	return v
}

func resolveStreamFormat(value string) (*openai.AudioSpeechNewParamsStreamFormat, error) {
	if value == "" {
		return nil, nil
	}
	switch strings.ToLower(value) {
	case streamFormatAudio:
		streamFmt := openai.AudioSpeechNewParamsStreamFormatAudio
		return &streamFmt, nil
	case streamFormatSSE:
		streamFmt := openai.AudioSpeechNewParamsStreamFormatSSE
		return &streamFmt, nil
	default:
		return nil, fmt.Errorf("unsupported stream format %q", value)
	}
}

func supportsTTSInstructions(model string) bool {
	return strings.EqualFold(model, "gpt-4o-mini-tts")
}

func isTTSModel(model string) bool {
	model = strings.TrimSpace(strings.ToLower(model))
	if model == "" {
		return false
	}
	if strings.HasPrefix(model, "tts-") {
		return true
	}
	return strings.HasSuffix(model, "-tts")
}

func derefPositiveInt(value *int) int {
	if value == nil {
		return 0
	}
	if *value > 0 {
		return *value
	}
	return 0
}

func firstPositiveInt(values ...int) int {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}

func parseSampleRateFromContentType(contentType string) int {
	return parseIntParam(contentType, "rate", "samplerate")
}

func parseChannelsFromContentType(contentType string) int {
	return parseIntParam(contentType, "channels")
}

func parseIntParam(contentType string, keys ...string) int {
	if contentType == "" {
		return 0
	}
	parts := strings.Split(contentType, ";")
	if len(parts) <= 1 {
		return 0
	}
	for _, raw := range parts[1:] {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		kv := strings.SplitN(raw, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(kv[0]))
		for _, target := range keys {
			if key == target {
				if v, err := strconv.Atoi(strings.TrimSpace(kv[1])); err == nil && v > 0 {
					return v
				}
			}
		}
	}
	return 0
}

func resamplePCM16(data []byte, fromRate, toRate int) ([]byte, error) {
	if fromRate <= 0 || toRate <= 0 || len(data)%2 != 0 {
		return nil, fmt.Errorf("invalid resample parameters")
	}
	if fromRate == toRate {
		return data, nil
	}
	var ratio = float64(toRate) / float64(fromRate)
	if ratio <= 0 {
		return nil, fmt.Errorf("invalid resample ratio")
	}
	inSamples := len(data) / 2
	outSamples := int(float64(inSamples-1)*ratio) + 1
	out := make([]byte, outSamples*2)
	for i := 0; i < outSamples; i++ {
		srcPos := float64(i) / ratio
		srcIdx := int(srcPos)
		if srcIdx >= inSamples-1 {
			copy(out[i*2:], data[(inSamples-1)*2:])
			continue
		}
		frac := srcPos - float64(srcIdx)
		a := int16(binary.LittleEndian.Uint16(data[srcIdx*2:]))
		b := int16(binary.LittleEndian.Uint16(data[(srcIdx+1)*2:]))
		sample := int16(float64(a) + frac*(float64(b)-float64(a)))
		binary.LittleEndian.PutUint16(out[i*2:], uint16(sample))
	}
	return out, nil
}

func previewText(text string, limit int) string {
	text = strings.TrimSpace(text)
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit] + "…"
}

func sanitizeStreamFormat(value string) string {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case streamFormatAudio:
		return streamFormatAudio
	case streamFormatSSE:
		return streamFormatSSE
	default:
		return ""
	}
}
