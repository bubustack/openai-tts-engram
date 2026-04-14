# 🔊 OpenAI Text-to-Speech Engram

Streaming Engram that converts text into PCM audio using OpenAI's realtime / text-to-speech APIs.

## 🌟 Highlights

- Supports both chunked audio streaming and final synthesis summaries.
- Keeps a simple runtime contract: text in, `speech.audio.*` events out.
- Uses the same OpenAI secret shape as the chat and speech-to-text engrams.
- Produces audio ready for downstream playback bridges such as LiveKit.

## 🚀 Quick Start

```bash
make lint
go test ./...
make docker-build
```

Apply `Engram.yaml`, mount an `openai` secret with `API_KEY`, and reference the
template from your Story step.

## ⚙️ Configuration (`Engram.spec.with`)

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `model` | string | OpenAI TTS model (e.g., `gpt-4o-mini-tts`). | `gpt-4o-mini-tts` |
| `voice` | string | Voice preset. | `alloy` |
| `format` | string | Output audio format (`pcm`, `opus`, `mp3`, `wav`, `flac`, `aac`). | `pcm` |
| `streamFormat` | string | OpenAI streaming mode (`audio` or `sse`). | `sse` |
| `sampleRate` | int | Output sample rate (Hz). | `48000` |
| `targetSampleRate` | int | Optional resampling target for PCM outputs (`0` keeps the native rate). | `0` |
| `channels` | int | Number of audio channels. | `1` |
| `speed` | number | Playback speed multiplier. | `1.0` |
| `instructions` | string | Optional style instructions for supported TTS models. | unset |

## 🔐 Secrets

Secret `openai` must map to a Kubernetes secret containing `API_KEY`, with optional `BASE_URL`, `ORG_ID`, and `PROJECT_ID` keys.

## 📥 Inputs

```json
{
  "text": "Hello world",
  "voice": "alloy",
  "model": "gpt-4o-mini-tts",
  "format": "pcm",
  "speed": 1.1,
  "instructions": "Warm, concise, and conversational."
}
```

Per-request overrides also support `streamFormat`, `sampleRate`, `channels`,
`speed`, and `instructions`.

## 📤 Outputs

```json
{
  "type": "speech.audio.v1",
  "audio": {
    "encoding": "pcm",
    "sampleRate": 48000,
    "channels": 1,
    "data": "<base64-encoded audio>"
  }
}
```

## 🔄 Streaming Mode

In streaming mode the engram emits:

| Stream type | Description |
|-------------|-------------|
| `speech.audio.delta` | Base64/PCM chunks suitable for immediate playback. |
| `speech.audio.done.v1` | Final summary containing stream metadata. The current template/test surface still uses this event name even though Tractatus canonicalizes the shared summary type as `speech.audio.done`. |

The generated audio can be passed directly to a playback bridge such as `livekit-bridge`.

## 🧪 Local Development

- `make lint` – Run the shared lint and static-analysis checks.
- `go test ./...` – Run the synthesis unit/integration tests.
- `make docker-build` – Build the engram image for local clusters.

## 🤝 Community & Support

- [Contributing](./CONTRIBUTING.md)
- [Support](./SUPPORT.md)
- [Security Policy](./SECURITY.md)
- [Code of Conduct](./CODE_OF_CONDUCT.md)
- [Discord](https://discord.gg/dysrB7D8H6)


## 📄 License

Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
