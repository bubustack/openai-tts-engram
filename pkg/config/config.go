package config

import "strings"

const (
	DefaultModel        = "gpt-4o-mini-tts"
	DefaultVoice        = "alloy"
	DefaultFormat       = "pcm"
	DefaultStreamFormat = "sse"
	DefaultSampleRate   = 48000
	DefaultChannels     = 1
	DefaultSpeed        = 1.0
)

// Config captures static configuration injected via Engram.spec.with.
type Config struct {
	Model            string  `json:"model" mapstructure:"model"`
	Voice            string  `json:"voice" mapstructure:"voice"`
	Format           string  `json:"format" mapstructure:"format"`
	StreamFormat     string  `json:"streamFormat" mapstructure:"streamFormat"`
	SampleRate       int     `json:"sampleRate" mapstructure:"sampleRate"`
	TargetSampleRate int     `json:"targetSampleRate" mapstructure:"targetSampleRate"`
	Channels         int     `json:"channels" mapstructure:"channels"`
	Speed            float64 `json:"speed" mapstructure:"speed"`
	Instructions     string  `json:"instructions" mapstructure:"instructions"`
}

// Normalize applies defaults and bounds checking to the user supplied config.
func Normalize(cfg Config) Config {
	cfg.Model = firstNonEmpty(cfg.Model, DefaultModel)
	cfg.Voice = firstNonEmpty(cfg.Voice, DefaultVoice)
	cfg.Format = strings.TrimSpace(strings.ToLower(firstNonEmpty(cfg.Format, DefaultFormat)))
	cfg.StreamFormat = normalizeStreamFormat(cfg.StreamFormat)
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = DefaultSampleRate
	}
	if cfg.TargetSampleRate < 0 {
		cfg.TargetSampleRate = 0
	}
	if cfg.Channels <= 0 {
		cfg.Channels = DefaultChannels
	}
	if cfg.Speed <= 0 {
		cfg.Speed = DefaultSpeed
	} else {
		cfg.Speed = clampSpeed(cfg.Speed)
	}
	cfg.Instructions = strings.TrimSpace(cfg.Instructions)
	return cfg
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

func clampSpeed(v float64) float64 {
	if v < 0.25 {
		return 0.25
	}
	if v > 4.0 {
		return 4.0
	}
	return v
}

func normalizeStreamFormat(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	switch value {
	case "":
		return DefaultStreamFormat
	case "audio", "sse":
		return value
	default:
		return DefaultStreamFormat
	}
}
