package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	sdk "github.com/bubustack/bubu-sdk-go"
	tsengram "github.com/bubustack/openai-tts-engram/pkg/engram"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := sdk.Start(ctx, tsengram.New()); err != nil {
		log.Fatalf("openai-tts engram failed: %v", err)
	}
}
