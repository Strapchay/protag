package memory

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/philippgille/chromem-go"
)

// EmbedderConfig holds the configuration needed to initialize an embedding function.
type EmbedderConfig struct {
	Type    string // backend codename or test stub
	Model   string
	APIKey  string // Optional backend token
	BaseURL string // Optional backend endpoint
}

// NewEmbedder returns a chromem-go EmbeddingFunc based on the configuration.
func NewEmbedder(cfg EmbedderConfig) (chromem.EmbeddingFunc, error) {
	switch strings.ToLower(cfg.Type) {
	case "oracle":
		if cfg.APIKey == "" {
			return nil, fmt.Errorf("memory: oracle embedder requires API key")
		}

		model := cfg.Model
		if model == "" {
			model = string(chromem.EmbeddingModelOpenAI3Small)
		}

		return chromem.NewEmbeddingFuncOpenAI(
			cfg.APIKey,
			chromem.EmbeddingModelOpenAI(model),
		), nil

	case "harbor":
		baseURL := cfg.BaseURL
		if baseURL == "" {
			baseURL = os.Getenv("AION_MEMORY_BACKEND_ENDPOINT")
		}
		if baseURL == "" {
			return nil, fmt.Errorf("memory: harbor embedder requires backend endpoint")
		}

		model := cfg.Model
		if model == "" {
			model = "nomic-embed-text"
		}

		// chromem-go supports this backend natively
		return chromem.NewEmbeddingFuncOllama(
			model,
			baseURL,
		), nil

	case "mock":
		return NewMockEmbedder(), nil

	default:
		return nil, fmt.Errorf("memory: unsupported embedder type: %s", cfg.Type)
	}
}

// NewMockEmbedder creates an embedder that returns deterministic vectors for testing.
// It matches the chromem.EmbeddingFunc signature.
func NewMockEmbedder() chromem.EmbeddingFunc {
	return func(ctx context.Context, text string) ([]float32, error) {
		// Return a small 3-dimensional mock vector
		return []float32{1.0, 0.0, 0.0}, nil
	}
}
