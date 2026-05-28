package orchestrator

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInferenceGatewayProxiesOpenAICompatibleRequest(t *testing.T) {
	gateway := NewInferenceGateway(&Config{
		Execution: ExecutionConfig{
			Mode:                   "gateway",
			MaxConcurrentRequests:  1,
			RequestQueueTimeoutSec: 1,
		},
		InferenceGateway: InferenceGatewayConfig{
			Enabled:       true,
			ListenAddr:    "127.0.0.1:0",
			PublicBaseURL: "http://127.0.0.1:0",
			GatewayKey:    "local-key",
			TargetProfile: "forge",
		},
		Inference: InferenceConfig{
			Models: map[string]ModelProfile{
				"forge": {
					Provider: "redacted-openai-compatible",
					Model:    "redacted-model",
					Endpoint: "http://upstream.local",
					Env:      map[string]string{"API_KEY": "redacted-token"},
				},
			},
		},
	})
	gateway.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("unexpected upstream path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer redacted-token" {
			t.Fatalf("unexpected upstream auth header: %q", got)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"ok"}}]}`)),
		}, nil
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	req.Header.Set(gatewayAuthHeader, "local-key")
	req.Header.Set("X-Aion-Target-Profile", "forge")
	rec := httptest.NewRecorder()

	gateway.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"ok"`) {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
	if status := gateway.Status(); status.TotalRequests != 1 || status.InUse != 0 {
		t.Fatalf("unexpected status: %#v", status)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestInferenceGatewayRejectsInvalidLocalKey(t *testing.T) {
	gateway := NewInferenceGateway(&Config{
		Execution: ExecutionConfig{Mode: "gateway", MaxConcurrentRequests: 1, RequestQueueTimeoutSec: 1},
		InferenceGateway: InferenceGatewayConfig{
			Enabled:    true,
			GatewayKey: "local-key",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()

	gateway.handleProxy(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestInferenceGatewayModelsEndpointUsesConfiguredProfiles(t *testing.T) {
	gateway := NewInferenceGateway(&Config{
		Inference: InferenceConfig{
			Models: map[string]ModelProfile{
				"forge": {Model: "redacted-model-forge"},
			},
		},
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)

	gateway.handleModels(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode models response: %v", err)
	}
	if len(payload.Data) != 1 || payload.Data[0].ID != "redacted-model-forge" {
		t.Fatalf("unexpected models payload: %#v", payload)
	}
}
