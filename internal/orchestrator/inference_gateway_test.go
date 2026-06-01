package orchestrator

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInferenceGatewayProxiesOpenAICompatibleRequest(t *testing.T) {
	logDir := t.TempDir()
	gateway := NewInferenceGateway(&Config{
		Orchestrator: OrchestratorConfig{LogLevel: "debug"},
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
	}, logDir)
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
	} else if status.StatusCounts[http.StatusOK] != 1 {
		t.Fatalf("expected 200 status count, got %#v", status.StatusCounts)
	} else if len(status.RecentEvents) == 0 {
		t.Fatalf("expected recent gateway events")
	} else if status.LogPath == "" {
		t.Fatalf("expected gateway log path")
	}
}

func TestInferenceGatewayProxiesPiOpenAIPathAlias(t *testing.T) {
	gateway := NewInferenceGateway(&Config{
		Orchestrator: OrchestratorConfig{LogLevel: "debug"},
		Execution: ExecutionConfig{
			Mode:                   "gateway",
			MaxConcurrentRequests:  1,
			RequestQueueTimeoutSec: 1,
		},
		InferenceGateway: InferenceGatewayConfig{
			Enabled:       true,
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
		if r.URL.String() != "http://upstream.local/v1/chat/completions" {
			t.Fatalf("unexpected upstream URL: %s", r.URL.String())
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"ok"}}]}`)),
		}, nil
	})

	req := httptest.NewRequest(http.MethodPost, "/chat/completions", strings.NewReader(`{"stream":true}`))
	req.Header.Set(gatewayAuthHeader, "local-key")
	req.Header.Set("X-Aion-Target-Profile", "forge")
	rec := httptest.NewRecorder()

	gateway.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if status := gateway.Status(); status.StatusCounts[http.StatusOK] != 1 {
		t.Fatalf("expected 200 status count, got %#v", status.StatusCounts)
	}
}

func TestInferenceGatewayEmitsActivityPulseDuringLongRequest(t *testing.T) {
	gateway := NewInferenceGateway(&Config{
		Orchestrator: OrchestratorConfig{LogLevel: "debug"},
		Execution: ExecutionConfig{
			Mode:                   "gateway",
			MaxConcurrentRequests:  1,
			RequestQueueTimeoutSec: 1,
		},
		InferenceGateway: InferenceGatewayConfig{
			Enabled:       true,
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
	gateway.activityPulseInterval = 5 * time.Millisecond
	activity := make(chan string, 16)
	gateway.SetActivityFunc(func(agentID, domainID, phase string) {
		if agentID == "agent-sql" && domainID == "sql" {
			activity <- phase
		}
	})
	gateway.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seenInitialActive := false
		deadline := time.After(200 * time.Millisecond)
		for {
			select {
			case phase := <-activity:
				if phase == "active" {
					if seenInitialActive {
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     http.Header{"Content-Type": []string{"application/json"}},
							Body:       io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"ok"}}]}`)),
						}, nil
					}
					seenInitialActive = true
				}
			case <-deadline:
				t.Fatalf("timed out waiting for gateway activity pulse")
			}
		}
	})

	req := httptest.NewRequest(http.MethodPost, "/chat/completions", strings.NewReader(`{"stream":true}`))
	req.Header.Set(gatewayAuthHeader, "local-key")
	req.Header.Set("X-Aion-Target-Profile", "forge")
	req.Header.Set("X-Aion-Agent-ID", "agent-sql")
	req.Header.Set("X-Aion-Domain-ID", "sql")
	rec := httptest.NewRecorder()

	gateway.handleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestInferenceGatewayStartupCreatesDebugLog(t *testing.T) {
	logDir := t.TempDir()
	gateway := NewInferenceGateway(&Config{
		Orchestrator: OrchestratorConfig{LogLevel: "debug"},
		Execution:    ExecutionConfig{Mode: "gateway", MaxConcurrentRequests: 1},
		InferenceGateway: InferenceGatewayConfig{
			Enabled:    true,
			ListenAddr: "127.0.0.1:0",
		},
	}, logDir)
	gateway.recordStartup("127.0.0.1:0")

	logPath := filepath.Join(logDir, "inference_gateway_debug.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read gateway log: %v", err)
	}
	if !strings.Contains(string(data), "gateway started") {
		t.Fatalf("gateway log missing startup event:\n%s", string(data))
	}
}

func TestInferenceGatewayRecordsExtensionLoaded(t *testing.T) {
	logDir := t.TempDir()
	gateway := NewInferenceGateway(&Config{
		Execution: ExecutionConfig{Mode: "gateway", MaxConcurrentRequests: 1},
		InferenceGateway: InferenceGatewayConfig{
			Enabled:    true,
			GatewayKey: "local-key",
		},
	}, logDir)
	req := httptest.NewRequest(http.MethodPost, gatewayExtensionLoadedPath, strings.NewReader(`{}`))
	req.Header.Set(gatewayAuthHeader, "local-key")
	req.Header.Set("X-Aion-Agent-ID", "agent-storage")
	req.Header.Set("X-Aion-Domain-ID", "storage")
	req.Header.Set("X-Aion-Target-Provider", "redacted-provider")
	rec := httptest.NewRecorder()

	gateway.handleExtensionLoaded(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	status := gateway.Status()
	if len(status.RecentEvents) == 0 {
		t.Fatalf("expected recent events")
	}
	last := status.RecentEvents[len(status.RecentEvents)-1]
	if last.AgentID != "agent-storage" || last.Message != "pi gateway extension loaded" {
		t.Fatalf("unexpected extension event: %#v", last)
	}
}

func TestInferenceGatewayRecordsUnknownPath(t *testing.T) {
	gateway := NewInferenceGateway(&Config{
		Execution:        ExecutionConfig{Mode: "gateway", MaxConcurrentRequests: 1},
		InferenceGateway: InferenceGatewayConfig{Enabled: true},
	})
	req := httptest.NewRequest(http.MethodPost, "/unexpected/path", strings.NewReader(`{}`))
	req.Header.Set("X-Aion-Agent-ID", "agent-storage")
	rec := httptest.NewRecorder()

	gateway.handleUnknown(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	status := gateway.Status()
	if status.TotalRequests != 1 || status.StatusCounts[http.StatusNotFound] != 1 {
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
	if status := gateway.Status(); status.Rejected != 1 || status.StatusCounts[http.StatusUnauthorized] != 1 {
		t.Fatalf("unexpected status after rejection: %#v", status)
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
