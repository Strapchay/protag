package orchestrator

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGatewayLimiterCanGrowWithoutReorderingWaiters(t *testing.T) {
	limiter := newGatewayLimiter(1)
	releaseFirst, err := limiter.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	acquired := make(chan func(), 1)
	go func() {
		release, acquireErr := limiter.acquire(context.Background())
		if acquireErr == nil {
			acquired <- release
		}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		_, _, queued := limiter.snapshot()
		if queued == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second request did not enter queue")
		}
		time.Sleep(time.Millisecond)
	}
	limiter.setCapacity(2)
	select {
	case releaseSecond := <-acquired:
		releaseSecond()
	case <-time.After(time.Second):
		t.Fatal("capacity increase did not admit queued request")
	}
	releaseFirst()
	capacity, inUse, queued := limiter.snapshot()
	if capacity != 2 || inUse != 0 || queued != 0 {
		t.Fatalf("unexpected limiter snapshot: capacity=%d in_use=%d queued=%d", capacity, inUse, queued)
	}
}

func TestInferenceGatewayRetriesBeforeReturningResponse(t *testing.T) {
	gateway := NewInferenceGateway(&Config{
		Execution: ExecutionConfig{Mode: "gateway", MaxConcurrentRequests: 1, RequestQueueTimeoutSec: 1},
		InferenceGateway: InferenceGatewayConfig{
			Enabled:          true,
			TargetProfile:    "forge",
			MaxRetries:       2,
			RetryBaseDelayMS: 1,
			RetryMaxDelayMS:  2,
		},
		Inference: InferenceConfig{Models: map[string]ModelProfile{
			"forge": {Provider: "redacted-provider", Endpoint: "http://upstream.local"},
		}},
	})
	gateway.sleepFn = func(context.Context, time.Duration) error { return nil }
	attempts := 0
	gateway.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != `{"stream":true}` {
			t.Fatalf("attempt %d body=%q err=%v", attempts, string(body), err)
		}
		status := http.StatusServiceUnavailable
		payload := `{"error":"busy"}`
		if attempts == 2 {
			status = http.StatusOK
			payload = `{"ok":true}`
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(payload))}, nil
	})

	req := httptest.NewRequest(http.MethodPost, "/chat/completions", strings.NewReader(`{"stream":true}`))
	req.Header.Set("X-Aion-Target-Profile", "forge")
	rec := httptest.NewRecorder()
	gateway.handleProxy(rec, req)

	if rec.Code != http.StatusOK || attempts != 2 {
		t.Fatalf("status=%d attempts=%d body=%s", rec.Code, attempts, rec.Body.String())
	}
	status := gateway.Status()
	if status.RetryAttempts != 1 || status.Retried != 1 {
		t.Fatalf("unexpected retry status: %#v", status)
	}
}

func TestInferenceGatewayUpstreamTimeoutReleasesCapacity(t *testing.T) {
	gateway := NewInferenceGateway(&Config{
		Execution: ExecutionConfig{Mode: "gateway", MaxConcurrentRequests: 1, RequestQueueTimeoutSec: 5},
		InferenceGateway: InferenceGatewayConfig{
			Enabled:            true,
			TargetProfile:      "forge",
			MaxRetries:         2,
			RetryBaseDelayMS:   1,
			RetryMaxDelayMS:    2,
			UpstreamTimeoutSec: 1,
		},
		Inference: InferenceConfig{Models: map[string]ModelProfile{
			"forge": {Provider: "redacted-provider", Endpoint: "http://upstream.local"},
		}},
	})
	gateway.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})

	req := httptest.NewRequest(http.MethodPost, "/chat/completions", strings.NewReader(`{"stream":true}`))
	req.Header.Set("X-Aion-Target-Profile", "forge")
	rec := httptest.NewRecorder()
	gateway.handleProxy(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	status := gateway.Status()
	if status.InUse != 0 || status.Queued != 0 {
		t.Fatalf("timeout did not release gateway capacity: %#v", status)
	}
	if status.StatusCounts[http.StatusGatewayTimeout] != 1 {
		t.Fatalf("expected one recorded 504, got %#v", status.StatusCounts)
	}
}

func TestInferenceGatewaySetCapacityValidatesRange(t *testing.T) {
	gateway := NewInferenceGateway(&Config{Execution: ExecutionConfig{MaxConcurrentRequests: 1}})
	status, err := gateway.SetCapacity(3)
	if err != nil || status.Capacity != 3 {
		t.Fatalf("SetCapacity(3) status=%#v err=%v", status, err)
	}
	if _, err := gateway.SetCapacity(0); err == nil {
		t.Fatal("expected invalid capacity error")
	}
}

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

func TestInferenceGatewayUnixTransportUsesCapabilityPolicy(t *testing.T) {
	registry := newAgentCapabilityRegistry()
	token, err := registry.issuePolicy(AgentCapabilityPolicy{
		AgentID:    "agent-data",
		DomainID:   "data",
		Profile:    "forge",
		Provider:   "redacted-openai-compatible",
		Model:      "permitted-model",
		Generation: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	gateway := NewInferenceGateway(&Config{
		Execution:        ExecutionConfig{Mode: "gateway", MaxConcurrentRequests: 1, RequestQueueTimeoutSec: 1},
		InferenceGateway: InferenceGatewayConfig{Enabled: true, GatewayKey: "tcp-only-key"},
		Inference: InferenceConfig{Models: map[string]ModelProfile{
			"forge": {
				Provider: "redacted-openai-compatible",
				Model:    "permitted-model",
				Endpoint: "http://upstream.local",
				Env:      map[string]string{"RESOURCE_CREDENTIAL": "test-value"},
			},
		}},
	})
	gateway.SetCapabilityResolver(registry.resolvePolicy)
	gateway.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode upstream body: %v", err)
		}
		if payload["model"] != "permitted-model" {
			t.Fatalf("capability model was not enforced: %#v", payload)
		}
		if got := r.Header.Get("X-Aion-Agent-ID"); got != "" {
			t.Fatalf("agent-controlled header leaked upstream: %q", got)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader("data: [DONE]\n\n")),
		}, nil
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"spoofed-model","stream":true}`))
	req.Header.Set(gatewayAgentCapabilityHeader, token)
	req.Header.Set("X-Aion-Agent-ID", "agent-spoofed")
	req.Header.Set("X-Aion-Target-Profile", "other-profile")
	req = req.WithContext(context.WithValue(req.Context(), gatewayTransportKey{}, gatewayTransportUnix))
	rec := httptest.NewRecorder()
	gateway.handleProxy(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	status := gateway.Status()
	found := false
	for _, event := range status.RecentEvents {
		if event.RequestID == "gw-1" && event.AgentID == "agent-data" && event.DomainID == "data" {
			found = true
		}
	}
	if !found {
		t.Fatalf("gateway events did not use capability identity: %#v", status.RecentEvents)
	}
}

func TestInferenceGatewayUnixTransportRejectsRotatedCapability(t *testing.T) {
	registry := newAgentCapabilityRegistry()
	stale, err := registry.issuePolicy(AgentCapabilityPolicy{AgentID: "agent-data", Profile: "forge", Model: "model-a", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.issuePolicy(AgentCapabilityPolicy{AgentID: "agent-data", Profile: "forge", Model: "model-a", Generation: 2}); err != nil {
		t.Fatal(err)
	}
	gateway := NewInferenceGateway(&Config{
		Execution:        ExecutionConfig{Mode: "gateway", MaxConcurrentRequests: 1},
		InferenceGateway: InferenceGatewayConfig{Enabled: true},
	})
	gateway.SetCapabilityResolver(registry.resolvePolicy)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	req.Header.Set(gatewayAgentCapabilityHeader, stale)
	req = req.WithContext(context.WithValue(req.Context(), gatewayTransportKey{}, gatewayTransportUnix))
	rec := httptest.NewRecorder()
	gateway.handleProxy(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("rotated capability status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestInferenceGatewayUnixTransportRejectsWrongProtocolPath(t *testing.T) {
	registry := newAgentCapabilityRegistry()
	token, err := registry.issuePolicy(AgentCapabilityPolicy{
		AgentID:  "agent-data",
		Profile:  "forge",
		Provider: "openai-compatible",
		Model:    "model-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	gateway := NewInferenceGateway(&Config{
		Execution:        ExecutionConfig{Mode: "gateway", MaxConcurrentRequests: 1},
		InferenceGateway: InferenceGatewayConfig{Enabled: true},
	})
	gateway.SetCapabilityResolver(registry.resolvePolicy)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"model-a"}`))
	req.Header.Set(gatewayAgentCapabilityHeader, token)
	req = req.WithContext(context.WithValue(req.Context(), gatewayTransportKey{}, gatewayTransportUnix))
	rec := httptest.NewRecorder()
	gateway.handleProxy(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("wrong protocol status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestInferenceGatewayStartsProtectedUnixListener(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "inference.sock")
	registry := newAgentCapabilityRegistry()
	token, err := registry.issuePolicy(AgentCapabilityPolicy{AgentID: "agent-data", DomainID: "data", Profile: "forge", Model: "model-a"})
	if err != nil {
		t.Fatal(err)
	}
	gateway := NewInferenceGateway(&Config{
		Execution:        ExecutionConfig{Mode: "gateway", MaxConcurrentRequests: 1},
		InferenceGateway: InferenceGatewayConfig{Enabled: true, ListenAddr: "127.0.0.1:0"},
	})
	gateway.SetUnixSocket(socketPath)
	gateway.SetCapabilityResolver(registry.resolvePolicy)
	if err := gateway.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = gateway.Shutdown(ctx)
	})
	info, err := os.Stat(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %o", info.Mode().Perm())
	}
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}}}
	req, err := http.NewRequest(http.MethodPost, "http://unix/aion/gateway/extension-loaded", strings.NewReader(`{"transport":"unix"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(gatewayAgentCapabilityHeader, token)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
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
	if len(gateway.Status().Active) != 0 {
		t.Fatalf("expected active request registry to be empty after completion, got %#v", gateway.Status().Active)
	}
}

func TestInferenceGatewayStatusTracksActiveRequest(t *testing.T) {
	releaseUpstream := make(chan struct{})
	gateway := NewInferenceGateway(&Config{
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
		<-releaseUpstream
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"ok"}}]}`)),
		}, nil
	})

	req := httptest.NewRequest(http.MethodPost, "/chat/completions", strings.NewReader(`{"stream":true}`))
	req.Header.Set(gatewayAuthHeader, "local-key")
	req.Header.Set("X-Aion-Target-Profile", "forge")
	req.Header.Set("X-Aion-Agent-ID", "agent-sql")
	req.Header.Set("X-Aion-Domain-ID", "sql")
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		gateway.handleProxy(rec, req)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for {
		status := gateway.Status()
		if len(status.Active) == 1 && status.Active[0].AgentID == "agent-sql" && status.Active[0].Provider == "redacted-openai-compatible" && status.Active[0].MaxAttempts == 1 {
			break
		}
		select {
		case <-deadline:
			close(releaseUpstream)
			t.Fatalf("timed out waiting for active gateway request, status=%#v", status)
		case <-time.After(10 * time.Millisecond):
		}
	}
	close(releaseUpstream)
	<-done
	if len(gateway.Status().Active) != 0 {
		t.Fatalf("expected active registry to clear after completion, got %#v", gateway.Status().Active)
	}
}

func TestInferenceGatewayRetryAfterIsBounded(t *testing.T) {
	gateway := NewInferenceGateway(&Config{InferenceGateway: InferenceGatewayConfig{
		RetryBaseDelayMS: 10,
		RetryMaxDelayMS:  250,
	}})
	resp := &http.Response{Header: http.Header{"Retry-After": []string{"30"}}}
	if got := gateway.retryDelay(resp, 1); got != 250*time.Millisecond {
		t.Fatalf("retry delay=%s want=250ms", got)
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
