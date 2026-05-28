package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const gatewayAuthHeader = "X-Aion-Gateway-Key"

// InferenceGatewayStatus is exposed through debug-status for operational
// observability without leaking upstream provider credentials.
type InferenceGatewayStatus struct {
	Enabled       bool   `json:"enabled"`
	ListenAddr    string `json:"listen_addr"`
	PublicBaseURL string `json:"public_base_url"`
	Capacity      int    `json:"capacity"`
	InUse         int    `json:"in_use"`
	Queued        int    `json:"queued"`
	TotalRequests int64  `json:"total_requests"`
	Rejected      int64  `json:"rejected"`
	Upstream429   int64  `json:"upstream_429"`
	LastError     string `json:"last_error,omitempty"`
}

type InferenceGateway struct {
	config   *Config
	server   *http.Server
	listener net.Listener
	limiter  *gatewayLimiter
	client   *http.Client

	totalRequests atomic.Int64
	rejected      atomic.Int64
	upstream429   atomic.Int64

	mu        sync.Mutex
	lastError string
}

func NewInferenceGateway(config *Config) *InferenceGateway {
	capacity := 1
	if config != nil && config.Execution.MaxConcurrentRequests > 0 {
		capacity = config.Execution.MaxConcurrentRequests
	}
	g := &InferenceGateway{
		config:  config,
		limiter: newGatewayLimiter(capacity),
		client: &http.Client{
			Timeout: 0,
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", g.handleHealth)
	mux.HandleFunc("/aion/gateway/status", g.handleStatus)
	mux.HandleFunc("/v1/models", g.handleModels)
	mux.HandleFunc("/v1/chat/completions", g.handleProxy)
	mux.HandleFunc("/v1/messages", g.handleProxy)
	g.server = &http.Server{Handler: mux}
	return g
}

func (g *InferenceGateway) Start() error {
	if g == nil || g.config == nil || !g.config.GatewayEnabled() {
		return nil
	}
	addr := strings.TrimSpace(g.config.InferenceGateway.ListenAddr)
	if addr == "" {
		addr = "127.0.0.1:50151"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		g.recordError(err)
		return fmt.Errorf("inference gateway listen: %w", err)
	}
	g.listener = ln
	log.Printf("inference-gateway: listening on %s", addr)
	go func() {
		if err := g.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			g.recordError(err)
			log.Printf("inference-gateway: server error: %v", err)
		}
	}()
	return nil
}

func (g *InferenceGateway) Shutdown(ctx context.Context) error {
	if g == nil || g.server == nil {
		return nil
	}
	return g.server.Shutdown(ctx)
}

func (g *InferenceGateway) Status() InferenceGatewayStatus {
	if g == nil || g.config == nil {
		return InferenceGatewayStatus{}
	}
	g.mu.Lock()
	lastError := g.lastError
	g.mu.Unlock()
	capacity, inUse, queued := 0, 0, 0
	if g.limiter != nil {
		capacity, inUse, queued = g.limiter.snapshot()
	}
	return InferenceGatewayStatus{
		Enabled:       g.config.GatewayEnabled(),
		ListenAddr:    g.config.InferenceGateway.ListenAddr,
		PublicBaseURL: g.config.InferenceGateway.PublicBaseURL,
		Capacity:      capacity,
		InUse:         inUse,
		Queued:        queued,
		TotalRequests: g.totalRequests.Load(),
		Rejected:      g.rejected.Load(),
		Upstream429:   g.upstream429.Load(),
		LastError:     lastError,
	}
}

func (g *InferenceGateway) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (g *InferenceGateway) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeGatewayJSON(w, http.StatusOK, g.Status())
}

func (g *InferenceGateway) handleModels(w http.ResponseWriter, r *http.Request) {
	if !g.authorized(r) {
		writeGatewayJSON(w, http.StatusUnauthorized, map[string]string{"error": "gateway unauthorized"})
		return
	}
	models := make([]map[string]any, 0, len(g.config.Inference.Models))
	for name, profile := range g.config.Inference.Models {
		id := strings.TrimSpace(profile.Model)
		if id == "" {
			id = name
		}
		models = append(models, map[string]any{
			"id":       id,
			"object":   "model",
			"owned_by": "aion-gateway",
		})
	}
	writeGatewayJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   models,
	})
}

func (g *InferenceGateway) handleProxy(w http.ResponseWriter, r *http.Request) {
	g.totalRequests.Add(1)
	if !g.authorized(r) {
		g.rejected.Add(1)
		writeGatewayJSON(w, http.StatusUnauthorized, map[string]string{"error": "gateway unauthorized"})
		return
	}
	release, err := g.acquire(r.Context())
	if err != nil {
		g.rejected.Add(1)
		g.recordError(err)
		writeGatewayJSON(w, http.StatusTooManyRequests, map[string]string{"error": err.Error()})
		return
	}
	defer release()

	profile, err := g.targetProfile(r)
	if err != nil {
		g.rejected.Add(1)
		g.recordError(err)
		writeGatewayJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	target, err := targetURLForRequest(profile, r.URL)
	if err != nil {
		g.rejected.Add(1)
		g.recordError(err)
		writeGatewayJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, r.Body)
	if err != nil {
		g.recordError(err)
		writeGatewayJSON(w, http.StatusInternalServerError, map[string]string{"error": "gateway request creation failed"})
		return
	}
	copyGatewayHeaders(req.Header, r.Header)
	applyUpstreamAuth(req.Header, profile)
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := g.client.Do(req)
	if err != nil {
		g.recordError(err)
		writeGatewayJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream inference request failed"})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		g.upstream429.Add(1)
	}
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	buf := make([]byte, 32*1024)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				g.recordError(readErr)
			}
			return
		}
	}
}

func (g *InferenceGateway) acquire(ctx context.Context) (func(), error) {
	timeout := 10 * time.Minute
	if g.config != nil && g.config.Execution.RequestQueueTimeoutSec > 0 {
		timeout = time.Duration(g.config.Execution.RequestQueueTimeoutSec) * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	release, err := g.limiter.acquire(waitCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("gateway queue timeout")
	}
	return func() {
		release()
		cancel()
	}, nil
}

func (g *InferenceGateway) authorized(r *http.Request) bool {
	key := strings.TrimSpace(g.config.InferenceGateway.GatewayKey)
	if key == "" {
		return true
	}
	return r.Header.Get(gatewayAuthHeader) == key
}

func (g *InferenceGateway) targetProfile(r *http.Request) (ModelProfile, error) {
	profileName := strings.TrimSpace(r.Header.Get("X-Aion-Target-Profile"))
	if profileName == "" {
		profileName = strings.TrimSpace(g.config.InferenceGateway.TargetProfile)
	}
	if profileName != "" {
		if profile, ok := g.config.Inference.Models[profileName]; ok {
			if profile.Endpoint == "" {
				profile.Endpoint = defaultProviderEndpoint(profile.Provider)
			}
			if profile.Endpoint == "" {
				return ModelProfile{}, fmt.Errorf("gateway target profile %q has no endpoint", profileName)
			}
			return profile, nil
		}
		return ModelProfile{}, fmt.Errorf("gateway target profile %q not found", profileName)
	}
	profile := ModelProfile{
		Provider: g.config.Inference.Fallback.Provider,
		Model:    g.config.Inference.Fallback.Model,
		Endpoint: g.config.Inference.Fallback.Endpoint,
	}
	if profile.Endpoint == "" {
		profile.Endpoint = defaultProviderEndpoint(profile.Provider)
	}
	if profile.Endpoint == "" {
		return ModelProfile{}, fmt.Errorf("gateway target profile is not configured")
	}
	return profile, nil
}

func (g *InferenceGateway) recordError(err error) {
	if err == nil {
		return
	}
	g.mu.Lock()
	g.lastError = err.Error()
	g.mu.Unlock()
}

func targetURLForRequest(profile ModelProfile, requestURL *url.URL) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(profile.Endpoint), "/")
	if base == "" {
		return "", fmt.Errorf("gateway target endpoint is empty")
	}
	path := requestURL.Path
	if strings.HasSuffix(base, "/v1") && strings.HasPrefix(path, "/v1/") {
		path = strings.TrimPrefix(path, "/v1")
	}
	target := base + path
	if requestURL.RawQuery != "" {
		target += "?" + requestURL.RawQuery
	}
	return target, nil
}

func copyGatewayHeaders(dst, src http.Header) {
	hopByHop := map[string]bool{
		"Connection":          true,
		"Keep-Alive":          true,
		"Proxy-Authenticate":  true,
		"Proxy-Authorization": true,
		"Te":                  true,
		"Trailer":             true,
		"Transfer-Encoding":   true,
		"Upgrade":             true,
		gatewayAuthHeader:     true,
	}
	for key, values := range src {
		if hopByHop[http.CanonicalHeaderKey(key)] || strings.HasPrefix(strings.ToLower(key), "x-aion-") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func applyUpstreamAuth(headers http.Header, profile ModelProfile) {
	keyName, keyValue := firstProfileCredential(profile.Env)
	if strings.TrimSpace(keyValue) == "" {
		return
	}
	provider := strings.ToLower(profile.Provider)
	if strings.Contains(provider, "anthropic") || strings.EqualFold(keyName, "ANTHROPIC_API_KEY") {
		headers.Set("x-api-key", keyValue)
		headers.Set("anthropic-version", "2023-06-01")
		return
	}
	headers.Set("Authorization", "Bearer "+keyValue)
}

func firstProfileCredential(env map[string]string) (string, string) {
	for key, value := range env {
		if strings.TrimSpace(value) != "" {
			return key, value
		}
	}
	return "", ""
}

func gatewayAPIForProvider(provider string) string {
	p := strings.ToLower(strings.TrimSpace(provider))
	if strings.Contains(p, "anthropic") || strings.Contains(p, "claude") {
		return "anthropic-messages"
	}
	return "openai-completions"
}

func defaultProviderEndpoint(provider string) string {
	p := strings.ToLower(strings.TrimSpace(provider))
	switch {
	case strings.Contains(p, "anthropic") || strings.Contains(p, "claude"):
		return "https://api.anthropic.com"
	case strings.Contains(p, "openai"):
		return "https://api.openai.com"
	case strings.Contains(p, "nvidia") || strings.Contains(p, "nim"):
		return "https://integrate.api.nvidia.com"
	default:
		return ""
	}
}

func writeGatewayJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

type gatewayLimiter struct {
	capacity int
	ch       chan struct{}
	mu       sync.Mutex
	queued   int
}

func newGatewayLimiter(capacity int) *gatewayLimiter {
	if capacity <= 0 {
		capacity = 1
	}
	return &gatewayLimiter{
		capacity: capacity,
		ch:       make(chan struct{}, capacity),
	}
}

func (l *gatewayLimiter) acquire(ctx context.Context) (func(), error) {
	l.mu.Lock()
	l.queued++
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.queued--
		l.mu.Unlock()
	}()
	select {
	case l.ch <- struct{}{}:
		return func() { <-l.ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *gatewayLimiter) snapshot() (capacity, inUse, queued int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.capacity, len(l.ch), l.queued
}
