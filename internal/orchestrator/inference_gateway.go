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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const gatewayAuthHeader = "X-Aion-Gateway-Key"
const gatewayExtensionLoadedPath = "/aion/gateway/extension-loaded"
const defaultGatewayActivityPulseInterval = 10 * time.Second

// InferenceGatewayStatus is exposed through debug-status for operational
// observability without leaking upstream provider credentials.
type InferenceGatewayStatus struct {
	Enabled       bool           `json:"enabled"`
	ListenAddr    string         `json:"listen_addr"`
	PublicBaseURL string         `json:"public_base_url"`
	Capacity      int            `json:"capacity"`
	InUse         int            `json:"in_use"`
	Queued        int            `json:"queued"`
	TotalRequests int64          `json:"total_requests"`
	Rejected      int64          `json:"rejected"`
	Upstream429   int64          `json:"upstream_429"`
	LastError     string         `json:"last_error,omitempty"`
	LogPath       string         `json:"log_path,omitempty"`
	StatusCounts  map[int]int64  `json:"status_counts,omitempty"`
	RecentEvents  []GatewayEvent `json:"recent_events,omitempty"`
}

type GatewayEvent struct {
	At         string `json:"at"`
	Level      string `json:"level"`
	RequestID  string `json:"request_id,omitempty"`
	AgentID    string `json:"agent_id,omitempty"`
	DomainID   string `json:"domain_id,omitempty"`
	Profile    string `json:"profile,omitempty"`
	Provider   string `json:"provider,omitempty"`
	Method     string `json:"method,omitempty"`
	Path       string `json:"path,omitempty"`
	Status     int    `json:"status,omitempty"`
	QueueMS    int64  `json:"queue_ms,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
	Bytes      int64  `json:"bytes,omitempty"`
	Message    string `json:"message"`
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

	mu           sync.Mutex
	lastError    string
	logPath      string
	statusCounts map[int]int64
	recentEvents []GatewayEvent
	activityFn   func(agentID, domainID, phase string)

	activityPulseInterval time.Duration
}

func NewInferenceGateway(config *Config, logsDir ...string) *InferenceGateway {
	capacity := 1
	if config != nil && config.Execution.MaxConcurrentRequests > 0 {
		capacity = config.Execution.MaxConcurrentRequests
	}
	logPath := ""
	if len(logsDir) > 0 && strings.TrimSpace(logsDir[0]) != "" {
		logPath = filepath.Join(logsDir[0], "inference_gateway_debug.log")
	}
	g := &InferenceGateway{
		config:                config,
		limiter:               newGatewayLimiter(capacity),
		logPath:               logPath,
		statusCounts:          make(map[int]int64),
		activityPulseInterval: defaultGatewayActivityPulseInterval,
		client: &http.Client{
			Timeout: 0,
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", g.handleHealth)
	mux.HandleFunc("/aion/gateway/status", g.handleStatus)
	mux.HandleFunc(gatewayExtensionLoadedPath, g.handleExtensionLoaded)
	mux.HandleFunc("/v1/models", g.handleModels)
	mux.HandleFunc("/v1/chat/completions", g.handleProxy)
	mux.HandleFunc("/v1/messages", g.handleProxy)
	mux.HandleFunc("/chat/completions", g.handleProxy)
	mux.HandleFunc("/messages", g.handleProxy)
	mux.HandleFunc("/", g.handleUnknown)
	g.server = &http.Server{Handler: mux}
	return g
}

func (g *InferenceGateway) SetActivityFunc(fn func(agentID, domainID, phase string)) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.activityFn = fn
	g.mu.Unlock()
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
	g.recordStartup(addr)
	go func() {
		if err := g.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			g.recordError(err)
			log.Printf("inference-gateway: server error: %v", err)
		}
	}()
	return nil
}

func (g *InferenceGateway) recordStartup(addr string) {
	g.recordEvent(GatewayEvent{
		At:      time.Now().UTC().Format(time.RFC3339Nano),
		Level:   "info",
		Method:  "START",
		Path:    addr,
		Message: fmt.Sprintf("gateway started capacity=%d public_base_url=%s", g.limiter.capacity, g.config.InferenceGateway.PublicBaseURL),
		Profile: strings.TrimSpace(g.config.InferenceGateway.TargetProfile),
	})
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
	statusCounts := make(map[int]int64, len(g.statusCounts))
	for status, count := range g.statusCounts {
		statusCounts[status] = count
	}
	recentEvents := append([]GatewayEvent(nil), g.recentEvents...)
	logPath := g.logPath
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
		LogPath:       logPath,
		StatusCounts:  statusCounts,
		RecentEvents:  recentEvents,
	}
}

func (g *InferenceGateway) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (g *InferenceGateway) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeGatewayJSON(w, http.StatusOK, g.Status())
}

func (g *InferenceGateway) handleExtensionLoaded(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	info := gatewayRequestInfo{
		RequestID: fmt.Sprintf("gw-ext-%d", time.Now().UnixNano()),
		AgentID:   strings.TrimSpace(r.Header.Get("X-Aion-Agent-ID")),
		DomainID:  strings.TrimSpace(r.Header.Get("X-Aion-Domain-ID")),
		Profile:   strings.TrimSpace(r.Header.Get("X-Aion-Target-Profile")),
		Provider:  strings.TrimSpace(r.Header.Get("X-Aion-Target-Provider")),
		Method:    r.Method,
		Path:      r.URL.Path,
	}
	if !g.authorized(r) {
		g.rejected.Add(1)
		g.recordFailure(info, http.StatusUnauthorized, start, "extension loaded signal unauthorized")
		writeGatewayJSON(w, http.StatusUnauthorized, map[string]string{"error": "gateway unauthorized"})
		return
	}
	g.recordEvent(GatewayEvent{
		At:         time.Now().UTC().Format(time.RFC3339Nano),
		Level:      "info",
		RequestID:  info.RequestID,
		AgentID:    info.AgentID,
		DomainID:   info.DomainID,
		Profile:    info.Profile,
		Provider:   info.Provider,
		Method:     info.Method,
		Path:       info.Path,
		Status:     http.StatusOK,
		DurationMS: time.Since(start).Milliseconds(),
		Message:    "pi gateway extension loaded",
	})
	writeGatewayJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (g *InferenceGateway) handleModels(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	info := gatewayRequestInfo{
		RequestID: fmt.Sprintf("gw-models-%d", time.Now().UnixNano()),
		AgentID:   strings.TrimSpace(r.Header.Get("X-Aion-Agent-ID")),
		DomainID:  strings.TrimSpace(r.Header.Get("X-Aion-Domain-ID")),
		Profile:   strings.TrimSpace(r.Header.Get("X-Aion-Target-Profile")),
		Provider:  strings.TrimSpace(r.Header.Get("X-Aion-Target-Provider")),
		Method:    r.Method,
		Path:      r.URL.Path,
	}
	if !g.authorized(r) {
		g.rejected.Add(1)
		g.recordFailure(info, http.StatusUnauthorized, start, "models request unauthorized")
		writeGatewayJSON(w, http.StatusUnauthorized, map[string]string{"error": "gateway unauthorized"})
		return
	}
	g.totalRequests.Add(1)
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
	g.recordStatus(http.StatusOK)
	g.recordEvent(GatewayEvent{
		At:         time.Now().UTC().Format(time.RFC3339Nano),
		Level:      "debug",
		RequestID:  info.RequestID,
		AgentID:    info.AgentID,
		DomainID:   info.DomainID,
		Profile:    info.Profile,
		Provider:   info.Provider,
		Method:     info.Method,
		Path:       info.Path,
		Status:     http.StatusOK,
		DurationMS: time.Since(start).Milliseconds(),
		Message:    "models request completed",
	})
}

func (g *InferenceGateway) handleUnknown(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	info := gatewayRequestInfo{
		RequestID: fmt.Sprintf("gw-unknown-%d", time.Now().UnixNano()),
		AgentID:   strings.TrimSpace(r.Header.Get("X-Aion-Agent-ID")),
		DomainID:  strings.TrimSpace(r.Header.Get("X-Aion-Domain-ID")),
		Profile:   strings.TrimSpace(r.Header.Get("X-Aion-Target-Profile")),
		Provider:  strings.TrimSpace(r.Header.Get("X-Aion-Target-Provider")),
		Method:    r.Method,
		Path:      r.URL.Path,
	}
	g.totalRequests.Add(1)
	g.recordFailure(info, http.StatusNotFound, start, "unknown gateway path")
	writeGatewayJSON(w, http.StatusNotFound, map[string]string{"error": "unknown gateway path"})
}

func (g *InferenceGateway) handleProxy(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := fmt.Sprintf("gw-%d", g.totalRequests.Add(1))
	reqInfo := gatewayRequestInfo{
		RequestID: requestID,
		AgentID:   strings.TrimSpace(r.Header.Get("X-Aion-Agent-ID")),
		DomainID:  strings.TrimSpace(r.Header.Get("X-Aion-Domain-ID")),
		Profile:   strings.TrimSpace(r.Header.Get("X-Aion-Target-Profile")),
		Provider:  strings.TrimSpace(r.Header.Get("X-Aion-Target-Provider")),
		Method:    r.Method,
		Path:      r.URL.Path,
	}
	g.recordEvent(GatewayEvent{
		At:        time.Now().UTC().Format(time.RFC3339Nano),
		Level:     "debug",
		RequestID: requestID,
		AgentID:   reqInfo.AgentID,
		DomainID:  reqInfo.DomainID,
		Profile:   reqInfo.Profile,
		Provider:  reqInfo.Provider,
		Method:    reqInfo.Method,
		Path:      reqInfo.Path,
		Message:   "request received",
	})
	if !g.authorized(r) {
		g.rejected.Add(1)
		g.recordFailure(reqInfo, http.StatusUnauthorized, start, "gateway unauthorized")
		writeGatewayJSON(w, http.StatusUnauthorized, map[string]string{"error": "gateway unauthorized"})
		return
	}
	stopPulse := g.startActivityPulse(r.Context(), reqInfo)
	defer stopPulse()
	queueStart := time.Now()
	release, err := g.acquire(r.Context())
	queueMS := time.Since(queueStart).Milliseconds()
	if err != nil {
		g.rejected.Add(1)
		g.recordError(err)
		g.recordFailure(reqInfo, http.StatusTooManyRequests, start, err.Error())
		writeGatewayJSON(w, http.StatusTooManyRequests, map[string]string{"error": err.Error()})
		return
	}
	defer release()
	reqInfo.QueueMS = queueMS
	g.emitActivity(reqInfo, "admitted")

	profile, err := g.targetProfile(r)
	if err != nil {
		g.rejected.Add(1)
		g.recordError(err)
		g.recordFailure(reqInfo, http.StatusBadGateway, start, err.Error())
		writeGatewayJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	reqInfo.Provider = profile.Provider
	if reqInfo.Profile == "" {
		reqInfo.Profile = g.config.InferenceGateway.TargetProfile
	}
	target, err := targetURLForRequest(profile, r.URL)
	if err != nil {
		g.rejected.Add(1)
		g.recordError(err)
		g.recordFailure(reqInfo, http.StatusBadGateway, start, err.Error())
		writeGatewayJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	if targetURL, parseErr := url.Parse(target); parseErr == nil {
		g.emitActivity(reqInfo, "forwarding")
		g.recordEvent(GatewayEvent{
			At:        time.Now().UTC().Format(time.RFC3339Nano),
			Level:     "debug",
			RequestID: requestID,
			AgentID:   reqInfo.AgentID,
			DomainID:  reqInfo.DomainID,
			Profile:   reqInfo.Profile,
			Provider:  reqInfo.Provider,
			Method:    reqInfo.Method,
			Path:      reqInfo.Path,
			QueueMS:   queueMS,
			Message:   "forwarding to upstream host " + targetURL.Host,
		})
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, r.Body)
	if err != nil {
		g.recordError(err)
		g.recordFailure(reqInfo, http.StatusInternalServerError, start, "gateway request creation failed")
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
		g.recordFailure(reqInfo, http.StatusBadGateway, start, "upstream inference request failed: "+err.Error())
		writeGatewayJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream inference request failed"})
		return
	}
	defer resp.Body.Close()
	g.recordStatus(resp.StatusCode)
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
	var bytesWritten int64
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				g.recordFailure(reqInfo, resp.StatusCode, start, "client write failed: "+writeErr.Error())
				return
			}
			bytesWritten += int64(n)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				g.recordError(readErr)
				g.recordFailure(reqInfo, resp.StatusCode, start, "upstream read failed: "+readErr.Error())
				return
			}
			g.emitActivity(reqInfo, "completed")
			g.recordCompletion(reqInfo, resp.StatusCode, start, bytesWritten)
			return
		}
	}
}

func (g *InferenceGateway) startActivityPulse(ctx context.Context, info gatewayRequestInfo) func() {
	if strings.TrimSpace(info.AgentID) == "" {
		return func() {}
	}
	done := make(chan struct{})
	interval := g.activityPulseInterval
	if interval <= 0 {
		interval = defaultGatewayActivityPulseInterval
	}
	g.emitActivity(info, "active")
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				g.emitActivity(info, "active")
			}
		}
	}()
	return func() {
		close(done)
	}
}

func (g *InferenceGateway) emitActivity(info gatewayRequestInfo, phase string) {
	g.mu.Lock()
	fn := g.activityFn
	g.mu.Unlock()
	if fn != nil {
		fn(info.AgentID, info.DomainID, phase)
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

type gatewayRequestInfo struct {
	RequestID string
	AgentID   string
	DomainID  string
	Profile   string
	Provider  string
	Method    string
	Path      string
	QueueMS   int64
}

func (g *InferenceGateway) recordCompletion(info gatewayRequestInfo, status int, start time.Time, bytesWritten int64) {
	level := "debug"
	message := "request completed"
	if status >= 500 {
		level = "error"
		message = "upstream server error"
	} else if status >= 400 {
		level = "warn"
		message = "upstream request rejected"
	}
	g.recordEvent(GatewayEvent{
		At:         time.Now().UTC().Format(time.RFC3339Nano),
		Level:      level,
		RequestID:  info.RequestID,
		AgentID:    info.AgentID,
		DomainID:   info.DomainID,
		Profile:    info.Profile,
		Provider:   info.Provider,
		Method:     info.Method,
		Path:       info.Path,
		Status:     status,
		QueueMS:    info.QueueMS,
		DurationMS: time.Since(start).Milliseconds(),
		Bytes:      bytesWritten,
		Message:    message,
	})
}

func (g *InferenceGateway) recordFailure(info gatewayRequestInfo, status int, start time.Time, message string) {
	g.recordStatus(status)
	g.recordEvent(GatewayEvent{
		At:         time.Now().UTC().Format(time.RFC3339Nano),
		Level:      "error",
		RequestID:  info.RequestID,
		AgentID:    info.AgentID,
		DomainID:   info.DomainID,
		Profile:    info.Profile,
		Provider:   info.Provider,
		Method:     info.Method,
		Path:       info.Path,
		Status:     status,
		QueueMS:    info.QueueMS,
		DurationMS: time.Since(start).Milliseconds(),
		Message:    sanitizeGatewayMessage(message),
	})
}

func (g *InferenceGateway) recordStatus(status int) {
	g.mu.Lock()
	if g.statusCounts == nil {
		g.statusCounts = make(map[int]int64)
	}
	g.statusCounts[status]++
	g.mu.Unlock()
}

func (g *InferenceGateway) recordEvent(event GatewayEvent) {
	if event.At == "" {
		event.At = time.Now().UTC().Format(time.RFC3339Nano)
	}
	event.Message = sanitizeGatewayMessage(event.Message)
	g.mu.Lock()
	if len(g.recentEvents) >= 50 {
		g.recentEvents = g.recentEvents[len(g.recentEvents)-49:]
	}
	g.recentEvents = append(g.recentEvents, event)
	logPath := g.logPath
	logLevel := "info"
	if g.config != nil {
		logLevel = strings.ToLower(strings.TrimSpace(g.config.Orchestrator.LogLevel))
	}
	g.mu.Unlock()

	if event.Level != "debug" || logLevel == "debug" || logLevel == "trace" {
		log.Printf("inference-gateway: %s request=%s agent=%s domain=%s profile=%s provider=%s method=%s path=%s status=%d queue_ms=%d duration_ms=%d bytes=%d msg=%s",
			event.Level,
			event.RequestID,
			event.AgentID,
			event.DomainID,
			event.Profile,
			event.Provider,
			event.Method,
			event.Path,
			event.Status,
			event.QueueMS,
			event.DurationMS,
			event.Bytes,
			event.Message,
		)
	}
	if logPath != "" {
		_ = os.MkdirAll(filepath.Dir(logPath), 0o755)
		if data, err := json.Marshal(event); err == nil {
			_ = appendTextFile(logPath, string(data)+"\n")
		}
	}
}

func sanitizeGatewayMessage(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return ""
	}
	replacements := []string{"Authorization", "x-api-key", "api_key", "apikey", "token", "secret", "password"}
	for _, replacement := range replacements {
		message = strings.ReplaceAll(message, replacement, "[redacted]")
		message = strings.ReplaceAll(message, strings.ToUpper(replacement), "[redacted]")
	}
	if len(message) > 500 {
		return message[:500] + "...(truncated)"
	}
	return message
}

func targetURLForRequest(profile ModelProfile, requestURL *url.URL) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(profile.Endpoint), "/")
	if base == "" {
		return "", fmt.Errorf("gateway target endpoint is empty")
	}
	path := canonicalGatewayProxyPath(requestURL.Path)
	if strings.HasSuffix(base, "/v1") && strings.HasPrefix(path, "/v1/") {
		path = strings.TrimPrefix(path, "/v1")
	}
	target := base + path
	if requestURL.RawQuery != "" {
		target += "?" + requestURL.RawQuery
	}
	return target, nil
}

func canonicalGatewayProxyPath(path string) string {
	switch path {
	case "/chat/completions":
		return "/v1/chat/completions"
	case "/messages":
		return "/v1/messages"
	default:
		return path
	}
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
