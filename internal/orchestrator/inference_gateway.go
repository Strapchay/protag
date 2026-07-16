package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const gatewayAuthHeader = "X-Aion-Gateway-Key"
const gatewayExtensionLoadedPath = "/aion/gateway/extension-loaded"
const defaultGatewayActivityPulseInterval = 10 * time.Second
const maxGatewayCapacity = 16

// InferenceGatewayStatus is exposed through debug-status for operational
// observability without leaking upstream provider credentials.
type InferenceGatewayStatus struct {
	Enabled            bool                   `json:"enabled"`
	ListenAddr         string                 `json:"listen_addr"`
	PublicBaseURL      string                 `json:"public_base_url"`
	Capacity           int                    `json:"capacity"`
	InUse              int                    `json:"in_use"`
	Queued             int                    `json:"queued"`
	Active             []GatewayActiveRequest `json:"active,omitempty"`
	TotalRequests      int64                  `json:"total_requests"`
	Rejected           int64                  `json:"rejected"`
	Upstream429        int64                  `json:"upstream_429"`
	RetryAttempts      int64                  `json:"retry_attempts"`
	Retried            int64                  `json:"retried_requests"`
	UpstreamTimeoutSec int                    `json:"upstream_timeout_sec"`
	LastError          string                 `json:"last_error,omitempty"`
	LogPath            string                 `json:"log_path,omitempty"`
	StatusCounts       map[int]int64          `json:"status_counts,omitempty"`
	RecentEvents       []GatewayEvent         `json:"recent_events,omitempty"`
}

type GatewayActiveRequest struct {
	RequestID   string `json:"request_id"`
	AgentID     string `json:"agent_id,omitempty"`
	DomainID    string `json:"domain_id,omitempty"`
	Profile     string `json:"profile,omitempty"`
	Provider    string `json:"provider,omitempty"`
	Phase       string `json:"phase"`
	StartedAt   string `json:"started_at"`
	UpdatedAt   string `json:"updated_at"`
	AgeMS       int64  `json:"age_ms"`
	QueueMS     int64  `json:"queue_ms,omitempty"`
	Attempt     int    `json:"attempt,omitempty"`
	MaxAttempts int    `json:"max_attempts,omitempty"`
}

type GatewayEvent struct {
	At          string `json:"at"`
	Level       string `json:"level"`
	RequestID   string `json:"request_id,omitempty"`
	AgentID     string `json:"agent_id,omitempty"`
	DomainID    string `json:"domain_id,omitempty"`
	Profile     string `json:"profile,omitempty"`
	Provider    string `json:"provider,omitempty"`
	Method      string `json:"method,omitempty"`
	Path        string `json:"path,omitempty"`
	Status      int    `json:"status,omitempty"`
	QueueMS     int64  `json:"queue_ms,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"`
	Bytes       int64  `json:"bytes,omitempty"`
	Attempt     int    `json:"attempt,omitempty"`
	MaxAttempts int    `json:"max_attempts,omitempty"`
	Message     string `json:"message"`
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
	retryAttempts atomic.Int64
	retried       atomic.Int64

	mu           sync.Mutex
	lastError    string
	logPath      string
	statusCounts map[int]int64
	recentEvents []GatewayEvent
	activityFn   func(agentID, domainID, phase string)
	active       map[string]gatewayActiveRequest

	activityPulseInterval time.Duration
	sleepFn               func(context.Context, time.Duration) error
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
		active:                make(map[string]gatewayActiveRequest),
		activityPulseInterval: defaultGatewayActivityPulseInterval,
		sleepFn:               sleepWithContext,
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
	capacity, _, _ := g.limiter.snapshot()
	g.recordEvent(GatewayEvent{
		At:      time.Now().UTC().Format(time.RFC3339Nano),
		Level:   "info",
		Method:  "START",
		Path:    addr,
		Message: fmt.Sprintf("gateway started capacity=%d public_base_url=%s", capacity, g.config.InferenceGateway.PublicBaseURL),
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
	active := make([]GatewayActiveRequest, 0, len(g.active))
	now := time.Now()
	for _, item := range g.active {
		active = append(active, GatewayActiveRequest{
			RequestID:   item.info.RequestID,
			AgentID:     item.info.AgentID,
			DomainID:    item.info.DomainID,
			Profile:     item.info.Profile,
			Provider:    item.info.Provider,
			Phase:       item.phase,
			StartedAt:   item.startedAt.UTC().Format(time.RFC3339Nano),
			UpdatedAt:   item.updatedAt.UTC().Format(time.RFC3339Nano),
			AgeMS:       now.Sub(item.startedAt).Milliseconds(),
			QueueMS:     item.info.QueueMS,
			Attempt:     item.info.Attempt,
			MaxAttempts: item.info.MaxAttempts,
		})
	}
	sort.Slice(active, func(i, j int) bool {
		if active[i].StartedAt == active[j].StartedAt {
			return active[i].RequestID < active[j].RequestID
		}
		return active[i].StartedAt < active[j].StartedAt
	})
	logPath := g.logPath
	g.mu.Unlock()
	capacity, inUse, queued := 0, 0, 0
	if g.limiter != nil {
		capacity, inUse, queued = g.limiter.snapshot()
	}
	return InferenceGatewayStatus{
		Enabled:            g.config.GatewayEnabled(),
		ListenAddr:         g.config.InferenceGateway.ListenAddr,
		PublicBaseURL:      g.config.InferenceGateway.PublicBaseURL,
		Capacity:           capacity,
		InUse:              inUse,
		Queued:             queued,
		Active:             active,
		TotalRequests:      g.totalRequests.Load(),
		Rejected:           g.rejected.Load(),
		Upstream429:        g.upstream429.Load(),
		RetryAttempts:      g.retryAttempts.Load(),
		Retried:            g.retried.Load(),
		UpstreamTimeoutSec: g.upstreamTimeoutSec(),
		LastError:          lastError,
		LogPath:            logPath,
		StatusCounts:       statusCounts,
		RecentEvents:       recentEvents,
	}
}

// SetCapacity updates the FIFO admission limit for future requests. Requests
// already admitted are allowed to finish when the capacity is reduced.
func (g *InferenceGateway) SetCapacity(capacity int) (InferenceGatewayStatus, error) {
	if g == nil || g.limiter == nil {
		return InferenceGatewayStatus{}, fmt.Errorf("inference gateway is not initialized")
	}
	if capacity < 1 || capacity > maxGatewayCapacity {
		return g.Status(), fmt.Errorf("gateway capacity must be between 1 and %d", maxGatewayCapacity)
	}
	g.limiter.setCapacity(capacity)
	g.recordEvent(GatewayEvent{
		Level:   "info",
		Method:  "CONTROL",
		Path:    "/aion/gateway/capacity",
		Message: fmt.Sprintf("gateway capacity changed to %d", capacity),
	})
	return g.Status(), nil
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
		g.emitActivity(reqInfo, "failed")
		g.recordFailure(reqInfo, http.StatusUnauthorized, start, "gateway unauthorized")
		writeGatewayJSON(w, http.StatusUnauthorized, map[string]string{"error": "gateway unauthorized"})
		return
	}
	stopPulse := g.startActivityPulse(r.Context(), reqInfo, "queued")
	defer stopPulse()
	queueStart := time.Now()
	release, err := g.acquire(r.Context())
	queueMS := time.Since(queueStart).Milliseconds()
	if err != nil {
		g.rejected.Add(1)
		g.recordError(err)
		g.emitActivity(reqInfo, "failed")
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
		g.emitActivity(reqInfo, "failed")
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
		g.emitActivity(reqInfo, "failed")
		g.recordFailure(reqInfo, http.StatusBadGateway, start, err.Error())
		writeGatewayJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		g.recordError(err)
		g.emitActivity(reqInfo, "failed")
		g.recordFailure(reqInfo, http.StatusBadRequest, start, "gateway request body could not be read")
		writeGatewayJSON(w, http.StatusBadRequest, map[string]string{"error": "gateway request body could not be read"})
		return
	}

	proxyCtx, cancelProxy := context.WithTimeout(r.Context(), time.Duration(g.upstreamTimeoutSec())*time.Second)
	defer cancelProxy()
	resp, err := g.forwardWithRetry(proxyCtx, r, target, body, profile, &reqInfo, start)
	if err != nil {
		g.recordError(err)
		phase := "failed"
		status := http.StatusBadGateway
		message := "upstream inference request failed: " + err.Error()
		if errors.Is(proxyCtx.Err(), context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
			message = "upstream inference request timed out"
		} else if r.Context().Err() != nil {
			phase = "canceled"
		}
		g.emitActivity(reqInfo, phase)
		g.recordFailure(reqInfo, status, start, message)
		if r.Context().Err() == nil {
			writeGatewayJSON(w, status, map[string]string{"error": message})
		}
		return
	}
	defer resp.Body.Close()
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
				g.emitActivity(reqInfo, "failed")
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
				g.emitActivity(reqInfo, "failed")
				if errors.Is(proxyCtx.Err(), context.DeadlineExceeded) {
					g.recordFailure(reqInfo, http.StatusGatewayTimeout, start, "upstream inference stream timed out")
				} else {
					g.recordFailure(reqInfo, resp.StatusCode, start, "upstream read failed: "+readErr.Error())
				}
				return
			}
			g.emitActivity(reqInfo, "completed")
			g.recordCompletion(reqInfo, resp.StatusCode, start, bytesWritten)
			return
		}
	}
}

func (g *InferenceGateway) upstreamTimeoutSec() int {
	if g != nil && g.config != nil && g.config.InferenceGateway.UpstreamTimeoutSec > 0 {
		return g.config.InferenceGateway.UpstreamTimeoutSec
	}
	return 300
}

func (g *InferenceGateway) forwardWithRetry(ctx context.Context, incoming *http.Request, target string, body []byte, profile ModelProfile, info *gatewayRequestInfo, start time.Time) (*http.Response, error) {
	maxRetries := 0
	if g.config != nil && g.config.InferenceGateway.MaxRetries > 0 {
		maxRetries = g.config.InferenceGateway.MaxRetries
	}
	maxAttempts := maxRetries + 1
	info.MaxAttempts = maxAttempts
	retried := false

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		info.Attempt = attempt
		g.emitActivity(*info, "forwarding")
		g.recordForwarding(*info, target, maxAttempts)

		req, err := http.NewRequestWithContext(ctx, incoming.Method, target, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("gateway request creation failed: %w", err)
		}
		copyGatewayHeaders(req.Header, incoming.Header)
		applyUpstreamAuth(req.Header, profile)
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, requestErr := g.client.Do(req)
		if requestErr == nil {
			g.recordStatus(resp.StatusCode)
			if resp.StatusCode == http.StatusTooManyRequests {
				g.upstream429.Add(1)
			}
			if !retryableGatewayStatus(resp.StatusCode) || attempt == maxAttempts {
				return resp, nil
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			requestErr = fmt.Errorf("upstream returned HTTP %d", resp.StatusCode)
		} else if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if attempt == maxAttempts || !retryableGatewayError(requestErr) {
			return nil, requestErr
		}
		if !retried {
			g.retried.Add(1)
			retried = true
		}
		g.retryAttempts.Add(1)
		delay := g.retryDelay(resp, attempt)
		g.emitActivity(*info, "retry_wait")
		g.recordEvent(GatewayEvent{
			Level:       "warn",
			RequestID:   info.RequestID,
			AgentID:     info.AgentID,
			DomainID:    info.DomainID,
			Profile:     info.Profile,
			Provider:    info.Provider,
			Method:      info.Method,
			Path:        info.Path,
			QueueMS:     info.QueueMS,
			Attempt:     attempt,
			MaxAttempts: maxAttempts,
			Message:     fmt.Sprintf("retrying upstream request after %s: %v", delay, requestErr),
		})
		if err := g.sleepFn(ctx, delay); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("upstream inference request exhausted retries")
}

func (g *InferenceGateway) recordForwarding(info gatewayRequestInfo, target string, maxAttempts int) {
	host := "configured upstream"
	if targetURL, err := url.Parse(target); err == nil && targetURL.Host != "" {
		host = targetURL.Host
	}
	g.recordEvent(GatewayEvent{
		Level:       "debug",
		RequestID:   info.RequestID,
		AgentID:     info.AgentID,
		DomainID:    info.DomainID,
		Profile:     info.Profile,
		Provider:    info.Provider,
		Method:      info.Method,
		Path:        info.Path,
		QueueMS:     info.QueueMS,
		Attempt:     info.Attempt,
		MaxAttempts: maxAttempts,
		Message:     "forwarding to upstream host " + host,
	})
}

func (g *InferenceGateway) startActivityPulse(ctx context.Context, info gatewayRequestInfo, phase string) func() {
	if strings.TrimSpace(info.AgentID) == "" {
		return func() {}
	}
	done := make(chan struct{})
	interval := g.activityPulseInterval
	if interval <= 0 {
		interval = defaultGatewayActivityPulseInterval
	}
	g.emitActivity(info, phase)
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
				g.emitActivityPulse(info)
			}
		}
	}()
	return func() {
		close(done)
	}
}

func (g *InferenceGateway) emitActivityPulse(info gatewayRequestInfo) {
	g.mu.Lock()
	fn := g.activityFn
	if item, ok := g.active[info.RequestID]; ok {
		item.updatedAt = time.Now()
		g.active[info.RequestID] = item
	}
	g.mu.Unlock()
	if fn != nil {
		fn(info.AgentID, info.DomainID, "active")
	}
}

func (g *InferenceGateway) emitActivity(info gatewayRequestInfo, phase string) {
	g.mu.Lock()
	fn := g.activityFn
	g.recordActiveLocked(info, phase)
	g.mu.Unlock()
	if fn != nil {
		fn(info.AgentID, info.DomainID, phase)
	}
}

func (g *InferenceGateway) recordActiveLocked(info gatewayRequestInfo, phase string) {
	if strings.TrimSpace(info.RequestID) == "" {
		return
	}
	if g.active == nil {
		g.active = make(map[string]gatewayActiveRequest)
	}
	if phase == "completed" || phase == "failed" || phase == "canceled" {
		delete(g.active, info.RequestID)
		return
	}
	now := time.Now()
	item := g.active[info.RequestID]
	if item.startedAt.IsZero() {
		item.startedAt = now
		item.info = info
	}
	if info.Provider != "" {
		item.info.Provider = info.Provider
	}
	if info.Profile != "" {
		item.info.Profile = info.Profile
	}
	if info.Attempt > 0 {
		item.info.Attempt = info.Attempt
	}
	if info.MaxAttempts > 0 {
		item.info.MaxAttempts = info.MaxAttempts
	}
	if info.QueueMS > 0 {
		item.info.QueueMS = info.QueueMS
	}
	item.phase = phase
	item.updatedAt = now
	g.active[info.RequestID] = item
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

func retryableGatewayStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func retryableGatewayError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if strings.HasPrefix(err.Error(), "upstream returned HTTP ") {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "connection reset") || strings.Contains(text, "unexpected eof") || strings.Contains(text, "server closed idle connection")
}

func (g *InferenceGateway) retryDelay(resp *http.Response, attempt int) time.Duration {
	base := time.Second
	maxDelay := 30 * time.Second
	if g.config != nil {
		if g.config.InferenceGateway.RetryBaseDelayMS > 0 {
			base = time.Duration(g.config.InferenceGateway.RetryBaseDelayMS) * time.Millisecond
		}
		if g.config.InferenceGateway.RetryMaxDelayMS > 0 {
			maxDelay = time.Duration(g.config.InferenceGateway.RetryMaxDelayMS) * time.Millisecond
		}
	}
	if resp != nil {
		if retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); retryAfter > 0 {
			if retryAfter > maxDelay {
				return maxDelay
			}
			return retryAfter
		}
	}
	delay := base
	for i := 1; i < attempt && delay < maxDelay; i++ {
		delay *= 2
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	half := delay / 2
	if half <= 0 {
		return delay
	}
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0
	}
	return when.Sub(now)
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
	RequestID   string
	AgentID     string
	DomainID    string
	Profile     string
	Provider    string
	Method      string
	Path        string
	QueueMS     int64
	Attempt     int
	MaxAttempts int
}

type gatewayActiveRequest struct {
	info      gatewayRequestInfo
	phase     string
	startedAt time.Time
	updatedAt time.Time
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
		At:          time.Now().UTC().Format(time.RFC3339Nano),
		Level:       level,
		RequestID:   info.RequestID,
		AgentID:     info.AgentID,
		DomainID:    info.DomainID,
		Profile:     info.Profile,
		Provider:    info.Provider,
		Method:      info.Method,
		Path:        info.Path,
		Status:      status,
		QueueMS:     info.QueueMS,
		DurationMS:  time.Since(start).Milliseconds(),
		Bytes:       bytesWritten,
		Attempt:     info.Attempt,
		MaxAttempts: info.MaxAttempts,
		Message:     message,
	})
}

func (g *InferenceGateway) recordFailure(info gatewayRequestInfo, status int, start time.Time, message string) {
	g.recordStatus(status)
	g.recordEvent(GatewayEvent{
		At:          time.Now().UTC().Format(time.RFC3339Nano),
		Level:       "error",
		RequestID:   info.RequestID,
		AgentID:     info.AgentID,
		DomainID:    info.DomainID,
		Profile:     info.Profile,
		Provider:    info.Provider,
		Method:      info.Method,
		Path:        info.Path,
		Status:      status,
		QueueMS:     info.QueueMS,
		DurationMS:  time.Since(start).Milliseconds(),
		Attempt:     info.Attempt,
		MaxAttempts: info.MaxAttempts,
		Message:     sanitizeGatewayMessage(message),
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
		log.Printf("inference-gateway: %s request=%s agent=%s domain=%s profile=%s provider=%s method=%s path=%s status=%d queue_ms=%d duration_ms=%d bytes=%d attempt=%d/%d msg=%s",
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
			event.Attempt,
			event.MaxAttempts,
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
	mu       sync.Mutex
	inUse    int
	waiters  []*gatewayWaiter
}

type gatewayWaiter struct {
	ready   chan struct{}
	granted bool
}

func newGatewayLimiter(capacity int) *gatewayLimiter {
	if capacity <= 0 {
		capacity = 1
	}
	return &gatewayLimiter{capacity: capacity}
}

func (l *gatewayLimiter) acquire(ctx context.Context) (func(), error) {
	l.mu.Lock()
	if len(l.waiters) == 0 && l.inUse < l.capacity {
		l.inUse++
		l.mu.Unlock()
		return l.releaseFunc(), nil
	}
	waiter := &gatewayWaiter{ready: make(chan struct{})}
	l.waiters = append(l.waiters, waiter)
	l.grantLocked()
	l.mu.Unlock()

	select {
	case <-waiter.ready:
		return l.releaseFunc(), nil
	case <-ctx.Done():
		l.mu.Lock()
		if waiter.granted {
			l.mu.Unlock()
			return l.releaseFunc(), nil
		}
		for i, queued := range l.waiters {
			if queued == waiter {
				l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
				break
			}
		}
		l.grantLocked()
		l.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (l *gatewayLimiter) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			if l.inUse > 0 {
				l.inUse--
			}
			l.grantLocked()
			l.mu.Unlock()
		})
	}
}

func (l *gatewayLimiter) setCapacity(capacity int) {
	if capacity <= 0 {
		capacity = 1
	}
	l.mu.Lock()
	l.capacity = capacity
	l.grantLocked()
	l.mu.Unlock()
}

func (l *gatewayLimiter) grantLocked() {
	for l.inUse < l.capacity && len(l.waiters) > 0 {
		waiter := l.waiters[0]
		l.waiters = l.waiters[1:]
		waiter.granted = true
		l.inUse++
		close(waiter.ready)
	}
}

func (l *gatewayLimiter) snapshot() (capacity, inUse, queued int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.capacity, l.inUse, len(l.waiters)
}
