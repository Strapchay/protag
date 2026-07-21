package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"aion-kernel/internal/hub"
)

func TestPiGatewayExtensionStreamsOverUnixSocket(t *testing.T) {
	piBinary, err := exec.LookPath("pi")
	if err != nil {
		t.Skip("Pi is not installed")
	}
	extension, err := filepath.Abs(filepath.Join("..", "..", "install", "pi", "extensions", "aion-gateway-provider.ts"))
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name     string
		protocol string
		path     string
		stream   string
	}{
		{
			name:     "openai-compatible",
			protocol: "openai-completions",
			path:     "/v1/chat/completions",
			stream: strings.Join([]string{
				`data: {"id":"response-openai","model":"test-model","choices":[{"delta":{"reasoning_content":"socket thought"}}]}`,
				`data: {"id":"response-openai","choices":[{"delta":{"content":"socket reply"},"finish_reason":"stop"}]}`,
				`data: {"id":"response-openai","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2}}`,
				`data: [DONE]`,
				"",
			}, "\n\n"),
		},
		{
			name:     "anthropic",
			protocol: "anthropic-messages",
			path:     "/v1/messages",
			stream: strings.Join([]string{
				sseEvent("message_start", `{"type":"message_start","message":{"id":"response-anthropic","model":"test-model","usage":{"input_tokens":3,"output_tokens":0}}}`),
				sseEvent("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`),
				sseEvent("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"socket thought"}}`),
				sseEvent("content_block_stop", `{"type":"content_block_stop","index":0}`),
				sseEvent("content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`),
				sseEvent("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"socket reply"}}`),
				sseEvent("content_block_stop", `{"type":"content_block_stop","index":1}`),
				sseEvent("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`),
				sseEvent("message_stop", `{"type":"message_stop"}`),
				"",
			}, "\n\n"),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runPiUnixExtensionCase(t, piBinary, extension, testCase.protocol, testCase.path, testCase.stream)
		})
	}
}

func TestPiGatewayExtensionNormalizesTextEncodedToolCall(t *testing.T) {
	piBinary, err := exec.LookPath("pi")
	if err != nil {
		t.Skip("Pi is not installed")
	}
	extension, err := filepath.Abs(filepath.Join("..", "..", "install", "pi", "extensions", "aion-gateway-provider.ts"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	socketPath := filepath.Join(root, "inference.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	requestErrors := make(chan error, 3)
	var requestCount atomic.Int32
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/aion/gateway/extension-loaded" {
			w.WriteHeader(http.StatusOK)
			return
		}
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			requestErrors <- readErr
			http.Error(w, "read request", http.StatusBadRequest)
			return
		}
		number := requestCount.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		if number == 1 {
			if !strings.Contains(string(body), `"name":"bash"`) {
				requestErrors <- fmt.Errorf("Pi tools were not forwarded: %s", body)
				return
			}
			encoded := "{\n  \"name\": \"bash\",\n  \"parameters\": {\"command\": \"printf recovered-tool-call\n\"}\n}\n</tool_call>\n"
			chunk, _ := json.Marshal(map[string]any{
				"id":    "text-tool-call",
				"model": "test-model",
				"choices": []any{map[string]any{
					"delta":         map[string]any{"content": encoded},
					"finish_reason": "stop",
				}},
			})
			_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", chunk)
			requestErrors <- nil
			return
		}
		if number == 2 {
			if !strings.Contains(string(body), "recovered-tool-call") {
				requestErrors <- fmt.Errorf("recovered JSON tool result missing from follow-up: %s", body)
				return
			}
			encoded := "<function=bash>\n<parameter=command>\nprintf recovered-tagged-tool-call\n</parameter>\n</function>\n</tool_call>\n"
			chunk, _ := json.Marshal(map[string]any{
				"id":    "tagged-tool-call",
				"model": "test-model",
				"choices": []any{map[string]any{
					"delta":         map[string]any{"content": encoded},
					"finish_reason": "stop",
				}},
			})
			_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", chunk)
			requestErrors <- nil
			return
		}
		if !strings.Contains(string(body), "recovered-tagged-tool-call") {
			requestErrors <- fmt.Errorf("recovered tool result missing from follow-up: %s", body)
			return
		}
		chunk, _ := json.Marshal(map[string]any{
			"id":    "after-tool-call",
			"model": "test-model",
			"choices": []any{map[string]any{
				"delta":         map[string]any{"content": "fallback complete"},
				"finish_reason": "stop",
			}},
		})
		_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", chunk)
		requestErrors <- nil
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = os.Remove(socketPath)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	process, err := SpawnPiAgent(ctx, PiAgentConfig{
		Binary:         piBinary,
		SessionDir:     filepath.Join(root, "sessions"),
		HostSessionDir: filepath.Join(root, "sessions"),
		WorkingDir:     root,
		Provider:       "aion-gateway",
		Model:          "test-model",
		ExtensionPaths: []string{extension},
		Env: []string{
			"AION_INFERENCE_GATEWAY_ENABLED=true",
			"AION_INFERENCE_SOCKET=" + socketPath,
			"AION_AGENT_CAPABILITY=test-capability",
			"AION_TARGET_MODEL=test-model",
			"AION_TARGET_PROFILE=test-profile",
			"AION_TARGET_API=openai-completions",
			"PI_CODING_AGENT_DIR=" + filepath.Join(root, "pi-config"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Kill() })
	if err := process.SendPrompt("Exercise the text-encoded tool-call adapter."); err != nil {
		t.Fatal(err)
	}

	var finalText string
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Pi tool-call recovery timed out; requests=%d text=%q", requestCount.Load(), finalText)
		case event, ok := <-process.Events():
			if !ok {
				t.Fatalf("Pi event stream closed; requests=%d text=%q", requestCount.Load(), finalText)
			}
			if message, parseErr := event.ParseMessage(); parseErr == nil && message.FullText() != "" {
				finalText = message.FullText()
			}
			if event.Type == "agent_end" {
				goto complete
			}
		}
	}

complete:
	if requestCount.Load() != 3 {
		t.Fatalf("expected recovered tool executions to produce three inference requests, got %d", requestCount.Load())
	}
	for range 3 {
		if err := <-requestErrors; err != nil {
			t.Fatal(err)
		}
	}
	if finalText != "fallback complete" {
		t.Fatalf("unexpected final text %q", finalText)
	}
}

func sseEvent(eventType, payload string) string {
	return "event: " + eventType + "\ndata: " + payload
}

func runPiUnixExtensionCase(t *testing.T, piBinary, extension, protocol, inferencePath, responseStream string) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "aion-pi-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socketPath := filepath.Join(root, "inference.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	requestSeen := make(chan error, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/aion/gateway/extension-loaded" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != inferencePath {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		if r.Header.Get("X-Aion-Agent-Capability") != "test-capability" {
			requestSeen <- fmt.Errorf("missing agent capability")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			requestSeen <- err
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if payload["model"] != "test-model" || payload["stream"] != true {
			requestSeen <- fmt.Errorf("unexpected payload: %#v", payload)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		requestSeen <- nil
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(responseStream))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = os.Remove(socketPath)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	process, err := SpawnPiAgent(ctx, PiAgentConfig{
		Binary:         piBinary,
		SessionDir:     filepath.Join(root, "sessions"),
		HostSessionDir: filepath.Join(root, "sessions"),
		WorkingDir:     root,
		Provider:       "aion-gateway",
		Model:          "test-model",
		ExtensionPaths: []string{extension},
		Env: []string{
			"AION_INFERENCE_GATEWAY_ENABLED=true",
			"AION_INFERENCE_SOCKET=" + socketPath,
			"AION_AGENT_CAPABILITY=test-capability",
			"AION_TARGET_MODEL=test-model",
			"AION_TARGET_PROFILE=test-profile",
			"AION_TARGET_API=" + protocol,
			"PI_CODING_AGENT_DIR=" + filepath.Join(root, "pi-config"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Kill() })
	if err := process.SendPrompt("Return the configured local test response."); err != nil {
		t.Fatal(err)
	}

	var thinking, text string
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Pi stream timed out; thinking=%q text=%q", thinking, text)
		case event, ok := <-process.Events():
			if !ok {
				t.Fatalf("Pi event stream closed; thinking=%q text=%q", thinking, text)
			}
			if message, err := event.ParseMessage(); err == nil {
				if got := message.FullThinking(); got != "" {
					thinking = got
				}
				if got := message.FullText(); got != "" {
					text = got
				}
			}
			if event.Type == "agent_end" {
				goto complete
			}
		}
	}

complete:
	if err := <-requestSeen; err != nil {
		t.Fatal(err)
	}
	if thinking != "socket thought" || text != "socket reply" {
		t.Fatalf("Pi stream thinking=%q text=%q", thinking, text)
	}
}

func TestPiRPCTurnDeliverySemantics(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		deliver func(*PiAgentProcess) error
	}{
		{name: "idle-follow-up", deliver: func(process *PiAgentProcess) error {
			return process.SendFollowUp("idle follow-up marker")
		}},
		{name: "idle-steer", deliver: func(process *PiAgentProcess) error {
			return process.SendSteer("idle steer marker")
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			process, probe := spawnPiRPCProbe(t, false)
			if err := testCase.deliver(process); err != nil {
				t.Fatal(err)
			}
			assertNoPiRPCRequest(t, probe.requests, 750*time.Millisecond)
		})
	}

	t.Run("idle-prompt-starts-turn", func(t *testing.T) {
		process, probe := spawnPiRPCProbe(t, false)
		if err := process.SendPrompt("idle prompt marker"); err != nil {
			t.Fatal(err)
		}
		request := awaitPiRPCRequest(t, probe.requests)
		if !strings.Contains(request.body, "idle prompt marker") {
			t.Fatalf("prompt was not forwarded to inference: %s", request.body)
		}
	})

	for _, testCase := range []struct {
		name    string
		marker  string
		deliver func(*PiAgentProcess, string) error
	}{
		{name: "active-follow-up-queues-next-turn", marker: "active follow-up marker", deliver: func(process *PiAgentProcess, marker string) error {
			return process.SendFollowUp(marker)
		}},
		{name: "active-steer-queues-next-turn", marker: "active steer marker", deliver: func(process *PiAgentProcess, marker string) error {
			return process.SendSteer(marker)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			process, probe := spawnPiRPCProbe(t, true)
			if err := process.SendPrompt("start active turn"); err != nil {
				t.Fatal(err)
			}
			_ = awaitPiRPCRequest(t, probe.requests)
			if err := testCase.deliver(process, testCase.marker); err != nil {
				t.Fatal(err)
			}
			close(probe.releaseFirst)
			request := awaitPiRPCRequest(t, probe.requests)
			if !strings.Contains(request.body, testCase.marker) {
				t.Fatalf("queued command was not forwarded on the next turn: %s", request.body)
			}
		})
	}
}

func TestAgentSupervisorUsesPromptForIdleContractDelivery(t *testing.T) {
	probe, config := newPiRPCProbe(t, false)
	supervisor := NewAgentSupervisor(AgentConfig{
		AgentID:          "agent-consumer",
		DomainID:         "consumer",
		PiAgent:          config,
		HeartbeatTimeout: time.Hour,
		ProgressTimeout:  time.Hour,
		MaxCrashRestarts: 1,
	})
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		supervisor.mu.Lock()
		process := supervisor.piAgent
		supervisor.mu.Unlock()
		if process != nil {
			_ = process.Kill()
		}
		_ = supervisor.Stop()
	})

	payload, _ := json.Marshal(map[string]any{"id": "contract-idle", "revision": 1})
	if err := supervisor.DeliverContext(hub.Message{
		ID:        "effect-idle",
		Type:      hub.MsgContractProduced,
		FromAgent: "orchestrator",
		ToAgent:   "agent-consumer",
		Payload:   payload,
	}); err != nil {
		t.Fatal(err)
	}
	request := awaitPiRPCRequest(t, probe.requests)
	if !strings.Contains(request.body, "ContractProduced") || !strings.Contains(request.body, "contract-idle") {
		t.Fatalf("idle contract event did not start a Pi prompt turn: %s", request.body)
	}
}

func TestAgentSupervisorQueuesContractDuringActiveTurn(t *testing.T) {
	probe, config := newPiRPCProbe(t, true)
	supervisor := NewAgentSupervisor(AgentConfig{
		AgentID:          "agent-producer",
		DomainID:         "producer",
		InitialPrompt:    "start active contract turn",
		PiAgent:          config,
		HeartbeatTimeout: time.Hour,
		ProgressTimeout:  time.Hour,
		MaxCrashRestarts: 1,
	})
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		supervisor.mu.Lock()
		process := supervisor.piAgent
		supervisor.mu.Unlock()
		if process != nil {
			_ = process.Kill()
		}
		_ = supervisor.Stop()
	})

	_ = awaitPiRPCRequest(t, probe.requests)
	payload, _ := json.Marshal(map[string]any{"id": "contract-active", "revision": 0})
	if err := supervisor.DeliverContext(hub.Message{
		ID:        "effect-active",
		Type:      hub.MsgContractRequested,
		FromAgent: "orchestrator",
		ToAgent:   "agent-producer",
		Payload:   payload,
	}); err != nil {
		t.Fatal(err)
	}
	close(probe.releaseFirst)
	request := awaitPiRPCRequest(t, probe.requests)
	if !strings.Contains(request.body, "ContractRequested") || !strings.Contains(request.body, "contract-active") {
		t.Fatalf("active contract event was not queued for the next Pi turn: %s", request.body)
	}
}

type piRPCRequest struct {
	number int32
	body   string
}

type piRPCProbe struct {
	requests     chan piRPCRequest
	releaseFirst chan struct{}
}

func spawnPiRPCProbe(t *testing.T, blockFirst bool) (*PiAgentProcess, *piRPCProbe) {
	t.Helper()
	probe, config := newPiRPCProbe(t, blockFirst)
	process, err := SpawnPiAgent(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Kill() })
	return process, probe
}

func newPiRPCProbe(t *testing.T, blockFirst bool) (*piRPCProbe, PiAgentConfig) {
	t.Helper()
	piBinary, err := exec.LookPath("pi")
	if err != nil {
		t.Skip("Pi is not installed")
	}
	extension, err := filepath.Abs(filepath.Join("..", "..", "install", "pi", "extensions", "aion-gateway-provider.ts"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("/tmp", "aion-pi-turns-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socketPath := filepath.Join(root, "inference.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	probe := &piRPCProbe{
		requests:     make(chan piRPCRequest, 8),
		releaseFirst: make(chan struct{}),
	}
	var requestCount atomic.Int32
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/aion/gateway/extension-loaded" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/v1/chat/completions" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			http.Error(w, "read request", http.StatusBadRequest)
			return
		}
		number := requestCount.Add(1)
		probe.requests <- piRPCRequest{number: number, body: string(body)}
		if blockFirst && number == 1 {
			select {
			case <-probe.releaseFirst:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: {\"id\":\"response-%d\",\"model\":\"test-model\",\"choices\":[{\"delta\":{\"content\":\"reply-%d\"},\"finish_reason\":\"stop\"}]}\n\n", number, number)
		_, _ = fmt.Fprint(w, "data: {\"id\":\"usage\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = os.Remove(socketPath)
	})
	return probe, PiAgentConfig{
		Binary:         piBinary,
		SessionDir:     filepath.Join(root, "sessions"),
		HostSessionDir: filepath.Join(root, "sessions"),
		WorkingDir:     root,
		Provider:       "aion-gateway",
		Model:          "test-model",
		ExtensionPaths: []string{extension},
		Env: []string{
			"AION_INFERENCE_GATEWAY_ENABLED=true",
			"AION_INFERENCE_SOCKET=" + socketPath,
			"AION_AGENT_CAPABILITY=test-capability",
			"AION_TARGET_MODEL=test-model",
			"AION_TARGET_PROFILE=test-profile",
			"AION_TARGET_API=openai-completions",
			"PI_CODING_AGENT_DIR=" + filepath.Join(root, "pi-config"),
		},
	}
}

func awaitPiRPCRequest(t *testing.T, requests <-chan piRPCRequest) piRPCRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Pi inference request")
		return piRPCRequest{}
	}
}

func assertNoPiRPCRequest(t *testing.T, requests <-chan piRPCRequest, duration time.Duration) {
	t.Helper()
	select {
	case request := <-requests:
		t.Fatalf("idle RPC command unexpectedly started inference request %d: %s", request.number, request.body)
	case <-time.After(duration):
	}
}
