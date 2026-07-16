package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
