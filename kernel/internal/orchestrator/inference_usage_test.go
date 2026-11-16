package orchestrator

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"aion-kernel/internal/dag"
)

func TestGatewayUsageCaptureParsesOpenAIAndAnthropic(t *testing.T) {
	tests := []struct {
		name           string
		body           string
		wantPrompt     int64
		wantCompletion int64
	}{
		{
			name:           "openai-json",
			body:           `{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":120,"completion_tokens":30}}`,
			wantPrompt:     120,
			wantCompletion: 30,
		},
		{
			name:           "openai-sse-final-chunk",
			body:           "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":80,\"completion_tokens\":20}}\n\ndata: [DONE]\n",
			wantPrompt:     80,
			wantCompletion: 20,
		},
		{
			name:           "anthropic-message-events",
			body:           "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":50,\"output_tokens\":1}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":25}}\n",
			wantPrompt:     50,
			wantCompletion: 25,
		},
		{
			name:           "largest-usage-wins",
			body:           `{"usage":{"prompt_tokens":10,"completion_tokens":5},"data":{"usage":{"prompt_tokens":40,"completion_tokens":9}}}`,
			wantPrompt:     40,
			wantCompletion: 9,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			capture := &gatewayUsageCapture{}
			capture.Write([]byte(tc.body))
			usage, ok := capture.Usage()
			if !ok {
				t.Fatal("usage not detected")
			}
			if usage.PromptTokens != tc.wantPrompt || usage.CompletionTokens != tc.wantCompletion {
				t.Fatalf("usage = %d/%d, want %d/%d", usage.PromptTokens, usage.CompletionTokens, tc.wantPrompt, tc.wantCompletion)
			}
		})
	}
}

func TestGatewayUsageCaptureIgnoresPayloadWithoutUsage(t *testing.T) {
	capture := &gatewayUsageCapture{}
	capture.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	if _, ok := capture.Usage(); ok {
		t.Fatal("usage was detected in a payload without usage fields")
	}
}

func TestInferenceGatewayRecordsPerAgentTokenUsage(t *testing.T) {
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
	}, t.TempDir())
	gateway.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"choices":[],"usage":{"prompt_tokens":120,"completion_tokens":30}}`)),
		}, nil
	})
	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"stream":false}`))
		req.Header.Set(gatewayAuthHeader, "local-key")
		req.Header.Set("X-Aion-Target-Profile", "forge")
		req.Header.Set("X-Aion-Agent-ID", "agent-alpha")
		rec := httptest.NewRecorder()
		gateway.handleProxy(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		return rec
	}
	request()
	request()
	prompt, completion := gateway.AgentTokenUsage("agent-alpha")
	if prompt != 240 || completion != 60 {
		t.Fatalf("agent usage = %d/%d, want 240/60", prompt, completion)
	}
	status := gateway.Status()
	if status.AgentUsage["agent-alpha"].PromptTokens != 240 || status.AgentUsage["agent-alpha"].CompletionTokens != 60 {
		t.Fatalf("status usage = %#v", status.AgentUsage)
	}
	if other, _ := gateway.AgentTokenUsage("agent-beta"); other != 0 {
		t.Fatalf("unrelated agent usage = %d", other)
	}
}

func TestNodeCompletionAttributesGatewayTokenUsage(t *testing.T) {
	server, addr := startTestServer(t)
	capability, err := server.IssueAgentCapability("agent-alpha")
	if err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"alpha-node", "beta-node"} {
		if err := server.dagManager.AddNode(dag.DagNode{
			ID:            nodeID,
			DomainID:      "alpha",
			AssignedAgent: "agent-alpha",
			TaskSpec:      "task " + nodeID,
			Status:        dag.StatusInProgress,
			TargetFiles:   []string{"alpha/" + nodeID + ".go"},
		}); err != nil {
			t.Fatal(err)
		}
	}
	usages := []gatewayTokenUsage{
		{PromptTokens: 120, CompletionTokens: 30},
		{PromptTokens: 200, CompletionTokens: 50},
	}
	call := 0
	server.SetTokenUsageCallback(func(agentID string) (int64, int64) {
		if agentID != "agent-alpha" {
			t.Fatalf("unexpected agent usage lookup %q", agentID)
		}
		usage := usages[call]
		call++
		return usage.PromptTokens, usage.CompletionTokens
	})

	first := sendTestRequestWithCapability(t, addr, "update-node", map[string]any{"node_id": "alpha-node", "status": "Done"}, capability)
	if first.Error != "" {
		t.Fatalf("first update: %s", first.Error)
	}
	node, err := server.dagManager.GetNode("alpha-node")
	if err != nil {
		t.Fatal(err)
	}
	if node.PromptTokens != 120 || node.CompletionTokens != 30 {
		t.Fatalf("first node tokens = %d/%d, want 120/30", node.PromptTokens, node.CompletionTokens)
	}

	second := sendTestRequestWithCapability(t, addr, "update-node", map[string]any{"node_id": "beta-node", "status": "Done"}, capability)
	if second.Error != "" {
		t.Fatalf("second update: %s", second.Error)
	}
	node, err = server.dagManager.GetNode("beta-node")
	if err != nil {
		t.Fatal(err)
	}
	if node.PromptTokens != 80 || node.CompletionTokens != 20 {
		t.Fatalf("second node tokens = %d/%d, want 80/20", node.PromptTokens, node.CompletionTokens)
	}
}
