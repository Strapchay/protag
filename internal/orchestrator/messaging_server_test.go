package orchestrator

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"aion-kernel/internal/messaging"
)

func TestSendMessageUsesCapabilityIdentityAndRequestsAck(t *testing.T) {
	store, err := messaging.Open(filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	server := &Server{capabilities: newAgentCapabilityRegistry(), messageStore: store}
	capability, err := server.IssueAgentRuntimeCapability(AgentCapabilityPolicy{
		AgentID: "agent-report", DomainID: "report", Provider: "deepseek", Model: "deepseek-flash",
	})
	if err != nil {
		t.Fatal(err)
	}

	params, _ := json.Marshal(map[string]any{
		"agent_id": "agent-ledger", "text": "CONSUMER PLAN", "expects_ack": true,
	})
	response := server.handleSendMessage(Request{ID: "send-1", Capability: capability, Params: params})
	if response.Error != "" {
		t.Fatalf("send: %s", response.Error)
	}
	result, _ := response.Result.(map[string]string)
	messageID := result["message_id"]
	if messageID == "" || result["from_agent"] != "agent-report" || result["kind"] != messaging.KindRequest {
		t.Fatalf("result=%#v", result)
	}

	message, err := store.Status(t.Context(), messageID)
	if err != nil {
		t.Fatal(err)
	}
	if message.FromAgent != "agent-report" || message.ActorState != messaging.ActorAwaitingAck {
		t.Fatalf("message=%#v", message)
	}

	ackParams, _ := json.Marshal(map[string]any{"id": messageID, "state": messaging.ActorAcked, "note": "received"})
	ackResponse := server.handleMessageAck(Request{ID: "ack-1", Capability: capability, Params: ackParams})
	if ackResponse.Error == "" {
		t.Fatal("non-target agent was allowed to ack the message")
	}

	targetCapability, err := server.IssueAgentRuntimeCapability(AgentCapabilityPolicy{
		AgentID: "agent-ledger", DomainID: "ledger", Provider: "deepseek", Model: "deepseek-flash",
	})
	if err != nil {
		t.Fatal(err)
	}
	ackResponse = server.handleMessageAck(Request{ID: "ack-2", Capability: targetCapability, Params: ackParams})
	if ackResponse.Error != "" {
		t.Fatalf("target ack failed: %s", ackResponse.Error)
	}
	message, err = store.Status(t.Context(), messageID)
	if err != nil || message.ActorState != messaging.ActorAcked || message.Note != "received" {
		t.Fatalf("acked message=%#v err=%v", message, err)
	}
}

func TestSendMessageUserIdentityAndStatusQuery(t *testing.T) {
	store, err := messaging.Open(filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	server := &Server{capabilities: newAgentCapabilityRegistry(), messageStore: store}

	params, _ := json.Marshal(map[string]any{"agent_id": "agent-ledger", "text": "status check"})
	response := server.handleSendMessage(Request{ID: "send-2", Params: params})
	if response.Error != "" {
		t.Fatalf("send: %s", response.Error)
	}
	result, _ := response.Result.(map[string]string)
	if result["from_agent"] != "user" || result["kind"] != messaging.KindNotify {
		t.Fatalf("result=%#v", result)
	}

	statusParams, _ := json.Marshal(map[string]string{"id": result["message_id"]})
	statusResponse := server.handleMessageStatus(Request{ID: "status-1", Params: statusParams})
	if statusResponse.Error != "" {
		t.Fatalf("status: %s", statusResponse.Error)
	}
	message, ok := statusResponse.Result.(messaging.Message)
	if !ok || message.ID != result["message_id"] || message.FromAgent != "user" {
		t.Fatalf("status=%#v", statusResponse.Result)
	}
}
