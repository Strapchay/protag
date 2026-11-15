package orchestrator

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	coordination "aion-coordination"
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
	statusParams, _ := json.Marshal(map[string]string{"id": messageID})
	message, err = store.Status(t.Context(), messageID)
	if err != nil || message.ActorState != messaging.ActorAcked || message.Note != "received" {
		t.Fatalf("acked message=%#v err=%v", message, err)
	}
	notifications, err := store.List(t.Context(), messaging.ListFilter{FromAgent: "agent-ledger", ToAgent: "agent-report"})
	if err != nil || len(notifications) != 1 || notifications[0].Kind != messaging.KindNotify {
		t.Fatalf("status notification=%#v err=%v", notifications, err)
	}

	otherCapability, err := server.IssueAgentRuntimeCapability(AgentCapabilityPolicy{
		AgentID: "agent-other", DomainID: "other", Provider: "deepseek", Model: "deepseek-flash",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response := server.handleMessageStatus(Request{ID: "status-other", Capability: otherCapability, Params: statusParams}); response.Error == "" {
		t.Fatal("unrelated agent was allowed to query the message")
	}
	listParams, _ := json.Marshal(map[string]any{"agent_id": "agent-report"})
	if response := server.handleMessageList(Request{ID: "list-other", Capability: otherCapability, Params: listParams}); response.Error == "" {
		t.Fatal("unrelated agent was allowed to list another agent's messages")
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

func TestMessageStatusWaitReturnsOnActorChange(t *testing.T) {
	store, err := messaging.Open(filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	server := &Server{capabilities: newAgentCapabilityRegistry(), messageStore: store}
	capability, _ := server.IssueAgentRuntimeCapability(AgentCapabilityPolicy{
		AgentID: "agent-report", DomainID: "report", Provider: "deepseek", Model: "deepseek-flash",
	})
	if err := store.Submit(t.Context(), messaging.Message{
		ID: "msg-wait", FromAgent: "agent-report", ToAgent: "agent-ledger",
		Kind: messaging.KindRequest, Payload: "plan",
	}); err != nil {
		t.Fatal(err)
	}
	params, _ := json.Marshal(map[string]any{"id": "msg-wait", "wait": true, "timeout_ms": 2000})

	result := make(chan Response, 1)
	go func() {
		result <- server.handleMessageStatus(Request{ID: "status-wait", Capability: capability, Params: params})
	}()
	time.Sleep(100 * time.Millisecond)
	if err := store.Ack(t.Context(), "msg-wait", messaging.ActorAcked, "received"); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-result:
		if response.Error != "" {
			t.Fatalf("wait: %s", response.Error)
		}
		message, ok := response.Result.(messaging.Message)
		if !ok || message.ActorState != messaging.ActorAcked {
			t.Fatalf("wait result=%#v", response.Result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("message-status --wait did not return after the actor ack")
	}
}

func TestMessageStatusWaitRejectsExcessiveTimeout(t *testing.T) {
	store, err := messaging.Open(filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	server := &Server{capabilities: newAgentCapabilityRegistry(), messageStore: store}
	if err := store.Submit(t.Context(), messaging.Message{
		ID: "msg-timeout", FromAgent: "agent-report", ToAgent: "agent-ledger",
		Kind: messaging.KindRequest, Payload: "plan",
	}); err != nil {
		t.Fatal(err)
	}
	params, _ := json.Marshal(map[string]any{
		"id": "msg-timeout", "wait": true, "timeout_ms": (16 * time.Minute).Milliseconds(),
	})
	response := server.handleMessageStatus(Request{ID: "status-timeout", Params: params})
	if response.Error == "" {
		t.Fatal("excessive message wait was accepted")
	}
}

func TestContractHandshakeHelpers(t *testing.T) {
	if !nodeRequiresPeerHandshake("Send CONSUMER PLAN with --expects-ack before validation") {
		t.Fatal("handshake task was not detected")
	}
	if nodeRequiresPeerHandshake("Implement the ledger package") {
		t.Fatal("plain task was misclassified")
	}
	contract := &coordination.Contract{
		ConsumerAgentID: "agent-report",
		ProducerAgentID: "agent-ledger",
		Details:         coordination.ContractDetails{Name: "ledger-validation-rules"},
	}
	message := messaging.Message{
		Kind: messaging.KindRequest, FromAgent: "agent-report", ToAgent: "agent-ledger",
		ActorState: messaging.ActorAcked, Payload: "CONSUMER PLAN | ledger-validation-rules",
	}
	if !messageSatisfiesContractHandshake(message, contract) {
		t.Fatal("valid handshake was rejected")
	}
	message.ActorState = messaging.ActorAwaitingAck
	if messageSatisfiesContractHandshake(message, contract) {
		t.Fatal("unacknowledged handshake was accepted")
	}
}
