package hub

import (
	"encoding/json"
	"testing"
)

// mockDeliverer implements ContextDeliverer for testing.
type mockDeliverer struct {
	id       string
	messages []Message
}

func (m *mockDeliverer) DeliverContext(msg Message) error {
	m.messages = append(m.messages, msg)
	return nil
}

func (m *mockDeliverer) AgentID() string {
	return m.id
}

func TestRouterDeliverToAgent(t *testing.T) {
	router := NewRouter("")

	agent := &mockDeliverer{id: "agent-1"}
	router.RegisterAgent("agent-1", agent)

	payload, _ := json.Marshal(StubFulfilledPayload{StubID: "stub-1"})
	msg := Message{
		ID:        "msg-1",
		Type:      MsgStubFulfilled,
		FromAgent: "agent-2",
		ToAgent:   "agent-1",
		Payload:   payload,
	}

	if err := router.Route(msg); err != nil {
		t.Fatalf("Route: %v", err)
	}

	if len(agent.messages) != 1 {
		t.Fatalf("expected 1 message delivered, got %d", len(agent.messages))
	}
	if agent.messages[0].Type != MsgStubFulfilled {
		t.Fatalf("expected StubFulfilled, got %s", agent.messages[0].Type)
	}
}

func TestRouterDeadLetter(t *testing.T) {
	router := NewRouter("")

	// Send to non-existent agent
	msg := Message{
		ID:        "msg-1",
		Type:      MsgContextShare,
		FromAgent: "agent-1",
		ToAgent:   "agent-2",
	}

	if err := router.Route(msg); err != nil {
		t.Fatalf("Route to non-existent agent should not error: %v", err)
	}

	if router.DeadLetterCount("agent-2") != 1 {
		t.Fatal("expected 1 dead letter")
	}

	// Register agent — dead letters should drain
	agent := &mockDeliverer{id: "agent-2"}
	router.RegisterAgent("agent-2", agent)

	// Give goroutine time to drain
	// In real tests we'd use a sync mechanism, but for this test we check the count
	if router.DeadLetterCount("agent-2") != 0 {
		t.Fatal("dead letters should be drained after registration")
	}
}

func TestRouterInternalMessages(t *testing.T) {
	router := NewRouter("")

	msg := Message{
		ID:   "msg-1",
		Type: MsgDependencyDiscovered,
	}

	if err := router.Route(msg); err != nil {
		t.Fatalf("internal message should not error: %v", err)
	}
}

func TestRouterUnregister(t *testing.T) {
	router := NewRouter("")

	agent := &mockDeliverer{id: "agent-1"}
	router.RegisterAgent("agent-1", agent)
	router.UnregisterAgent("agent-1")

	if router.RegisteredAgentCount() != 0 {
		t.Fatal("expected 0 registered agents after unregister")
	}
}

func TestRouterCorrectionRequest(t *testing.T) {
	router := NewRouter("")

	producer := &mockDeliverer{id: "agent-producer"}
	router.RegisterAgent("agent-producer", producer)

	msg := Message{
		ID:        "msg-1",
		Type:      MsgCorrectionRequest,
		FromAgent: "agent-consumer",
		ToAgent:   "agent-producer",
	}

	if err := router.Route(msg); err != nil {
		t.Fatalf("Route CorrectionRequest: %v", err)
	}

	if len(producer.messages) != 1 {
		t.Fatal("expected correction request delivered to producer")
	}
}

// errorDeliverer fails deliveries for testing retries.
type errorDeliverer struct {
	id        string
	attempts  int
	failUntil int
}

func (m *errorDeliverer) DeliverContext(msg Message) error {
	m.attempts++
	if m.attempts <= m.failUntil {
		return json.Unmarshal([]byte(`{"bad": json}`), &map[string]interface{}{}) // generic error
	}
	return nil
}

func (m *errorDeliverer) AgentID() string {
	return m.id
}

func TestCorrectionRequestBackoff(t *testing.T) {
	router := NewRouter("")

	agent := &errorDeliverer{id: "agent-1", failUntil: 4} // fail 4 times (meaning it should exhaust maxRetries=3)
	router.RegisterAgent("agent-1", agent)

	msg := Message{
		ID:        "msg-retry-test",
		Type:      MsgCorrectionRequest,
		FromAgent: "agent-2",
		ToAgent:   "agent-1",
	}

	// First attempt via Route()
	if err := router.Route(msg); err != nil {
		t.Fatalf("first route should not error synchronously: %v", err)
	}

	if agent.attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", agent.attempts)
	}

	// Wait briefly to let the background retry loop trigger. Use handleDeliveryFailure directly for deterministic testing of limits.
	err2 := router.handleDeliveryFailure(msg, json.Unmarshal([]byte(`{"bad": json}`), &map[string]interface{}{})) // retry 1
	if err2 != nil {
		t.Errorf("retry 1 returned err: %v", err2)
	}

	err3 := router.handleDeliveryFailure(msg, json.Unmarshal([]byte(`{"bad": json}`), &map[string]interface{}{})) // retry 2
	if err3 != nil {
		t.Errorf("retry 2 returned err: %v", err3)
	}

	err4 := router.handleDeliveryFailure(msg, json.Unmarshal([]byte(`{"bad": json}`), &map[string]interface{}{})) // retry 3 exhausts it
	if err4 == nil {
		t.Errorf("retry 3 should have returned an error for exhaustion")
	}
}
