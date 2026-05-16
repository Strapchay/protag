package stub

import "testing"

func TestRegisterAndGet(t *testing.T) {
	reg := NewRegistry()

	contract := Contract{
		ID:         "stub-001",
		ProducerID: "agent-a",
		ConsumerID: "agent-b",
		Details: ContractDetails{
			Name:     "GetUser",
			Kind:     "function",
			FilePath: "internal/db/user_repo.go",
		},
	}

	if err := reg.Register(contract); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, err := reg.GetByID("stub-001")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Details.Name != "GetUser" {
		t.Fatalf("expected GetUser, got %s", got.Details.Name)
	}
	if got.Status != StatusPending {
		t.Fatalf("expected Pending, got %s", got.Status)
	}
}

func TestRegisterDuplicate(t *testing.T) {
	reg := NewRegistry()

	contract := Contract{ID: "stub-001", ProducerID: "a", ConsumerID: "b"}
	reg.Register(contract)

	if err := reg.Register(contract); err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestFulfill(t *testing.T) {
	reg := NewRegistry()

	reg.Register(Contract{ID: "stub-001", ProducerID: "a", ConsumerID: "b"})

	fulfilled, err := reg.Fulfill("stub-001")
	if err != nil {
		t.Fatalf("Fulfill: %v", err)
	}
	if fulfilled.Status != StatusFulfilled {
		t.Fatalf("expected Fulfilled, got %s", fulfilled.Status)
	}
	if fulfilled.FulfilledAt == nil {
		t.Fatal("FulfilledAt should be set")
	}
}

func TestFulfillAlreadyFulfilled(t *testing.T) {
	reg := NewRegistry()

	reg.Register(Contract{ID: "stub-001", ProducerID: "a", ConsumerID: "b"})
	reg.Fulfill("stub-001")

	if _, err := reg.Fulfill("stub-001"); err == nil {
		t.Fatal("expected error fulfilling already-fulfilled stub")
	}
}

func TestGetPending(t *testing.T) {
	reg := NewRegistry()

	reg.Register(Contract{ID: "s1", ProducerID: "agent-a", ConsumerID: "agent-b"})
	reg.Register(Contract{ID: "s2", ProducerID: "agent-a", ConsumerID: "agent-c"})
	reg.Register(Contract{ID: "s3", ProducerID: "agent-b", ConsumerID: "agent-a"})

	pending := reg.GetPending("agent-a")
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending for agent-a, got %d", len(pending))
	}
}

func TestGetWaiting(t *testing.T) {
	reg := NewRegistry()

	reg.Register(Contract{ID: "s1", ProducerID: "agent-a", ConsumerID: "agent-b"})
	reg.Register(Contract{ID: "s2", ProducerID: "agent-c", ConsumerID: "agent-b"})

	waiting := reg.GetWaiting("agent-b")
	if len(waiting) != 2 {
		t.Fatalf("expected 2 waiting for agent-b, got %d", len(waiting))
	}
}

func TestFulfillNotFound(t *testing.T) {
	reg := NewRegistry()

	if _, err := reg.Fulfill("nonexistent"); err == nil {
		t.Fatal("expected not found error")
	}
}

func TestReject(t *testing.T) {
	reg := NewRegistry()

	reg.Register(Contract{ID: "s1", ProducerID: "a", ConsumerID: "b"})

	if err := reg.Reject("s1", "test reason"); err != nil {
		t.Fatalf("Reject: %v", err)
	}

	got, _ := reg.GetByID("s1")
	if got.Status != StatusRejected {
		t.Fatalf("expected Rejected, got %s", got.Status)
	}
}
