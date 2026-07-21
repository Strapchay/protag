package dashboard

import (
	"errors"
	"testing"
)

func TestHubViewRetriesSnapshotAndStreamFailures(t *testing.T) {
	m := NewHubModelWithOptions("127.0.0.1:0", Options{Verbose: true})

	updated, cmd := m.Update(hubSnapshotMsg{Err: errors.New("not ready")})
	m = updated.(*HubModel)
	if cmd == nil || m.reconnectAttempt != 1 {
		t.Fatalf("snapshot failure did not schedule reconnect: attempt=%d cmd=%v", m.reconnectAttempt, cmd)
	}

	oldEvents := m.eventCh
	updated, cmd = m.Update(streamHubClosedMsg{})
	m = updated.(*HubModel)
	if cmd == nil || m.reconnectAttempt != 2 {
		t.Fatalf("stream failure did not schedule reconnect: attempt=%d cmd=%v", m.reconnectAttempt, cmd)
	}
	if m.eventCh == oldEvents {
		t.Fatal("stream reconnect reused the closed event channel")
	}

	updated, _ = m.Update(hubSnapshotMsg{})
	m = updated.(*HubModel)
	if m.reconnectAttempt != 0 {
		t.Fatalf("successful snapshot did not reset reconnect attempt: %d", m.reconnectAttempt)
	}
}

func TestDashboardReconnectDelayIsBounded(t *testing.T) {
	if got := dashboardReconnectDelay(1); got <= 0 {
		t.Fatalf("first reconnect delay = %s", got)
	}
	if got := dashboardReconnectDelay(100); got > 5_000_000_000 {
		t.Fatalf("reconnect delay exceeded cap: %s", got)
	}
}
