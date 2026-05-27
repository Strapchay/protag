package orchestrator

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
	"aion-kernel/internal/locking"
	"aion-kernel/internal/stub"
)

func startTestServer(t testing.TB) (*Server, string) {
	t.Helper()
	dir := t.TempDir()

	dagMgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(dir, "dag.bin"),
		WalFilePath:   filepath.Join(dir, "dag.wal"),
		MaxNodes:      200,
		FlushDeadline: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { dagMgr.Close() })

	lockMgr := locking.NewManager([]string{"go.mod"})
	stubReg := stub.NewRegistry()

	srv := NewServer(dagMgr, lockMgr, stubReg, nil)

	// Listen on random port
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv.listener = ln

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.handleConnection(conn)
		}
	}()

	t.Cleanup(func() { srv.Stop() })

	return srv, ln.Addr().String()
}

func sendTestRequest(t testing.TB, addr string, method string, params interface{}) Response {
	t.Helper()

	paramsJSON, _ := json.Marshal(params)
	req := Request{
		Method: method,
		Params: paramsJSON,
		ID:     "test-1",
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(req); err != nil {
		t.Fatalf("encode: %v", err)
	}

	var resp Response
	if err := decoder.Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	return resp
}

func TestServerAcquireReleaseLock(t *testing.T) {
	_, addr := startTestServer(t)

	// Acquire
	resp := sendTestRequest(t, addr, "acquire-lock", map[string]string{
		"file":     "auth.go",
		"agent_id": "agent-1",
	})
	if resp.Error != "" {
		t.Fatalf("acquire-lock error: %s", resp.Error)
	}

	// Conflict
	resp = sendTestRequest(t, addr, "acquire-lock", map[string]string{
		"file":     "auth.go",
		"agent_id": "agent-2",
	})
	if resp.Error == "" {
		t.Fatal("expected lock conflict error")
	}

	// Release
	resp = sendTestRequest(t, addr, "release-lock", map[string]string{
		"file":     "auth.go",
		"agent_id": "agent-1",
	})
	if resp.Error != "" {
		t.Fatalf("release-lock error: %s", resp.Error)
	}
}

func TestServerSharedFileRejection(t *testing.T) {
	_, addr := startTestServer(t)

	resp := sendTestRequest(t, addr, "acquire-lock", map[string]string{
		"file":     "go.mod",
		"agent_id": "agent-1",
	})
	if resp.Error == "" {
		t.Fatal("expected shared file rejection")
	}
}

func TestServerHeartbeat(t *testing.T) {
	srv, addr := startTestServer(t)

	resp := sendTestRequest(t, addr, "heartbeat", map[string]string{
		"agent_id": "agent-1",
	})
	if resp.Error != "" {
		t.Fatalf("heartbeat error: %s", resp.Error)
	}

	ts, ok := srv.GetLastHeartbeat("agent-1")
	if !ok {
		t.Fatal("expected heartbeat recorded")
	}
	if ts == 0 {
		t.Fatal("expected non-zero heartbeat timestamp")
	}
}

func TestServerReadDagEmpty(t *testing.T) {
	_, addr := startTestServer(t)

	resp := sendTestRequest(t, addr, "read-dag", map[string]string{})
	if resp.Error != "" {
		t.Fatalf("read-dag error: %s", resp.Error)
	}
}

func TestServerUnknownMethod(t *testing.T) {
	_, addr := startTestServer(t)

	resp := sendTestRequest(t, addr, "nonexistent", map[string]string{})
	if resp.Error == "" {
		t.Fatal("expected unknown method error")
	}
}

func BenchmarkServerRoundTrip(b *testing.B) {
	_, addr := startTestServer(b)

	reqMap := map[string]string{
		"file_path": "/some/fake/file.go",
		"agent_id":  "bench-agent",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp := sendTestRequest(b, addr, "acquire-lock", reqMap)
		if resp.Error != "" {
			b.Fatalf("acquire-lock error: %s", resp.Error)
		}

		resp2 := sendTestRequest(b, addr, "release-lock", reqMap)
		if resp2.Error != "" {
			b.Fatalf("release-lock error: %s", resp2.Error)
		}
	}
}

func TestStreamingObservability(t *testing.T) {
	srv, addr := startTestServer(t)

	// Simulate a client dialing to listen to hub events
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	req := Request{
		Method: "tail-hub-events",
		ID:     "stream-1",
	}

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(req); err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Give the server a moment to register the subscription channel
	time.Sleep(50 * time.Millisecond)

	// Fire events on the orchestrator backend
	go func() {
		for i := 0; i < 10; i++ {
			msg := hub.Message{
				ID:      "msg-idx",
				Type:    hub.MsgStubFulfilled,
				Payload: []byte(`{"status":"running"}`),
			}
			srv.BroadcastHubEvent(msg)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Stream reader
	received := 0
	for i := 0; i < 10; i++ {
		var msg hub.Message
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		err := decoder.Decode(&msg)
		if err != nil {
			t.Fatalf("decode stream error at %d: %v", i, err)
		}
		if msg.Type != hub.MsgStubFulfilled {
			t.Errorf("expected agent status, got %s", msg.Type)
		}
		received++
	}

	if received != 10 {
		t.Errorf("expected to receive 10 stream emissions, got %d", received)
	}
}

func TestHubSnapshotCachePersists(t *testing.T) {
	dir := t.TempDir()
	dagMgr, err := dag.NewManager(dag.ManagerConfig{
		StoreFilePath: filepath.Join(dir, "dag.bin"),
		WalFilePath:   filepath.Join(dir, "dag.wal"),
		MaxNodes:      200,
		FlushDeadline: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer dagMgr.Close()

	srv := NewServer(dagMgr, locking.NewManager([]string{"go.mod"}), stub.NewRegistry(), nil)
	srv.SetLogsDir(dir)
	srv.BroadcastHubEvent(hub.Message{
		ID:        "snap-1",
		Type:      hub.MsgStubFulfilled,
		FromAgent: "coordinator",
		Payload:   []byte(`{"status":"ready"}`),
		Timestamp: time.Now(),
	})

	if _, err := os.Stat(filepath.Join(dir, "hub_snapshot.json")); err != nil {
		t.Fatalf("expected snapshot file: %v", err)
	}

	srv2 := NewServer(dagMgr, locking.NewManager([]string{"go.mod"}), stub.NewRegistry(), nil)
	srv2.SetLogsDir(dir)
	snap := srv2.loadHubSnapshot()
	if len(snap) != 1 || snap[0].ID != "snap-1" {
		t.Fatalf("unexpected snapshot contents: %#v", snap)
	}
}

func TestHubSnapshotAndSinceTail(t *testing.T) {
	srv, addr := startTestServer(t)

	oldMsg := hub.Message{
		ID:        "old-msg",
		Type:      hub.MsgStubFulfilled,
		FromAgent: "coordinator",
		Payload:   []byte(`{"status":"old"}`),
		Timestamp: time.Now().Add(-2 * time.Second),
	}
	srv.BroadcastHubEvent(oldMsg)

	snap := sendTestRequest(t, addr, "hub-snapshot", map[string]interface{}{})
	if snap.Error != "" {
		t.Fatalf("hub-snapshot error: %s", snap.Error)
	}
	result, ok := snap.Result.(map[string]interface{})
	if !ok {
		t.Fatalf("unexpected snapshot response: %#v", snap.Result)
	}
	messages, ok := result["messages"].([]interface{})
	if !ok || len(messages) != 1 {
		t.Fatalf("unexpected snapshot messages: %#v", result["messages"])
	}
	asOfRaw, ok := result["as_of"].(string)
	if !ok || asOfRaw == "" {
		t.Fatalf("unexpected snapshot as_of: %#v", result["as_of"])
	}
	asOf, err := time.Parse(time.RFC3339Nano, asOfRaw)
	if err != nil {
		t.Fatalf("parse as_of: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	freshMsg := hub.Message{
		ID:        "fresh-msg",
		Type:      hub.MsgStubFulfilled,
		FromAgent: "coordinator",
		Payload:   []byte(`{"status":"fresh"}`),
		Timestamp: time.Now(),
	}
	srv.BroadcastHubEvent(freshMsg)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial stream: %v", err)
	}
	defer conn.Close()

	req := Request{
		Method: "tail-hub-events",
		ID:     "stream-since-1",
		Params: mustJSON(t, map[string]interface{}{"since": asOf}),
	}
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)
	if err := encoder.Encode(req); err != nil {
		t.Fatalf("encode stream request: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	var got hub.Message
	if err := decoder.Decode(&got); err != nil {
		t.Fatalf("decode stream since: %v", err)
	}
	if got.ID != "fresh-msg" {
		t.Fatalf("expected fresh message, got %#v", got)
	}
}

func mustJSON(t testing.TB, v interface{}) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
