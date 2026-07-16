package main

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveDashboardAddrFromProjectServerInfo(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".aion"), 0o755); err != nil {
		t.Fatalf("mkdir .aion: %v", err)
	}

	info := map[string]string{"addr": "127.0.0.1:6123"}
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, ".aion", "server.json"), data, 0o644); err != nil {
		t.Fatalf("write server info: %v", err)
	}

	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldwd)
	})
	if err := os.Chdir(root); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	if got := resolveDashboardAddr(); got != "127.0.0.1:6123" {
		t.Fatalf("resolveDashboardAddr = %q", got)
	}
}

func TestResolveDashboardAddrFallsBackToEnv(t *testing.T) {
	t.Setenv("AION_ORCHESTRATOR_CORE_ADDR", "127.0.0.1:7001")
	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldwd)
	})
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	if got := resolveDashboardAddr(); got != "127.0.0.1:7001" {
		t.Fatalf("resolveDashboardAddr = %q", got)
	}
}

func TestResolveOrchestratorAddrPrefersAgentUnixSocket(t *testing.T) {
	t.Setenv("AION_ORCHESTRATOR_SOCKET", "/run/aion/control.sock")
	t.Setenv("AION_ORCHESTRATOR_ADDR", "127.0.0.1:7001")
	if got := resolveOrchestratorAddr(); got != "unix:///run/aion/control.sock" {
		t.Fatalf("resolveOrchestratorAddr = %q", got)
	}
}

func TestSendRequestUsesUnixSocket(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "control.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer conn.Close()
		var request Request
		if decodeErr := json.NewDecoder(conn).Decode(&request); decodeErr != nil {
			serverDone <- decodeErr
			return
		}
		serverDone <- json.NewEncoder(conn).Encode(Response{ID: request.ID, Result: json.RawMessage(`{"ok":true}`)})
	}()
	response, err := sendRequest("unix://"+socket, Request{ID: "unix-test", Method: "heartbeat"})
	if err != nil {
		t.Fatal(err)
	}
	if response.ID != "unix-test" || !strings.Contains(string(response.Result), `"ok":true`) {
		t.Fatalf("response = %#v", response)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestParseDebugStatusCommand(t *testing.T) {
	params, err := parseCommand("debug-status", nil)
	if err != nil {
		t.Fatalf("parse debug-status: %v", err)
	}
	if len(params) != 0 {
		t.Fatalf("debug-status params = %#v", params)
	}
}

func TestParseGatewayCapacityCommand(t *testing.T) {
	params, err := parseCommand("set-gateway-capacity", []string{"--capacity", "3"})
	if err != nil {
		t.Fatalf("parse gateway capacity: %v", err)
	}
	if params["capacity"] != 3 {
		t.Fatalf("capacity params = %#v", params)
	}
	if _, err := parseCommand("set-gateway-capacity", []string{"--capacity", "0"}); err == nil {
		t.Fatal("expected invalid capacity error")
	}
}

func TestParseUpdateNodeUsesAgentIdentity(t *testing.T) {
	t.Setenv("AION_AGENT_ID", "agent-env")
	params, err := parseCommand("update-node", []string{"--node-id", "node-1", "--status", "Done"})
	if err != nil {
		t.Fatalf("parse update-node: %v", err)
	}
	if params["agent_id"] != "agent-env" {
		t.Fatalf("agent identity = %#v", params["agent_id"])
	}

	params, err = parseCommand("update-node", []string{
		"--node-id", "node-1",
		"--status", "Done",
		"--agent-id", "agent-flag",
	})
	if err != nil {
		t.Fatalf("parse update-node with flag: %v", err)
	}
	if params["agent_id"] != "agent-flag" {
		t.Fatalf("flag agent identity = %#v", params["agent_id"])
	}
}

func TestParseUpdateNodeRequiresAgentIdentity(t *testing.T) {
	t.Setenv("AION_AGENT_ID", "")
	_, err := parseCommand("update-node", []string{"--node-id", "node-1", "--status", "Done"})
	if err == nil || !strings.Contains(err.Error(), "AION_AGENT_ID") {
		t.Fatalf("missing identity error = %v", err)
	}
}

func TestResolveDebugStatusAddrFromProjectServerInfo(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".aion"), 0o755); err != nil {
		t.Fatalf("mkdir .aion: %v", err)
	}
	data, err := json.Marshal(map[string]string{"addr": "127.0.0.1:6124"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, ".aion", "server.json"), data, 0o644); err != nil {
		t.Fatalf("write server info: %v", err)
	}

	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldwd)
	})
	if err := os.Chdir(root); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	if got := resolveCommandAddr("debug-status"); got != "127.0.0.1:6124" {
		t.Fatalf("resolveCommandAddr(debug-status) = %q", got)
	}
}

func TestPrintDebugStatusPretty(t *testing.T) {
	raw := json.RawMessage(`{
		"addr":"127.0.0.1:50051",
		"build_spec_status":"active",
		"dag_nodes":3,
		"inference_gateway":{"enabled":true,"queued":1}
	}`)

	output := captureStdout(t, func() {
		if err := printDebugStatus(raw, true, ""); err != nil {
			t.Fatalf("print debug status: %v", err)
		}
	})

	if !strings.Contains(output, "build_spec_status: active") {
		t.Fatalf("pretty output missing build spec status:\n%s", output)
	}
	if !strings.Contains(output, "inference_gateway:") || !strings.Contains(output, `"queued": 1`) {
		t.Fatalf("pretty output missing nested gateway JSON:\n%s", output)
	}
}

func TestPrintDebugStatusKey(t *testing.T) {
	raw := json.RawMessage(`{"build_spec_status":"active","dag_nodes":3}`)

	output := captureStdout(t, func() {
		if err := printDebugStatus(raw, true, "build_spec_status"); err != nil {
			t.Fatalf("print debug key: %v", err)
		}
	})

	if strings.TrimSpace(output) != "build_spec_status: active" {
		t.Fatalf("debug key output = %q", output)
	}
}

func TestPrintDebugStatusPrettyFormatsJSONStringValue(t *testing.T) {
	raw := json.RawMessage(`{
		"build_spec_status":"{\"status\":\"active\",\"plan\":{\"domains\":[{\"domain_id\":\"storage\"}]}}"
	}`)

	output := captureStdout(t, func() {
		if err := printDebugStatus(raw, true, "build_spec_status"); err != nil {
			t.Fatalf("print debug key: %v", err)
		}
	})

	if !strings.Contains(output, "build_spec_status:") {
		t.Fatalf("debug key output missing label:\n%s", output)
	}
	if !strings.Contains(output, `"status": "active"`) || !strings.Contains(output, `"domain_id": "storage"`) {
		t.Fatalf("debug key output did not format JSON string:\n%s", output)
	}
}

func TestFormatJSONStringRejectsPlainText(t *testing.T) {
	if formatted, ok := formatJSONString("active", "  "); ok || formatted != "" {
		t.Fatalf("plain text formatted as JSON: ok=%v formatted=%q", ok, formatted)
	}
}

func TestPrintDebugStatusMissingKey(t *testing.T) {
	err := printDebugStatus(json.RawMessage(`{"build_spec_status":"active"}`), false, "missing")
	if err == nil || !strings.Contains(err.Error(), `key "missing" not found`) {
		t.Fatalf("missing key error = %v", err)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = write
	t.Cleanup(func() {
		os.Stdout = old
	})

	fn()
	if err := write.Close(); err != nil {
		t.Fatalf("close write pipe: %v", err)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(read); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	return buf.String()
}
