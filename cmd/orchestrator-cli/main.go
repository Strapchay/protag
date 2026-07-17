package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"aion-kernel/cmd/orchestrator-cli/dashboard"
	tea "github.com/charmbracelet/bubbletea"
)

// Request matches the server's expected format.
type Request struct {
	Method     string          `json:"method"`
	Params     json.RawMessage `json:"params"`
	ID         string          `json:"id"`
	Capability string          `json:"capability,omitempty"`
}

// Response matches the server's response format.
type Response struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	if command == "--help" || command == "-h" {
		printUsage()
		os.Exit(0)
	}

	// Parse flags for the command
	if command == "dashboard" {
		verbose := hasFlag(os.Args[2:], "--verbose") || hasFlag(os.Args[2:], "-v")
		addr := resolveDashboardAddr()
		if addr == "" {
			exitError("dashboard requires a running project server or AION_ORCHESTRATOR_ADDR/AION_ORCHESTRATOR_CORE_ADDR")
		}
		p := tea.NewProgram(dashboard.NewModelWithOptions(addr, dashboard.Options{Verbose: verbose}), tea.WithMouseCellMotion())
		if _, err := p.Run(); err != nil {
			fmt.Printf("Error running dashboard: %v", err)
			os.Exit(1)
		}
		return
	}

	params, err := parseCommand(command, os.Args[2:])
	if err != nil {
		exitError(err.Error())
	}

	if command == "write-spec" {
		output, _ := json.MarshalIndent(params, "", "  ")
		fmt.Println(string(output))
		return
	}
	debugPretty := command == "debug-status" && (hasFlag(os.Args[2:], "--pretty") || hasFlag(os.Args[2:], "-p"))
	debugKey := ""
	if command == "debug-status" {
		debugKey = parseFlags(os.Args[2:])["key"]
	}

	paramsJSON, _ := json.Marshal(params)

	req := Request{
		Method:     command,
		Params:     paramsJSON,
		ID:         "cli-1",
		Capability: strings.TrimSpace(os.Getenv("AION_AGENT_CAPABILITY")),
	}

	// Connect to orchestrator
	addr := resolveCommandAddr(command)
	if addr == "" {
		exitError("connection error: no orchestrator address resolved from project server info or env")
	}

	resp, err := sendRequest(addr, req)
	if err != nil {
		exitError(fmt.Sprintf("connection error: %v", err))
	}

	if resp.Error != "" {
		exitError(resp.Error)
	}

	if command == "debug-status" && (debugPretty || debugKey != "") {
		if err := printDebugStatus(resp.Result, debugPretty, debugKey); err != nil {
			exitError(err.Error())
		}
		return
	}
	if command == "contract-fetch" {
		outputPath := parseFlags(os.Args[2:])["output"]
		if err := writeFetchedContract(resp.Result, outputPath); err != nil {
			exitError(err.Error())
		}
		return
	}

	// Output result as JSON.
	output, _ := json.MarshalIndent(json.RawMessage(resp.Result), "", "  ")
	fmt.Println(string(output))
}

func resolveCommandAddr(command string) string {
	switch command {
	case "debug-status", "set-gateway-capacity", "stop-agents":
		return resolveDashboardAddr()
	default:
		return resolveOrchestratorAddr()
	}
}

func sendRequest(addr string, req Request) (*Response, error) {
	network := "tcp"
	dialAddr := addr
	if strings.HasPrefix(addr, "unix://") {
		network = "unix"
		dialAddr = strings.TrimPrefix(addr, "unix://")
	}
	conn, err := net.Dial(network, dialAddr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(req); err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}

	var resp Response
	if err := decoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &resp, nil
}

func resolveOrchestratorAddr() string {
	if socket := strings.TrimSpace(os.Getenv("AION_ORCHESTRATOR_SOCKET")); socket != "" {
		return "unix://" + socket
	}
	if addr := os.Getenv("AION_ORCHESTRATOR_ADDR"); addr != "" {
		return addr
	}
	if addr := os.Getenv("AION_ORCHESTRATOR_CORE_ADDR"); addr != "" {
		return addr
	}
	if addr := os.Getenv("AION_ORCHESTRATOR_LISTEN_ADDR"); addr != "" {
		return addr
	}
	host := strings.TrimSpace(os.Getenv("AION_ORCHESTRATOR_HOST"))
	port := strings.TrimSpace(os.Getenv("AION_ORCHESTRATOR_PORT"))
	if host != "" && port != "" {
		return host + ":" + port
	}
	return ""
}

func resolveDashboardAddr() string {
	if addr := resolveProjectServerAddr(); addr != "" {
		return addr
	}
	return resolveOrchestratorAddr()
}

func resolveProjectServerAddr() string {
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}
	path := filepath.Join(cwd, ".aion", "server.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var info struct {
		Addr string `json:"addr"`
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return ""
	}
	return strings.TrimSpace(info.Addr)
}

func parseCommand(command string, args []string) (map[string]interface{}, error) {
	flags := parseFlags(args)

	switch command {
	case "acquire-lock":
		file, ok := flags["file"]
		if !ok {
			return nil, fmt.Errorf("acquire-lock requires --file <path>")
		}
		agentID := flags["agent-id"]
		if agentID == "" {
			agentID = os.Getenv("AION_AGENT_ID")
		}
		if agentID == "" {
			return nil, fmt.Errorf("acquire-lock requires --agent-id <uuid> or AION_AGENT_ID env")
		}
		return map[string]interface{}{"file": file, "agent_id": agentID}, nil

	case "release-lock":
		file, ok := flags["file"]
		if !ok {
			return nil, fmt.Errorf("release-lock requires --file <path>")
		}
		agentID := flags["agent-id"]
		if agentID == "" {
			agentID = os.Getenv("AION_AGENT_ID")
		}
		if agentID == "" {
			return nil, fmt.Errorf("release-lock requires --agent-id <uuid> or AION_AGENT_ID env")
		}
		return map[string]interface{}{"file": file, "agent_id": agentID}, nil

	case "update-node":
		nodeID := flags["node-id"]
		status := flags["status"]
		if nodeID == "" || status == "" {
			return nil, fmt.Errorf("update-node requires --node-id <uuid> --status <status>")
		}
		agentID := flags["agent-id"]
		if agentID == "" {
			agentID = os.Getenv("AION_AGENT_ID")
		}
		if agentID == "" {
			return nil, fmt.Errorf("update-node requires --agent-id <uuid> or AION_AGENT_ID env")
		}

		res := map[string]interface{}{"node_id": nodeID, "status": status, "agent_id": agentID}

		if val, ok := flags["started-at"]; ok {
			var m int64
			fmt.Sscanf(val, "%d", &m)
			res["started_at"] = m
		}
		if val, ok := flags["completed-at"]; ok {
			var m int64
			fmt.Sscanf(val, "%d", &m)
			res["completed_at"] = m
		}
		if val, ok := flags["prompt-tokens"]; ok {
			var m int32
			fmt.Sscanf(val, "%d", &m)
			res["prompt_tokens"] = m
		}
		if val, ok := flags["completion-tokens"]; ok {
			var m int32
			fmt.Sscanf(val, "%d", &m)
			res["completion_tokens"] = m
		}

		return res, nil

	case "contract-request":
		contractJSON := flags["contract"]
		if contractJSON == "" {
			return nil, fmt.Errorf("%s requires --contract '<json>'", command)
		}
		var contract interface{}
		if err := json.Unmarshal([]byte(contractJSON), &contract); err != nil {
			return nil, fmt.Errorf("invalid contract JSON: %w", err)
		}
		result := map[string]interface{}{"contract": contract}
		if commandID := flags["command-id"]; commandID != "" {
			result["command_id"] = commandID
		}
		return result, nil

	case "contract-publish":
		contractID := flags["contract-id"]
		artifactFile := flags["artifact-file"]
		if contractID == "" || artifactFile == "" {
			return nil, fmt.Errorf("contract-publish requires --contract-id <id> --artifact-file <path>")
		}
		content, err := os.ReadFile(artifactFile)
		if err != nil {
			return nil, fmt.Errorf("read contract artifact: %w", err)
		}
		expectedRevision := int64(0)
		if value := flags["expected-revision"]; value != "" {
			if _, err := fmt.Sscanf(value, "%d", &expectedRevision); err != nil || expectedRevision < 0 {
				return nil, fmt.Errorf("contract-publish --expected-revision must be a non-negative integer")
			}
		}
		result := map[string]interface{}{
			"contract_id":       contractID,
			"expected_revision": expectedRevision,
			"content":           content,
			"media_type":        flags["media-type"],
		}
		if evidence := flags["evidence"]; evidence != "" {
			var decoded interface{}
			if err := json.Unmarshal([]byte(evidence), &decoded); err != nil {
				return nil, fmt.Errorf("invalid evidence JSON: %w", err)
			}
			result["evidence"] = decoded
		}
		if commandID := flags["command-id"]; commandID != "" {
			result["command_id"] = commandID
		}
		return result, nil

	case "contract-fetch":
		contractID := flags["contract-id"]
		if contractID == "" || flags["output"] == "" {
			return nil, fmt.Errorf("contract-fetch requires --contract-id <id> --output <path>")
		}
		revision := int64(0)
		if value := flags["revision"]; value != "" {
			if _, err := fmt.Sscanf(value, "%d", &revision); err != nil || revision < 1 {
				return nil, fmt.Errorf("contract-fetch --revision must be a positive integer")
			}
		}
		return map[string]interface{}{"contract_id": contractID, "revision": revision}, nil

	case "contract-validate":
		contractID := flags["contract-id"]
		revision, err := requiredPositiveRevision(flags, "contract-validate")
		if err != nil || contractID == "" {
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("contract-validate requires --contract-id <id> --revision <N>")
		}
		result := map[string]interface{}{"contract_id": contractID, "revision": revision}
		if evidence := flags["evidence"]; evidence != "" {
			var decoded interface{}
			if err := json.Unmarshal([]byte(evidence), &decoded); err != nil {
				return nil, fmt.Errorf("invalid evidence JSON: %w", err)
			}
			result["evidence"] = decoded
		}
		if commandID := flags["command-id"]; commandID != "" {
			result["command_id"] = commandID
		}
		return result, nil

	case "contract-correct":
		contractID := flags["contract-id"]
		revision, err := requiredPositiveRevision(flags, "contract-correct")
		if err != nil {
			return nil, err
		}
		if contractID == "" || flags["reason"] == "" {
			return nil, fmt.Errorf("contract-correct requires --contract-id <id> --revision <N> --reason <text>")
		}
		result := map[string]interface{}{"contract_id": contractID, "revision": revision, "reason": flags["reason"]}
		if commandID := flags["command-id"]; commandID != "" {
			result["command_id"] = commandID
		}
		return result, nil

	case "contract-wait":
		contractID := flags["contract-id"]
		if contractID == "" {
			return nil, fmt.Errorf("contract-wait requires --contract-id <id> [--clear]")
		}
		result := map[string]interface{}{"contract_id": contractID, "hard": flags["clear"] == ""}
		if commandID := flags["command-id"]; commandID != "" {
			result["command_id"] = commandID
		}
		return result, nil

	case "contract-cancel", "contract-fail":
		contractID := flags["contract-id"]
		if contractID == "" {
			return nil, fmt.Errorf("%s requires --contract-id <id> [--reason <text>]", command)
		}
		result := map[string]interface{}{"contract_id": contractID, "reason": flags["reason"]}
		if commandID := flags["command-id"]; commandID != "" {
			result["command_id"] = commandID
		}
		return result, nil

	case "contract-status":
		if flags["contract-id"] == "" {
			return nil, fmt.Errorf("contract-status requires --contract-id <id>")
		}
		return map[string]interface{}{"contract_id": flags["contract-id"]}, nil

	case "contract-list":
		return map[string]interface{}{}, nil

	case "inject-edge":
		from := flags["from"]
		to := flags["to"]
		if from == "" || to == "" {
			return nil, fmt.Errorf("inject-edge requires --from <node> --to <node>")
		}
		return map[string]interface{}{"from": from, "to": to}, nil

	case "split-node":
		nodeID := flags["node-id"]
		intoJSON := flags["into"]
		if nodeID == "" || intoJSON == "" {
			return nil, fmt.Errorf("split-node requires --node-id <uuid> --into '<json>'")
		}
		var into interface{}
		if err := json.Unmarshal([]byte(intoJSON), &into); err != nil {
			return nil, fmt.Errorf("invalid split JSON: %w", err)
		}
		return map[string]interface{}{"node_id": nodeID, "into": into}, nil

	case "read-dag":
		nodeID := flags["node-id"]
		return map[string]interface{}{"node_id": nodeID}, nil

	case "heartbeat":
		agentID := flags["agent-id"]
		if agentID == "" {
			agentID = os.Getenv("AION_AGENT_ID")
		}
		if agentID == "" {
			return nil, fmt.Errorf("heartbeat requires --agent-id <uuid> or AION_AGENT_ID env")
		}
		return map[string]interface{}{"agent_id": agentID}, nil

	case "query-memory":
		text := flags["text"]
		if text == "" {
			return nil, fmt.Errorf("query-memory requires --text <query>")
		}
		var topK int
		if val, ok := flags["top-k"]; ok {
			fmt.Sscanf(val, "%d", &topK)
		}
		return map[string]interface{}{"text": text, "top_k": topK}, nil

	case "write-spec":
		content, ok := flags["content"]
		if !ok {
			return nil, fmt.Errorf("write-spec requires --content <markdown>")
		}
		dest := "docs/build_spec.md"
		if customDest, ok := flags["file"]; ok {
			dest = customDest
		}

		if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}
		if err := os.WriteFile(dest, []byte(content), 0644); err != nil {
			return nil, fmt.Errorf("failed to write spec: %w", err)
		}
		return map[string]interface{}{"status": "written", "file": dest}, nil

	case "send-message":
		agentID := flags["agent-id"]
		if agentID == "" {
			return nil, fmt.Errorf("send-message requires --agent-id <id>")
		}
		text := flags["text"]
		if text == "" {
			return nil, fmt.Errorf("send-message requires --text <message>")
		}
		return map[string]interface{}{"agent_id": agentID, "text": text}, nil

	case "debug-status":
		return map[string]interface{}{}, nil
	case "build-progress":
		return map[string]interface{}{}, nil
	case "stop-agents":
		return map[string]interface{}{}, nil
	case "set-gateway-capacity":
		capacity := 0
		if _, err := fmt.Sscanf(flags["capacity"], "%d", &capacity); err != nil || capacity < 1 {
			return nil, fmt.Errorf("set-gateway-capacity requires --capacity <positive integer>")
		}
		return map[string]interface{}{"capacity": capacity}, nil

	default:
		return nil, fmt.Errorf("unknown command: %s", command)
	}
}

func requiredPositiveRevision(flags map[string]string, command string) (int64, error) {
	var revision int64
	if _, err := fmt.Sscanf(flags["revision"], "%d", &revision); err != nil || revision < 1 {
		return 0, fmt.Errorf("%s requires --revision <positive integer>", command)
	}
	return revision, nil
}

func writeFetchedContract(raw json.RawMessage, outputPath string) error {
	if strings.TrimSpace(outputPath) == "" {
		return fmt.Errorf("contract-fetch requires --output <path>")
	}
	var result struct {
		Contract interface{} `json:"contract"`
		Content  []byte      `json:"content"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return fmt.Errorf("decode fetched contract: %w", err)
	}
	if len(result.Content) == 0 {
		return fmt.Errorf("fetched contract artifact is empty")
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("create artifact output directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(outputPath), ".contract-*")
	if err != nil {
		return fmt.Errorf("create temporary artifact: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(result.Content); err != nil {
		tmp.Close()
		return fmt.Errorf("write artifact: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync artifact: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close artifact: %w", err)
	}
	if err := os.Rename(tmpPath, outputPath); err != nil {
		return fmt.Errorf("publish artifact: %w", err)
	}
	output, _ := json.MarshalIndent(map[string]interface{}{"status": "fetched", "file": outputPath, "contract": result.Contract}, "", "  ")
	fmt.Println(string(output))
	return nil
}

func printDebugStatus(raw json.RawMessage, pretty bool, key string) error {
	var result map[string]interface{}
	if err := json.Unmarshal(raw, &result); err != nil {
		return fmt.Errorf("decode debug status: %w", err)
	}
	if key != "" {
		value, ok := result[key]
		if !ok {
			return fmt.Errorf("debug-status key %q not found", key)
		}
		if pretty {
			fmt.Printf("%s: %s\n", key, formatDebugValue(value, "  "))
			return nil
		}
		output, _ := json.MarshalIndent(value, "", "  ")
		fmt.Println(string(output))
		return nil
	}
	if !pretty {
		output, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(output))
		return nil
	}

	order := []string{
		"addr",
		"log_level",
		"logs_dir",
		"rpc_debug_log",
		"build_spec_status",
		"dag_nodes",
		"dag_edges",
		"hub_history_count",
		"hub_snapshot_count",
		"hub_subscribers",
		"heartbeat_count",
		"last_hub_event_at",
		"inference_gateway",
		"coordination",
		"agents",
	}
	printed := map[string]bool{}
	for _, name := range order {
		value, ok := result[name]
		if !ok {
			continue
		}
		printed[name] = true
		fmt.Printf("%s: %s\n", name, formatDebugValue(value, "  "))
	}
	for name, value := range result {
		if printed[name] {
			continue
		}
		fmt.Printf("%s: %s\n", name, formatDebugValue(value, "  "))
	}
	return nil
}

func formatDebugValue(value interface{}, indent string) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case string:
		if v == "" {
			return `""`
		}
		if formatted, ok := formatJSONString(v, indent); ok {
			return formatted
		}
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%v", v)
	default:
		output, err := json.MarshalIndent(value, indent, "  ")
		if err != nil {
			return fmt.Sprintf("%v", value)
		}
		text := string(output)
		if strings.Contains(text, "\n") {
			return "\n" + text
		}
		return text
	}
}

func formatJSONString(value, indent string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "[") {
		return "", false
	}
	var decoded interface{}
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
		return "", false
	}
	output, err := json.MarshalIndent(decoded, indent, "  ")
	if err != nil {
		return "", false
	}
	return "\n" + string(output), true
}

func hasFlag(args []string, names ...string) bool {
	for _, arg := range args {
		for _, name := range names {
			if arg == name {
				return true
			}
		}
	}
	return false
}

func parseFlags(args []string) map[string]string {
	flags := make(map[string]string)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--") {
			key := strings.TrimPrefix(arg, "--")
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
				flags[key] = args[i+1]
				i++
			} else {
				flags[key] = "true"
			}
		}
	}
	return flags
}

func exitError(msg string) {
	errResp, _ := json.Marshal(map[string]string{"error": msg})
	fmt.Fprintln(os.Stderr, string(errResp))
	os.Exit(1)
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `Usage: orchestrator-cli <command> [flags]

Commands:
  acquire-lock    --file <path> --agent-id <uuid>
  release-lock    --file <path> --agent-id <uuid>
	  update-node     --node-id <uuid> --status <Pending|InProgress|Done|Failed> [--agent-id <uuid>]
	contract-request  --contract '<json>'
	contract-publish  --contract-id <id> --artifact-file <path> [--expected-revision N]
	contract-fetch    --contract-id <id> [--revision N] --output <path>
	contract-validate --contract-id <id> --revision <N> [--evidence '<json>']
	contract-correct  --contract-id <id> --revision <N> --reason <text>
	contract-wait     --contract-id <id> [--clear]
	contract-cancel   --contract-id <id> [--reason <text>]
	contract-fail     --contract-id <id> [--reason <text>]
	contract-status   --contract-id <id>
	contract-list
  inject-edge     --from <node> --to <node>
  split-node      --node-id <uuid> --into '<json>'
  read-dag        [--node-id <uuid>]
  heartbeat       --agent-id <uuid>
  query-memory    --text <query> [--top-k N]
  debug-status    Print server/run/hub/DAG/agent diagnostics [--pretty|-p] [--key <field>]
  build-progress  Print current build-spec progress projection
  set-gateway-capacity --capacity <N>  Change runtime inference concurrency
  stop-agents     Pause active domain agents; resume with /continue-agents
  dashboard       Launch real-time monitoring TUI [--verbose]

Environment:
	  AION_ORCHESTRATOR_SOCKET Agent-only orchestrator Unix socket
	  AION_ORCHESTRATOR_ADDR   Orchestrator address
	  AION_ORCHESTRATOR_CORE_ADDR  Core orchestrator address codename
	  AION_AGENT_ID            Default agent ID for commands requiring --agent-id
	  AION_AGENT_CAPABILITY    Runtime-issued credential used automatically for authorized agent mutations`)
}
