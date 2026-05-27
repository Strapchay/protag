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
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	ID     string          `json:"id"`
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

	paramsJSON, _ := json.Marshal(params)

	req := Request{
		Method: command,
		Params: paramsJSON,
		ID:     "cli-1",
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

	// Output result as JSON
	output, _ := json.MarshalIndent(json.RawMessage(resp.Result), "", "  ")
	fmt.Println(string(output))
}

func resolveCommandAddr(command string) string {
	switch command {
	case "debug-status":
		return resolveDashboardAddr()
	default:
		return resolveOrchestratorAddr()
	}
}

func sendRequest(addr string, req Request) (*Response, error) {
	conn, err := net.Dial("tcp", addr)
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

		res := map[string]interface{}{"node_id": nodeID, "status": status}

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

	case "create-stub":
		contractJSON := flags["contract"]
		if contractJSON == "" {
			return nil, fmt.Errorf("create-stub requires --contract '<json>'")
		}
		var contract interface{}
		if err := json.Unmarshal([]byte(contractJSON), &contract); err != nil {
			return nil, fmt.Errorf("invalid contract JSON: %w", err)
		}
		return map[string]interface{}{"contract": contract}, nil

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

	default:
		return nil, fmt.Errorf("unknown command: %s", command)
	}
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
  update-node     --node-id <uuid> --status <Pending|InProgress|Done|Failed>
  create-stub     --contract '<json>'
  inject-edge     --from <node> --to <node>
  split-node      --node-id <uuid> --into '<json>'
  read-dag        [--node-id <uuid>]
  heartbeat       --agent-id <uuid>
  query-memory    --text <query> [--top-k N]
  debug-status    Print server/run/hub/DAG/agent diagnostics
  build-progress  Print current build-spec progress projection
  dashboard       Launch real-time monitoring TUI [--verbose]

Environment:
  AION_ORCHESTRATOR_ADDR   Orchestrator address
  AION_ORCHESTRATOR_CORE_ADDR  Core orchestrator address codename
  AION_AGENT_ID            Default agent ID for commands requiring --agent-id`)
}
