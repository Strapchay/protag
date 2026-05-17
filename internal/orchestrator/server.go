package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"aion-kernel/internal/dag"
	"aion-kernel/internal/hub"
	"aion-kernel/internal/locking"
	"aion-kernel/internal/memory"
	"aion-kernel/internal/stub"
)

// Request represents a JSON-RPC-style request from orchestrator-cli.
type Request struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	ID     string          `json:"id"`
}

// Response is the JSON-RPC-style response sent back to the CLI.
type Response struct {
	ID     string      `json:"id"`
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// Server is the Orchestrator's IPC server. All agent interactions with the
// kernel go through this server via orchestrator-cli.
type Server struct {
	mu           sync.RWMutex
	dagManager   *dag.Manager
	lockManager  *locking.Manager
	stubRegistry *stub.Registry
	memoryStore  memory.Store

	hubCallback       func(hub.Message) // called when hub messages need routing
	replanCb          func()
	reviveCb          func(string) error
	buildSpecCb       func(string) // called when user issues /build-spec
	refineCb          func(string) // called when user sends message to orchestrator
	retryCb           func() error
	continueCb        func() error
	resumeCb          func() error
	statusCb          func() string
	showSpecCb        func() (string, error)
	resetCb           func() error
	buildSpecStatusCb func() string
	buildSpecPlanCb   func() (string, error)
	buildSpecTraceCb  func() (string, error)
	buildSpecCancelCb func() error

	listener   net.Listener
	heartbeats map[string]int64 // agentID → last heartbeat unix ms
	hubSubs    map[chan hub.Message]struct{}
	hubHistory []hub.Message
}

// NewServer creates an Orchestrator server with the given subsystems.
func NewServer(
	dagManager *dag.Manager,
	lockManager *locking.Manager,
	stubRegistry *stub.Registry,
	memoryStore memory.Store,
) *Server {
	return &Server{
		dagManager:   dagManager,
		lockManager:  lockManager,
		stubRegistry: stubRegistry,
		memoryStore:  memoryStore,
		heartbeats:   make(map[string]int64),
		hubSubs:      make(map[chan hub.Message]struct{}),
	}
}

// SetHubCallback sets the function called when hub messages need routing.
func (s *Server) SetHubCallback(cb func(hub.Message)) {
	s.hubCallback = cb
}

func (s *Server) SetReplanCallback(cb func()) {
	s.replanCb = cb
}

func (s *Server) SetReviveCallback(cb func(string) error) {
	s.reviveCb = cb
}

func (s *Server) SetBuildSpecCallback(cb func(string)) {
	s.buildSpecCb = cb
}

func (s *Server) BroadcastHubEvent(msg hub.Message) {
	s.mu.Lock()
	s.hubHistory = append(s.hubHistory, msg)
	if len(s.hubHistory) > 500 {
		s.hubHistory = s.hubHistory[len(s.hubHistory)-500:]
	}
	subs := make([]chan hub.Message, 0, len(s.hubSubs))
	for ch := range s.hubSubs {
		subs = append(subs, ch)
	}
	s.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- msg:
		default: // non-blocking, drop if slow
		}
	}
}

// BroadcastStatus emits a SystemStatus message to all TUI subscribers.
// level is one of "info", "warn", "error", "ok".
func (s *Server) BroadcastStatus(text, level string) {
	s.BroadcastAgentStatus("orchestrator", text, level)
}

func (s *Server) BroadcastAgentStatus(agentID, text, level string) {
	payload, _ := json.Marshal(hub.SystemStatusPayload{Text: text, Level: level})
	s.BroadcastHubEvent(hub.Message{
		ID:        fmt.Sprintf("sys-%d", time.Now().UnixNano()),
		Type:      hub.MsgSystemStatus,
		FromAgent: agentID,
		Payload:   payload,
		Timestamp: time.Now(),
	})
}

// ListenAndServe starts the TCP server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("server: listen: %w", err)
	}
	s.listener = ln
	log.Printf("orchestrator: listening on %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("orchestrator: accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

// Stop stops the server.
func (s *Server) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// Addr returns the listener address, or empty if not started.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}

func (s *Server) debugLog(dir, text string) {
	// Filter out high-frequency DAG polling to keep the log readable
	if strings.Contains(text, "read-dag") || strings.Contains(text, "dag-tick") {
		return
	}
	f, _ := os.OpenFile("rpc_debug.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if f != nil {
		defer f.Close()
		fmt.Fprintf(f, "[%s] %s %s\n", time.Now().Format(time.Kitchen), dir, text)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	var req Request
	if err := decoder.Decode(&req); err != nil {
		if err != io.EOF {
			s.debugLog("ERR", fmt.Sprintf("decode failed: %v", err))
			encoder.Encode(Response{Error: fmt.Sprintf("invalid request: %v", err)})
		}
		return
	}

	reqBytes, _ := json.Marshal(req)
	s.debugLog("REQ", string(reqBytes))

	if req.Method == "tail-hub-events" {
		s.handleTailHubEvents(req, encoder, conn)
		return
	}

	if req.Method == "tail-agent-logs" {
		s.handleTailAgentLogs(req, encoder, conn)
		return
	}

	resp := s.handleRequest(req)
	respBytes, _ := json.Marshal(resp)
	s.debugLog("RES", string(respBytes))
	encoder.Encode(resp)
}

func (s *Server) handleTailHubEvents(req Request, encoder *json.Encoder, conn net.Conn) {
	ch := make(chan hub.Message, 100)
	s.mu.Lock()
	s.hubSubs[ch] = struct{}{}
	history := append([]hub.Message(nil), s.hubHistory...)
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.hubSubs, ch)
		s.mu.Unlock()
	}()

	for _, msg := range history {
		if err := encoder.Encode(msg); err != nil {
			return
		}
	}

	for {
		msg, ok := <-ch
		if !ok {
			return
		}
		if err := encoder.Encode(msg); err != nil {
			return // connection closed or error
		}
		b, _ := json.Marshal(msg)
		s.debugLog("HUB", string(b))
	}
}

func (s *Server) handleTailAgentLogs(req Request, encoder *json.Encoder, conn net.Conn) {
	var params struct {
		AgentID string `json:"agent_id"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		encoder.Encode(Response{Error: "invalid params for tail-agent-logs"})
		return
	}

	ch := make(chan hub.Message, 100)
	s.mu.Lock()
	s.hubSubs[ch] = struct{}{}
	history := append([]hub.Message(nil), s.hubHistory...)
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.hubSubs, ch)
		s.mu.Unlock()
	}()

	for _, msg := range history {
		if msg.FromAgent == params.AgentID || msg.ToAgent == params.AgentID {
			if err := encoder.Encode(msg); err != nil {
				return
			}
		}
	}

	for {
		msg, ok := <-ch
		if !ok {
			return
		}
		if msg.FromAgent == params.AgentID || msg.ToAgent == params.AgentID {
			if err := encoder.Encode(msg); err != nil {
				return // connection closed or error
			}
			b, _ := json.Marshal(msg)
			s.debugLog("LOG", string(b))
		}
	}
}

func (s *Server) handleRequest(req Request) Response {
	switch req.Method {
	case "acquire-lock":
		return s.handleAcquireLock(req)
	case "release-lock":
		return s.handleReleaseLock(req)
	case "update-node":
		return s.handleUpdateNode(req)
	case "create-stub":
		return s.handleCreateStub(req)
	case "inject-edge":
		return s.handleInjectEdge(req)
	case "split-node":
		return s.handleSplitNode(req)
	case "read-dag":
		return s.handleReadDag(req)
	case "heartbeat":
		return s.handleHeartbeat(req)
	case "validate-write":
		return s.handleValidateWrite(req)
	case "query-memory":
		return s.handleQueryMemory(req)
	case "send-message":
		return s.handleSendMessage(req)
	case "architect-status":
		return s.handleArchitectStatus(req)
	case "architect-retry":
		return s.handleArchitectRetry(req)
	case "architect-continue":
		return s.handleArchitectContinue(req)
	case "architect-resume":
		return s.handleArchitectResume(req)
	case "architect-show-spec":
		return s.handleArchitectShowSpec(req)
	case "architect-reset":
		return s.handleArchitectReset(req)
	case "build-spec-status":
		return s.handleBuildSpecStatus(req)
	case "build-spec-show-plan":
		return s.handleBuildSpecShowPlan(req)
	case "build-spec-show-trace":
		return s.handleBuildSpecShowTrace(req)
	case "build-spec-cancel":
		return s.handleBuildSpecCancel(req)
	case "trigger-replan":
		return s.handleTriggerReplan(req)
	case "revive-agent":
		return s.handleReviveAgent(req)
	default:
		return Response{ID: req.ID, Error: fmt.Sprintf("unknown method: %s", req.Method)}
	}
}

func (s *Server) SetRefineCallback(cb func(string)) {
	s.refineCb = cb
}

func (s *Server) SetArchitectRetryCallback(cb func() error) {
	s.retryCb = cb
}

func (s *Server) SetArchitectContinueCallback(cb func() error) {
	s.continueCb = cb
}

func (s *Server) SetArchitectResumeCallback(cb func() error) {
	s.resumeCb = cb
}

func (s *Server) SetArchitectStatusCallback(cb func() string) {
	s.statusCb = cb
}

func (s *Server) SetArchitectShowSpecCallback(cb func() (string, error)) {
	s.showSpecCb = cb
}

func (s *Server) SetArchitectResetCallback(cb func() error) {
	s.resetCb = cb
}

func (s *Server) SetBuildSpecStatusCallback(cb func() string) {
	s.buildSpecStatusCb = cb
}

func (s *Server) SetBuildSpecPlanCallback(cb func() (string, error)) {
	s.buildSpecPlanCb = cb
}

func (s *Server) SetBuildSpecTraceCallback(cb func() (string, error)) {
	s.buildSpecTraceCb = cb
}

func (s *Server) SetBuildSpecCancelCallback(cb func() error) {
	s.buildSpecCancelCb = cb
}

func (s *Server) SetRuntimeSubsystems(dagManager *dag.Manager, lockManager *locking.Manager, stubRegistry *stub.Registry, memoryStore memory.Store) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dagManager = dagManager
	s.lockManager = lockManager
	s.stubRegistry = stubRegistry
	s.memoryStore = memoryStore
}

func (s *Server) ClearHubHistory() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hubHistory = nil
}

func (s *Server) handleSendMessage(req Request) Response {
	var params struct {
		AgentID string `json:"agent_id"`
		Text    string `json:"text"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}

	// Intercept /build-spec command if addressed to orchestrator
	if params.AgentID == "orchestrator" {
		const prefix = "/build-spec"
		if strings.HasPrefix(params.Text, prefix) {
			if s.buildSpecCb != nil {
				log.Printf("server: user triggered /build-spec")
				go s.buildSpecCb(params.Text)
				return Response{ID: req.ID, Result: map[string]string{"status": "build_initiated"}}
			}
		}

		// Otherwise, it's a refinement message
		if s.refineCb != nil {
			log.Printf("server: user message sent to architect: %s", params.Text)
			go s.refineCb(params.Text)
			return Response{ID: req.ID, Result: map[string]string{"status": "refinement_received"}}
		}
	}

	if s.hubCallback != nil {
		log.Printf("server: user intervention sent to agent %s: %s", params.AgentID, params.Text)
		msg, _ := hub.NewMessage(hub.MsgContextShare, "user", params.AgentID, params.Text)
		s.hubCallback(*msg)
	}
	return Response{ID: req.ID, Result: map[string]string{"status": "sent"}}
}

func (s *Server) handleArchitectStatus(req Request) Response {
	if s.statusCb == nil {
		return Response{ID: req.ID, Error: "architect status callback not configured"}
	}
	return Response{ID: req.ID, Result: map[string]string{"status": s.statusCb()}}
}

func (s *Server) handleArchitectRetry(req Request) Response {
	if s.retryCb == nil {
		return Response{ID: req.ID, Error: "architect retry callback not configured"}
	}
	if err := s.retryCb(); err != nil {
		s.BroadcastStatus("Architect retry failed: "+err.Error(), "error")
		return Response{ID: req.ID, Error: err.Error()}
	}
	s.BroadcastStatus("Retrying last Architect request...", "info")
	return Response{ID: req.ID, Result: map[string]string{"status": "retry_started"}}
}

func (s *Server) handleArchitectContinue(req Request) Response {
	if s.continueCb == nil {
		return Response{ID: req.ID, Error: "architect continue callback not configured"}
	}
	if err := s.continueCb(); err != nil {
		s.BroadcastStatus("Architect continue failed: "+err.Error(), "error")
		return Response{ID: req.ID, Error: err.Error()}
	}
	s.BroadcastStatus("Continuing Architect session...", "info")
	return Response{ID: req.ID, Result: map[string]string{"status": "continue_started"}}
}

func (s *Server) handleArchitectResume(req Request) Response {
	if s.resumeCb == nil {
		return Response{ID: req.ID, Error: "architect resume callback not configured"}
	}
	if err := s.resumeCb(); err != nil {
		s.BroadcastStatus("Architect resume failed: "+err.Error(), "error")
		return Response{ID: req.ID, Error: err.Error()}
	}
	s.BroadcastStatus("Reconciling Architect session context...", "info")
	return Response{ID: req.ID, Result: map[string]string{"status": "resume_started"}}
}

func (s *Server) handleArchitectShowSpec(req Request) Response {
	if s.showSpecCb == nil {
		return Response{ID: req.ID, Error: "architect show-spec callback not configured"}
	}
	text, err := s.showSpecCb()
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	return Response{ID: req.ID, Result: map[string]string{"spec": text}}
}

func (s *Server) handleArchitectReset(req Request) Response {
	if s.resetCb == nil {
		return Response{ID: req.ID, Error: "architect reset callback not configured"}
	}
	if err := s.resetCb(); err != nil {
		s.BroadcastStatus("Architect reset failed: "+err.Error(), "error")
		return Response{ID: req.ID, Error: err.Error()}
	}
	s.BroadcastStatus("Architect session reset; fresh session started.", "ok")
	return Response{ID: req.ID, Result: map[string]string{"status": "reset_complete"}}
}

func (s *Server) handleBuildSpecStatus(req Request) Response {
	if s.buildSpecStatusCb == nil {
		return Response{ID: req.ID, Error: "build-spec status callback not configured"}
	}
	return Response{ID: req.ID, Result: map[string]string{"status": s.buildSpecStatusCb()}}
}

func (s *Server) handleBuildSpecShowPlan(req Request) Response {
	if s.buildSpecPlanCb == nil {
		return Response{ID: req.ID, Error: "build-spec show-plan callback not configured"}
	}
	text, err := s.buildSpecPlanCb()
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	return Response{ID: req.ID, Result: map[string]string{"plan": text}}
}

func (s *Server) handleBuildSpecShowTrace(req Request) Response {
	if s.buildSpecTraceCb == nil {
		return Response{ID: req.ID, Error: "build-spec show-trace callback not configured"}
	}
	text, err := s.buildSpecTraceCb()
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}
	return Response{ID: req.ID, Result: map[string]string{"trace": text}}
}

func (s *Server) handleBuildSpecCancel(req Request) Response {
	if s.buildSpecCancelCb == nil {
		return Response{ID: req.ID, Error: "build-spec cancel callback not configured"}
	}
	if err := s.buildSpecCancelCb(); err != nil {
		s.BroadcastStatus("Build-spec cancel failed: "+err.Error(), "error")
		return Response{ID: req.ID, Error: err.Error()}
	}
	return Response{ID: req.ID, Result: map[string]string{"status": "cancel_started"}}
}

func (s *Server) handleTriggerReplan(req Request) Response {
	log.Printf("server: user manually triggered DAG replan")
	if s.replanCb != nil {
		s.replanCb()
	}
	return Response{ID: req.ID, Result: []byte(`{"status":"replan_triggered"}`)}
}

func (s *Server) handleReviveAgent(req Request) Response {
	var params struct {
		AgentID string `json:"agent_id"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}

	if s.reviveCb != nil {
		log.Printf("server: user manually reviving agent %s", params.AgentID)
		if err := s.reviveCb(params.AgentID); err != nil {
			return Response{ID: req.ID, Error: err.Error()}
		}
	}
	return Response{ID: req.ID, Result: []byte(`{"status":"reviving"}`)}
}

func (s *Server) handleAcquireLock(req Request) Response {
	var params struct {
		File    string `json:"file"`
		AgentID string `json:"agent_id"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}

	// Extract assigned boundaries from DAG
	snapshot := s.dagManager.Snapshot()
	var assignedPaths []string
	for _, n := range snapshot.Nodes {
		if n.AssignedAgent == params.AgentID && !n.Status.IsTerminal() {
			assignedPaths = append(assignedPaths, n.TargetFiles...)
		}
	}

	if err := s.lockManager.Acquire(params.File, params.AgentID, assignedPaths); err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}

	return Response{ID: req.ID, Result: map[string]interface{}{
		"status": "acquired",
		"file":   params.File,
	}}
}

func (s *Server) handleReleaseLock(req Request) Response {
	var params struct {
		File    string `json:"file"`
		AgentID string `json:"agent_id"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}

	if err := s.lockManager.Release(params.File, params.AgentID); err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}

	return Response{ID: req.ID, Result: map[string]string{"status": "released"}}
}

func (s *Server) handleUpdateNode(req Request) Response {
	var params struct {
		NodeID           string `json:"node_id"`
		Status           string `json:"status"`
		StartedAt        int64  `json:"started_at,omitempty"`
		CompletedAt      int64  `json:"completed_at,omitempty"`
		PromptTokens     int32  `json:"prompt_tokens,omitempty"`
		CompletionTokens int32  `json:"completion_tokens,omitempty"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}

	status := parseNodeStatus(params.Status)

	var err error
	if params.StartedAt > 0 || params.PromptTokens > 0 {
		metrics := &dag.NodeMetrics{
			StartedAt:        params.StartedAt,
			CompletedAt:      params.CompletedAt,
			PromptTokens:     params.PromptTokens,
			CompletionTokens: params.CompletionTokens,
		}
		err = s.dagManager.UpdateNodeWithMetrics(params.NodeID, status, metrics)
	} else {
		err = s.dagManager.UpdateNode(params.NodeID, status)
	}

	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}

	if status == dag.StatusDone {
		// Check for unfulfilled stubs produced by this node/agent
		pending := s.stubRegistry.GetPending(params.NodeID)
		for _, stub := range pending {
			// Mark fulfilled
			if _, err := s.stubRegistry.Fulfill(stub.ID); err != nil {
				log.Printf("server: failed to fulfill stub %s: %v", stub.ID, err)
				continue
			}

			// Route message via Hub
			if s.hubCallback != nil {
				payload, _ := json.Marshal(stub)
				s.hubCallback(hub.Message{
					Type:      hub.MsgStubFulfilled,
					FromAgent: stub.ProducerID,
					ToAgent:   stub.ConsumerID,
					Payload:   payload,
				})
			}
		}

		// Fire-and-forget semantic memory writing if memory store is enabled
		if s.memoryStore != nil {
			node, err := s.dagManager.GetNode(params.NodeID)
			if err == nil && node.TaskSpec != "" {
				// Use the natural string TaskSpec as the text for dense embedding
				entry := memory.MemoryEntry{
					ID:        node.ID,
					Text:      fmt.Sprintf("Task: %s\nSpec: %s", node.ID, node.TaskSpec),
					AgentID:   node.AssignedAgent,
					TaskID:    node.ID,
					ProjectID: "default", // hardcoded default project for now
					Timestamp: time.Now().Unix(),
				}

				// Execute embedding/storage asynchronously so orchestrator thread isn't blocked
				go func() {
					// Use a short context for the write block
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()

					if wErr := s.memoryStore.Write(ctx, entry); wErr != nil {
						log.Printf("memory: failed to write node %s: %v", node.ID, wErr)
					}
				}()
			}
		}
	}

	return Response{ID: req.ID, Result: map[string]string{
		"status":  "updated",
		"node_id": params.NodeID,
	}}
}

func (s *Server) handleCreateStub(req Request) Response {
	var params struct {
		Contract stub.Contract `json:"contract"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}

	if err := s.stubRegistry.Register(params.Contract); err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}

	// Add stub edge to DAG
	edge := dag.DagEdge{
		FromNode: params.Contract.ProducerID,
		ToNode:   params.Contract.ConsumerID,
		Type:     dag.EdgeStubContract,
		StubID:   params.Contract.ID,
	}
	// Best-effort edge addition (nodes might not match DAG node IDs)
	s.dagManager.AddEdge(edge)

	return Response{ID: req.ID, Result: map[string]string{
		"status":  "created",
		"stub_id": params.Contract.ID,
	}}
}

func (s *Server) handleQueryMemory(req Request) Response {
	var params struct {
		Text string `json:"text"`
		TopK int    `json:"top_k"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}

	if s.memoryStore == nil {
		return Response{ID: req.ID, Error: "semantic memory is disabled in configuration"}
	}

	topK := params.TopK
	if topK <= 0 {
		topK = 5
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, err := s.memoryStore.Query(ctx, params.Text, topK)
	if err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}

	return Response{ID: req.ID, Result: results}
}

func (s *Server) handleInjectEdge(req Request) Response {
	var params struct {
		From string `json:"from"`
		To   string `json:"to"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}

	edge := dag.DagEdge{
		FromNode: params.From,
		ToNode:   params.To,
		Type:     dag.EdgeDependency,
	}

	if err := s.dagManager.AddEdge(edge); err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}

	payload, _ := json.Marshal(edge)
	if s.hubCallback != nil {
		s.hubCallback(hub.Message{
			Type:    hub.MsgDependencyDiscovered,
			Payload: payload,
		})
	}

	return Response{ID: req.ID, Result: map[string]string{"status": "injected"}}
}

func (s *Server) handleSplitNode(req Request) Response {
	var params struct {
		NodeID   string        `json:"node_id"`
		SubNodes []dag.DagNode `json:"into"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}

	if err := s.dagManager.SplitNode(params.NodeID, params.SubNodes); err != nil {
		return Response{ID: req.ID, Error: err.Error()}
	}

	return Response{ID: req.ID, Result: map[string]string{
		"status":    "split",
		"parent_id": params.NodeID,
	}}
}

func (s *Server) handleReadDag(req Request) Response {
	var params struct {
		NodeID string `json:"node_id"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}

	if params.NodeID != "" {
		node, err := s.dagManager.GetNode(params.NodeID)
		if err != nil {
			return Response{ID: req.ID, Error: err.Error()}
		}
		return Response{ID: req.ID, Result: node}
	}

	// Return full DAG snapshot
	snapshot := s.dagManager.Snapshot()
	return Response{ID: req.ID, Result: snapshot}
}

func (s *Server) handleHeartbeat(req Request) Response {
	var params struct {
		AgentID string `json:"agent_id"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}

	s.mu.Lock()
	s.heartbeats[params.AgentID] = currentTimeMs()
	s.mu.Unlock()

	return Response{ID: req.ID, Result: map[string]string{"status": "ok"}}
}

func (s *Server) handleValidateWrite(req Request) Response {
	var params struct {
		File string `json:"file"`
		ETag string `json:"etag"` // SHA-256 hash of file when agent started
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return Response{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
	}

	// Compute current hash
	currentHash, err := hashFile(params.File)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist — valid for new file creation
			return Response{ID: req.ID, Result: map[string]interface{}{
				"valid":    true,
				"new_file": true,
			}}
		}
		return Response{ID: req.ID, Error: fmt.Sprintf("hash file: %v", err)}
	}

	valid := currentHash == params.ETag
	return Response{ID: req.ID, Result: map[string]interface{}{
		"valid":        valid,
		"current_etag": currentHash,
	}}
}

// GetLastHeartbeat returns the last heartbeat time for an agent.
func (s *Server) GetLastHeartbeat(agentID string) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ts, ok := s.heartbeats[agentID]
	return ts, ok
}

func parseNodeStatus(s string) dag.NodeStatus {
	switch s {
	case "Pending":
		return dag.StatusPending
	case "InProgress":
		return dag.StatusInProgress
	case "Done":
		return dag.StatusDone
	case "Failed":
		return dag.StatusFailed
	case "Blocked":
		return dag.StatusBlocked
	default:
		return dag.StatusPending
	}
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func currentTimeMs() int64 {
	return time.Now().UnixMilli()
}
