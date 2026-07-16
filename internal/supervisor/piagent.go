package supervisor

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

type readCloserWrapper struct {
	io.Reader
	io.Closer
}

// PiAgentConfig configures a Pi Agent subprocess.
type PiAgentConfig struct {
	// Provider is the LLM provider resolved from a profile codename (e.g., oracle, harbor).
	Provider string
	// Model is the model ID resolved behind the codename.
	Model string
	// SessionDir is the directory for Pi Agent session files.
	SessionDir string
	// ResumeSession tells Pi to reopen the latest session in SessionDir.
	ResumeSession bool
	// WorkingDir is the project root directory.
	WorkingDir string
	// Env specifies additional environment variables (e.g., AION_ORCHESTRATOR_ADDR)
	Env []string
	// SkillPaths are paths to Pi Agent skill directories.
	SkillPaths []string
	// ExtensionPaths are paths to Pi Agent extension files.
	ExtensionPaths []string
	// Binary is the path to the Pi Agent binary (default: "pi").
	Binary string
	// Endpoint is resolved behind the profile for kernel-side routing. Pi
	// provider base URLs are overridden through extensions, not a CLI flag.
	Endpoint string
	// MockMode uses a mock subprocess for testing.
	MockMode bool
	// MockBinary is the path to the mock Pi Agent binary.
	MockBinary string
}

type PiAgentEvent struct {
	Type         string          `json:"type"`
	Message      json.RawMessage `json:"message,omitempty"`
	Data         json.RawMessage `json:"data,omitempty"`
	ErrorMessage string          `json:"errorMessage,omitempty"`
	Attempt      int             `json:"attempt,omitempty"`
	MaxAttempts  int             `json:"maxAttempts,omitempty"`
	ToolCallID   string          `json:"toolCallId,omitempty"`
	ToolName     string          `json:"toolName,omitempty"`
	Args         json.RawMessage `json:"args,omitempty"`
	Result       json.RawMessage `json:"result,omitempty"`
	IsError      bool            `json:"isError,omitempty"`

	// Streaming fields for modern agents
	Delta                 string          `json:"delta,omitempty"`
	AssistantMessageEvent json.RawMessage `json:"assistantMessageEvent,omitempty"`
	Choices               []PiChoice      `json:"choices,omitempty"`
}

type PiChoice struct {
	Index   int       `json:"index"`
	Message PiMessage `json:"message"`
	Delta   PiMessage `json:"delta"`
}

// PiMessageContent represents a single part of a message (text or thinking).
type PiMessageContent struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	Thinking string `json:"thinking,omitempty"`
}

// PiMessage represents a normalized message from the Pi agent.
type PiMessage struct {
	Role       string             `json:"role"`
	Content    []PiMessageContent `json:"content,omitempty"`
	RawContent json.RawMessage    `json:"content_raw,omitempty"` // For internal polymorphic unmarshaling
}

// UnmarshalJSON implements custom unmarshaling for PiMessage to handle string or array 'content'.
func (m *PiMessage) UnmarshalJSON(data []byte) error {
	type Alias PiMessage
	aux := &struct {
		Content json.RawMessage `json:"content"`
		*Alias
	}{
		Alias: (*Alias)(m),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if len(aux.Content) == 0 {
		return nil
	}

	// Try unmarshaling content as string
	var str string
	if err := json.Unmarshal(aux.Content, &str); err == nil {
		m.Content = []PiMessageContent{{Type: "text", Text: str}}
		return nil
	}

	// Try unmarshaling content as array of parts
	var parts []PiMessageContent
	if err := json.Unmarshal(aux.Content, &parts); err == nil {
		m.Content = parts
		return nil
	}

	return nil
}

// FullText returns the concatenated text from all text parts.
func (m *PiMessage) FullText() string {
	var sb strings.Builder
	for _, c := range m.Content {
		if c.Type == "text" {
			sb.WriteString(c.Text)
		}
	}
	return sb.String()
}

// FullThinking returns the concatenated thinking from all thinking parts.
func (m *PiMessage) FullThinking() string {
	var sb strings.Builder
	for _, c := range m.Content {
		if c.Type == "thinking" {
			sb.WriteString(c.Thinking)
		}
	}
	return sb.String()
}

// ParseMessage attempts to unmarshal the event's Message field into a normalized PiMessage.
func (e *PiAgentEvent) ParseMessage() (*PiMessage, error) {
	// Waterfall Level 1: Check 'choices' in the streaming response envelope
	if len(e.Choices) > 0 {
		choice := e.Choices[0]
		// Try choice.message (final)
		if len(choice.Message.Content) > 0 {
			return &choice.Message, nil
		}
		// Try choice.delta (streaming)
		if len(choice.Delta.Content) > 0 {
			if choice.Delta.Role == "" {
				choice.Delta.Role = "assistant"
			}
			return &choice.Delta, nil
		}
	}

	// Waterfall Level 2: Try 'Delta' field (direct streaming text)
	if e.Delta != "" {
		return &PiMessage{
			Role:    "assistant",
			Content: []PiMessageContent{{Type: "text", Text: e.Delta}},
		}, nil
	}

	// Waterfall Level 3: Try 'AssistantMessageEvent' (streaming thinking deltas)
	if len(e.AssistantMessageEvent) > 0 {
		var amEvent struct {
			Type  string `json:"type"`
			Delta string `json:"delta"`
		}
		if err := json.Unmarshal(e.AssistantMessageEvent, &amEvent); err == nil {
			if amEvent.Type == "thinking_delta" || amEvent.Type == "thinking_end" {
				return &PiMessage{
					Role:    "assistant",
					Content: []PiMessageContent{{Type: "thinking", Thinking: amEvent.Delta}},
				}, nil
			}
		}
	}

	// Waterfall Level 4: Try explicit 'Message' field (standard/final)
	if len(e.Message) > 0 {
		var msg PiMessage
		if err := json.Unmarshal(e.Message, &msg); err == nil && (len(msg.Content) > 0 || msg.Role != "") {
			return &msg, nil
		}
	}

	// Waterfall Level 5: Fallback to 'Data' if it contains a simple string
	if len(e.Data) > 0 {
		var str string
		if err := json.Unmarshal(e.Data, &str); err == nil {
			return &PiMessage{
				Role:    "assistant",
				Content: []PiMessageContent{{Type: "text", Text: str}},
			}, nil
		}
	}

	return nil, fmt.Errorf("could not extract message from event type %s", e.Type)
}

// PiAgentProcess manages a running Pi Agent subprocess.
type PiAgentProcess struct {
	mu      sync.Mutex
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	pid     int
	pgid    int
	events  chan PiAgentEvent
	done    chan struct{}
	running bool
}

// SpawnPiAgent starts a new Pi Agent subprocess in RPC mode.
func SpawnPiAgent(config PiAgentConfig) (*PiAgentProcess, error) {
	binary := config.Binary
	if binary == "" {
		binary = "pi"
	}
	if config.MockMode && config.MockBinary != "" {
		binary = config.MockBinary
	}

	args := []string{"--mode", "rpc"}
	if config.Provider != "" {
		args = append(args, "--provider", config.Provider)
	}
	if config.Model != "" {
		args = append(args, "--model", config.Model)
	}
	if config.SessionDir != "" {
		args = append(args, "--session-dir", config.SessionDir)
	}
	if config.ResumeSession {
		args = append(args, "--continue")
	}
	for _, skill := range config.SkillPaths {
		if strings.TrimSpace(skill) == "" {
			continue
		}
		args = append(args, "--skill", skill)
	}
	for _, extension := range config.ExtensionPaths {
		if strings.TrimSpace(extension) == "" {
			continue
		}
		args = append(args, "--extension", resolvePiPath(config.WorkingDir, extension))
	}

	// Setup raw logging for debugging
	var logFile *os.File
	if config.SessionDir != "" {
		_ = os.MkdirAll(config.SessionDir, 0755)
		logPath := filepath.Join(config.SessionDir, "pi_raw.log")
		logFile, _ = os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	}

	cmd := exec.Command(binary, args...)
	cmd.Dir = config.WorkingDir
	cmd.Env = append(os.Environ(), config.Env...)
	if logFile != nil {
		cmd.Stderr = logFile
		_, _ = fmt.Fprintf(logFile, "[aion] pi launch gateway_enabled=%t gateway_url=%s target_provider=%s target_profile=%s target_model=%s pi_provider=%s extensions=%s\n",
			envEnabled(config.Env, "AION_INFERENCE_GATEWAY_ENABLED"),
			redactedEnvValue(config.Env, "AION_INFERENCE_GATEWAY_URL"),
			redactedEnvValue(config.Env, "AION_TARGET_PROVIDER"),
			redactedEnvValue(config.Env, "AION_TARGET_PROFILE"),
			redactedEnvValue(config.Env, "AION_TARGET_MODEL"),
			config.Provider,
			strings.Join(resolvedExtensions(config.WorkingDir, config.ExtensionPaths), ","),
		)
	} else {
		cmd.Stderr = os.Stderr
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("piagent: stdin pipe: %w", err)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("piagent: stdout pipe: %w", err)
	}

	var stdout io.ReadCloser = stdoutPipe
	if logFile != nil {
		// Write everything from stdout to the log file too
		stdout = &readCloserWrapper{
			Reader: io.TeeReader(stdoutPipe, logFile),
			Closer: stdoutPipe,
		}
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("piagent: start: %w", err)
	}

	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		pgid = cmd.Process.Pid // fallback
	}

	p := &PiAgentProcess{
		cmd:     cmd,
		stdin:   stdin,
		stdout:  stdout,
		pid:     cmd.Process.Pid,
		pgid:    pgid,
		events:  make(chan PiAgentEvent, 64),
		done:    make(chan struct{}),
		running: true,
	}

	// Start event reader goroutine
	go p.readEvents()

	// Start wait goroutine
	go p.waitForExit()

	return p, nil
}

func envEnabled(env []string, key string) bool {
	return strings.EqualFold(redactedEnvValue(env, key), "true") || redactedEnvValue(env, key) == "1"
}

func redactedEnvValue(env []string, key string) string {
	prefix := key + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			value := strings.TrimSpace(strings.TrimPrefix(entry, prefix))
			if value == "" {
				return ""
			}
			if strings.Contains(strings.ToLower(key), "key") || strings.Contains(strings.ToLower(key), "token") || strings.Contains(strings.ToLower(key), "secret") {
				return "[redacted]"
			}
			return value
		}
	}
	return ""
}

func resolvedExtensions(workingDir string, extensions []string) []string {
	resolved := make([]string, 0, len(extensions))
	for _, extension := range extensions {
		if strings.TrimSpace(extension) == "" {
			continue
		}
		resolved = append(resolved, resolvePiPath(workingDir, extension))
	}
	return resolved
}

func resolvePiPath(workingDir, path string) string {
	path = strings.TrimSpace(path)
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	candidates := []string{}
	if workingDir != "" {
		candidates = append(candidates, filepath.Join(workingDir, path))
	}
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(cwd, path))
	}
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(exeDir, path),
			filepath.Join(exeDir, "..", path),
		)
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return path
}

// SendPrompt sends an initial prompt to the Pi Agent.
func (p *PiAgentProcess) SendPrompt(message string) error {
	return p.writeJSON(map[string]string{
		"type":    "prompt",
		"message": message,
	})
}

// SendFollowUp sends a follow-up message to the Pi Agent.
func (p *PiAgentProcess) SendFollowUp(message string) error {
	return p.writeJSON(map[string]string{
		"type":    "follow_up",
		"message": message,
	})
}

// SendSteer sends a steer command to redirect the Pi Agent.
func (p *PiAgentProcess) SendSteer(message string) error {
	return p.writeJSON(map[string]string{
		"type":    "steer",
		"message": message,
	})
}

// SendAbort sends an abort command to terminate the Pi Agent's current work.
func (p *PiAgentProcess) SendAbort() error {
	return p.writeJSON(map[string]string{
		"type": "abort",
	})
}

// Events returns a channel that receives Pi Agent events.
func (p *PiAgentProcess) Events() <-chan PiAgentEvent {
	return p.events
}

// Done returns a channel that closes when the process exits.
func (p *PiAgentProcess) Done() <-chan struct{} {
	return p.done
}

// Kill terminates the entire process group.
func (p *PiAgentProcess) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return nil
	}

	// Kill entire process group
	if err := syscall.Kill(-p.pgid, syscall.SIGKILL); err != nil {
		// Try killing just the process
		if p.cmd.Process != nil {
			return p.cmd.Process.Kill()
		}
		return err
	}
	return nil
}

// Wait waits for the process to exit and returns the error.
func (p *PiAgentProcess) Wait() error {
	<-p.done
	return p.cmd.Wait()
}

// IsAlive returns whether the process is still running.
func (p *PiAgentProcess) IsAlive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// PID returns the process ID.
func (p *PiAgentProcess) PID() int {
	return p.pid
}

// PGID returns the process group ID.
func (p *PiAgentProcess) PGID() int {
	return p.pgid
}

func (p *PiAgentProcess) writeJSON(data interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return fmt.Errorf("piagent: process not running")
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("piagent: marshal: %w", err)
	}

	jsonData = append(jsonData, '\n')
	if _, err := p.stdin.Write(jsonData); err != nil {
		return fmt.Errorf("piagent: write: %w", err)
	}

	return nil
}

func (p *PiAgentProcess) readEvents() {
	defer close(p.events)

	scanner := bufio.NewScanner(p.stdout)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB max line

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var event PiAgentEvent
		if err := json.Unmarshal(line, &event); err != nil {
			log.Printf("piagent: malformed event: %v (line: %s)", err, string(line))
			continue
		}

		p.events <- event
	}
}

func (p *PiAgentProcess) waitForExit() {
	p.cmd.Wait()
	p.mu.Lock()
	p.running = false
	p.mu.Unlock()
	close(p.done)
}
