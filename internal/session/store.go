package session

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var ErrNotFound = errors.New("session: not found")

type Store interface {
	CreateSession(sess *AgentSession) error
	LoadActiveSession(role AgentRole, agentID, scope string) (*AgentSession, error)
	SaveSession(sess *AgentSession) error
	AppendEvent(event SessionEvent) error
	ListRecentEvents(sessionID string, limit int) ([]SessionEvent, error)
	SaveCheckpoint(checkpoint SessionCheckpoint) error
	LoadLatestCheckpoint(sessionID string) (*SessionCheckpoint, error)
	SaveRequest(req AgentRequest) error
	LoadLastRequest(sessionID string) (*AgentRequest, error)
	SaveAttempt(attempt AgentRequestAttempt) error
	LoadLastAttempt(requestID string) (*AgentRequestAttempt, error)
	MarkSessionInactive(sessionID string) error
}

type FileStore struct {
	root string
}

func NewFileStore(root string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Join(root, "sessions"), 0755); err != nil {
		return nil, fmt.Errorf("session store: create root: %w", err)
	}
	return &FileStore{root: root}, nil
}

func (s *FileStore) CreateSession(sess *AgentSession) error {
	if sess == nil {
		return fmt.Errorf("session store: nil session")
	}
	if sess.Metadata == nil {
		sess.Metadata = map[string]interface{}{}
	}
	if err := sess.Validate(); err != nil {
		return fmt.Errorf("session store: invalid session: %w", err)
	}
	if err := os.MkdirAll(s.sessionDir(sess.SessionID), 0755); err != nil {
		return err
	}
	if err := s.SaveSession(sess); err != nil {
		return err
	}
	return s.writeActive(sess)
}

func (s *FileStore) LoadActiveSession(role AgentRole, agentID, scope string) (*AgentSession, error) {
	activePath := filepath.Join(s.root, "active", activeFileName(role, agentID, scope))
	data, err := os.ReadFile(activePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	sessionID := strings.TrimSpace(string(data))
	if sessionID == "" {
		return nil, ErrNotFound
	}
	return s.loadSession(sessionID)
}

func (s *FileStore) SaveSession(sess *AgentSession) error {
	if sess == nil {
		return fmt.Errorf("session store: nil session")
	}
	if err := sess.Validate(); err != nil {
		return fmt.Errorf("session store: invalid session: %w", err)
	}
	if err := os.MkdirAll(s.sessionDir(sess.SessionID), 0755); err != nil {
		return err
	}
	if err := writeJSONAtomic(filepath.Join(s.sessionDir(sess.SessionID), "session.json"), sess); err != nil {
		return err
	}
	return s.writeActive(sess)
}

func (s *FileStore) AppendEvent(event SessionEvent) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("session store: invalid event: %w", err)
	}
	if err := os.MkdirAll(s.sessionDir(event.SessionID), 0755); err != nil {
		return err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(s.sessionDir(event.SessionID), "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func (s *FileStore) ListRecentEvents(sessionID string, limit int) ([]SessionEvent, error) {
	f, err := os.Open(filepath.Join(s.sessionDir(sessionID), "events.jsonl"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var events []SessionEvent
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		var event SessionEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("session store: decode event: %w", err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if limit > 0 && len(events) > limit {
		events = events[len(events)-limit:]
	}
	return events, nil
}

func (s *FileStore) SaveCheckpoint(checkpoint SessionCheckpoint) error {
	if checkpoint.SessionID == "" {
		return fmt.Errorf("session store: checkpoint missing session id")
	}
	dir := filepath.Join(s.sessionDir(checkpoint.SessionID), "checkpoints")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	if err := writeJSONAtomic(filepath.Join(dir, checkpoint.CheckpointID+".json"), checkpoint); err != nil {
		return err
	}
	return writeJSONAtomic(filepath.Join(s.sessionDir(checkpoint.SessionID), "latest_checkpoint.json"), checkpoint)
}

func (s *FileStore) LoadLatestCheckpoint(sessionID string) (*SessionCheckpoint, error) {
	var checkpoint SessionCheckpoint
	if err := readJSON(filepath.Join(s.sessionDir(sessionID), "latest_checkpoint.json"), &checkpoint); err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &checkpoint, nil
}

func (s *FileStore) SaveRequest(req AgentRequest) error {
	if req.SessionID == "" || req.RequestID == "" {
		return fmt.Errorf("session store: request missing id")
	}
	dir := filepath.Join(s.sessionDir(req.SessionID), "requests")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	if err := writeJSONAtomic(filepath.Join(dir, req.RequestID+".json"), req); err != nil {
		return err
	}
	return writeJSONAtomic(filepath.Join(s.sessionDir(req.SessionID), "last_request.json"), req)
}

func (s *FileStore) LoadLastRequest(sessionID string) (*AgentRequest, error) {
	var req AgentRequest
	if err := readJSON(filepath.Join(s.sessionDir(sessionID), "last_request.json"), &req); err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &req, nil
}

func (s *FileStore) SaveAttempt(attempt AgentRequestAttempt) error {
	if attempt.RequestID == "" || attempt.AttemptID == "" {
		return fmt.Errorf("session store: attempt missing id")
	}
	req, err := s.findRequest(attempt.RequestID)
	if err != nil {
		return err
	}
	dir := filepath.Join(s.sessionDir(req.SessionID), "attempts", attempt.RequestID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	if err := writeJSONAtomic(filepath.Join(dir, attempt.AttemptID+".json"), attempt); err != nil {
		return err
	}
	return writeJSONAtomic(filepath.Join(dir, "last_attempt.json"), attempt)
}

func (s *FileStore) LoadLastAttempt(requestID string) (*AgentRequestAttempt, error) {
	req, err := s.findRequest(requestID)
	if err != nil {
		return nil, err
	}
	var attempt AgentRequestAttempt
	path := filepath.Join(s.sessionDir(req.SessionID), "attempts", requestID, "last_attempt.json")
	if err := readJSON(path, &attempt); err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &attempt, nil
}

func (s *FileStore) MarkSessionInactive(sessionID string) error {
	sess, err := s.loadSession(sessionID)
	if err != nil {
		return err
	}
	activePath := filepath.Join(s.root, "active", activeFileName(sess.Role, sess.AgentID, sess.Scope))
	if err := os.Remove(activePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *FileStore) loadSession(sessionID string) (*AgentSession, error) {
	var sess AgentSession
	if err := readJSON(filepath.Join(s.sessionDir(sessionID), "session.json"), &sess); err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &sess, nil
}

func (s *FileStore) writeActive(sess *AgentSession) error {
	dir := filepath.Join(s.root, "active")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(dir, activeFileName(sess.Role, sess.AgentID, sess.Scope)), []byte(sess.SessionID+"\n"))
}

func (s *FileStore) findRequest(requestID string) (*AgentRequest, error) {
	sessionDirs, err := filepath.Glob(filepath.Join(s.root, "sessions", "*"))
	if err != nil {
		return nil, err
	}
	sort.Strings(sessionDirs)
	for _, dir := range sessionDirs {
		var req AgentRequest
		path := filepath.Join(dir, "requests", requestID+".json")
		if err := readJSON(path, &req); err == nil {
			return &req, nil
		}
	}
	return nil, ErrNotFound
}

func (s *FileStore) sessionDir(sessionID string) string {
	return filepath.Join(s.root, "sessions", sessionID)
}

func activeFileName(role AgentRole, agentID, scope string) string {
	name := string(role) + "__" + agentID + "__" + scope
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return replacer.Replace(name) + ".active"
}

func writeJSONAtomic(path string, value interface{}) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeFileAtomic(path, data)
}

func readJSON(path string, value interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
