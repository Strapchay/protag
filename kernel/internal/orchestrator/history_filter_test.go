package orchestrator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"aion-kernel/internal/hub"
)

func writeHistory(t *testing.T, dir string, messages []hub.Message) {
	t.Helper()
	file, err := os.Create(filepath.Join(dir, "hub_history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for _, message := range messages {
		data, err := json.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(append(data, '\n')); err != nil {
			t.Fatal(err)
		}
	}
}

func historyMessage(t *testing.T, from, to, role, content string, at time.Time) hub.Message {
	t.Helper()
	msg, err := hub.NewMessage(hub.MsgContextShare, from, to, map[string]string{
		"type": "text", "role": role, "content": content,
	})
	if err != nil {
		t.Fatal(err)
	}
	msg.Timestamp = at
	return *msg
}

func TestLoadFilteredHubHistory(t *testing.T) {
	dir := t.TempDir()
	base := time.Now().Add(-time.Hour)
	writeHistory(t, dir, []hub.Message{
		historyMessage(t, "orchestrator", "tui", "assistant", "plan", base),
		historyMessage(t, "agent-ledger", "tui", "assistant", "ledger work", base.Add(time.Minute)),
		historyMessage(t, "agent-ledger", "tui", "toolResult", "binary noise", base.Add(2*time.Minute)),
		historyMessage(t, "agent-report", "tui", "assistant", "report work", base.Add(3*time.Minute)),
	})
	server := &Server{logsDir: dir}

	all := server.loadFilteredHubHistory("", false, time.Time{}, 10)
	if len(all) != 4 {
		t.Fatalf("all=%d, want 4", len(all))
	}

	withoutTools := server.loadFilteredHubHistory("", true, time.Time{}, 10)
	if len(withoutTools) != 3 {
		t.Fatalf("without tools=%d, want 3", len(withoutTools))
	}

	limited := server.loadFilteredHubHistory("", true, time.Time{}, 2)
	if len(limited) != 2 || limited[1].FromAgent != "agent-report" {
		t.Fatalf("limited=%#v", limited)
	}

	ledger := server.loadFilteredHubHistory("agent-ledger", false, time.Time{}, 10)
	if len(ledger) != 2 {
		t.Fatalf("ledger=%d, want 2", len(ledger))
	}

	recent := server.loadFilteredHubHistory("", true, base.Add(2*time.Minute+30*time.Second), 10)
	if len(recent) != 1 || recent[0].FromAgent != "agent-report" {
		t.Fatalf("recent=%#v", recent)
	}
}
