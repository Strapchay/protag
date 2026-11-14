package supervisor

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParseStreamMessagePrefersFullSnapshotOverThinkingDelta(t *testing.T) {
	event := PiAgentEvent{
		Type:    "message_update",
		Message: json.RawMessage(`{"role":"assistant","content":[{"type":"thinking","thinking":"full thought"},{"type":"text","text":"full text"}]}`),
		AssistantMessageEvent: json.RawMessage(
			`{"type":"thinking_delta","delta":"full thought"}`,
		),
	}

	message, mode, err := event.ParseStreamMessage()
	if err != nil {
		t.Fatalf("ParseStreamMessage: %v", err)
	}
	if mode != PiStreamSnapshot {
		t.Fatalf("mode=%q, want snapshot", mode)
	}
	if got := message.FullThinking(); got != "full thought" {
		t.Fatalf("thinking=%q", got)
	}
	if got := message.FullText(); got != "full text" {
		t.Fatalf("text=%q", got)
	}
}

func TestParseStreamMessageClassifiesThinkingDelta(t *testing.T) {
	event := PiAgentEvent{
		Type: "thinking",
		AssistantMessageEvent: json.RawMessage(
			`{"type":"thinking_delta","delta":"fragment"}`,
		),
	}

	message, mode, err := event.ParseStreamMessage()
	if err != nil {
		t.Fatalf("ParseStreamMessage: %v", err)
	}
	if mode != PiStreamDelta {
		t.Fatalf("mode=%q, want delta", mode)
	}
	if got := message.FullThinking(); got != "fragment" {
		t.Fatalf("thinking=%q", got)
	}
}

func TestAgentStreamStateAggregatesDeltasAndReplacesSnapshots(t *testing.T) {
	base := time.Unix(0, 0)
	var state agentStreamState

	out, emit := state.observe(PiStreamDelta, "a", base, false)
	if !emit || out != "a" {
		t.Fatalf("first delta emit=%t value=%q", emit, out)
	}

	out, emit = state.observe(PiStreamDelta, "b", base.Add(10*time.Millisecond), false)
	if emit {
		t.Fatalf("throttled delta emitted %q", out)
	}

	out, emit = state.observe(
		PiStreamDelta, "c", base.Add(streamEmitMinInterval+time.Millisecond), false,
	)
	if !emit || out != "abc" {
		t.Fatalf("interval delta emit=%t value=%q", emit, out)
	}

	out, emit = state.observe(
		PiStreamSnapshot, "abcd", base.Add(streamEmitMinInterval+2*time.Millisecond), true,
	)
	if !emit || out != "abcd" {
		t.Fatalf("forced snapshot emit=%t value=%q", emit, out)
	}

	out, emit = state.observe(
		PiStreamSnapshot, "ab", base.Add(streamEmitMinInterval+3*time.Millisecond), true,
	)
	if emit {
		t.Fatalf("stale snapshot emitted %q", out)
	}
}

func TestAgentStreamStateSnapshotReplacesUnrelatedContent(t *testing.T) {
	base := time.Unix(0, 0)
	var state agentStreamState

	if _, emit := state.observe(PiStreamDelta, "old", base, true); !emit {
		t.Fatal("expected initial delta to emit")
	}
	out, emit := state.observe(
		PiStreamSnapshot, "new content", base.Add(time.Second), false,
	)
	if !emit || out != "new content" {
		t.Fatalf("replacement snapshot emit=%t value=%q", emit, out)
	}
}
