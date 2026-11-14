package orchestrator

import (
	"testing"
	"time"
)

func TestAgentActivitySuppressesStaleWarnings(t *testing.T) {
	now := time.Now()
	old := now.Add(-10 * time.Minute)
	fresh := now.Add(-10 * time.Second)
	window := 2 * time.Minute

	cases := []struct {
		name          string
		info          AgentInfo
		gatewayActive bool
		want          bool
	}{
		{
			name: "active turn",
			info: AgentInfo{InTurn: true, LastProgress: old, LastHeartbeat: old},
			want: true,
		},
		{
			name: "active tool",
			info: AgentInfo{ToolActive: true, LastProgress: old, LastHeartbeat: old},
			want: true,
		},
		{
			name:          "active gateway request",
			info:          AgentInfo{LastProgress: old, LastHeartbeat: old},
			gatewayActive: true,
			want:          true,
		},
		{
			name: "fresh progress",
			info: AgentInfo{LastProgress: fresh, LastHeartbeat: old},
			want: true,
		},
		{
			name: "fresh heartbeat",
			info: AgentInfo{LastProgress: old, LastHeartbeat: fresh},
			want: true,
		},
		{
			name: "genuinely idle",
			info: AgentInfo{LastProgress: old, LastHeartbeat: old},
			want: false,
		},
		{
			name: "no activity timestamps",
			info: AgentInfo{},
			want: false,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := agentActivitySuppressesStale(testCase.info, testCase.gatewayActive, window, now)
			if got != testCase.want {
				t.Fatalf("suppress=%t, want %t", got, testCase.want)
			}
		})
	}
}
