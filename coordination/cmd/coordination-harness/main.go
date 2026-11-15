package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	coordination "aion-coordination"
)

func main() {
	stateDir := flag.String("state-dir", "", "directory for the coordination store")
	flag.Parse()
	if *stateDir == "" {
		fmt.Fprintln(os.Stderr, "coordination-harness requires --state-dir")
		os.Exit(2)
	}
	manager, err := coordination.Open(coordination.Options{StateDir: *stateDir})
	if err != nil {
		fail(err)
	}
	defer manager.Close()

	ctx := context.Background()
	consumer := coordination.Principal{Role: coordination.RoleAgent, AgentID: "agent-consumer", NodeIDs: []string{"node-consumer"}}
	producer := coordination.Principal{Role: coordination.RoleAgent, AgentID: "agent-producer", NodeIDs: []string{"node-producer"}}
	requested := coordination.Contract{
		ID:              "harness-contract",
		ProducerAgentID: producer.AgentID,
		ProducerNodeID:  "node-producer",
		ConsumerAgentID: consumer.AgentID,
		ConsumerNodeID:  "node-consumer",
		Details: coordination.ContractDetails{
			Name:       "HarnessBoundary",
			Kind:       "schema",
			Acceptance: []string{"consumer accepts the published fixture"},
		},
	}
	if _, err := manager.Request(ctx, consumer, coordination.RequestCommand{CommandID: "harness-request", Contract: requested}); err != nil {
		fail(err)
	}
	contract, err := manager.Get(ctx, consumer, requested.ID)
	if err != nil {
		fail(err)
	}
	if contract.Status == coordination.StatusRequested {
		contract, err = manager.Publish(ctx, producer, coordination.PublishCommand{
			CommandID:        "harness-publish-1",
			ContractID:       contract.ID,
			ExpectedRevision: 0,
			Content:          []byte(`{"version":1,"value":"harness"}`),
			MediaType:        "application/json",
		})
		if err != nil {
			fail(err)
		}
	}
	if contract.Status == coordination.StatusProduced {
		contract, err = manager.Validate(ctx, consumer, coordination.ValidateCommand{
			CommandID:  "harness-validate-1",
			ContractID: contract.ID,
			Revision:   contract.Revision,
			Evidence:   json.RawMessage(`{"harness":"pass"}`),
		})
		if err != nil {
			fail(err)
		}
	}
	if contract.Status != coordination.StatusFulfilled {
		fail(fmt.Errorf("harness contract ended in %s", contract.Status))
	}
	snapshot, err := manager.Snapshot(ctx)
	if err != nil {
		fail(err)
	}
	output, _ := json.MarshalIndent(snapshot, "", "  ")
	fmt.Println(string(output))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
