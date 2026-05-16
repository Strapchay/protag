// Package hub implements the Context Hub — the typed message routing layer
// of the Aion-Kernel. The Hub routes context messages (stub fulfillments,
// correction requests, context shares) from the Orchestrator to the correct
// agent supervisor, which forwards them as follow_up RPC messages to Pi Agent.
package hub
