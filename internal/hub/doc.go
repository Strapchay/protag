// Package hub implements the Context Hub — the typed message routing layer
// of the Aion-Kernel. The Hub routes context messages (contract transitions,
// correction requests, context shares) from the Orchestrator to the correct
// agent supervisor, which queues an active follow-up or starts an idle prompt.
package hub
