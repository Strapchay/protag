// Package stub implements stub contract management for inter-agent coordination.
// When an agent needs a resource from another domain that doesn't exist yet, it
// creates a stub contract describing the expected interface. The producing agent
// fulfills the stub, and the consumer is notified via follow_up.
package stub
