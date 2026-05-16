// Package coordinator implements the Coordinator plugin — a read-only Pi Agent
// that takes a user goal + project state and produces a DAG of tasks + domain
// assignments. The Coordinator never writes code; it only plans.
package coordinator
