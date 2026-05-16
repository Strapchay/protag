// Package supervisor implements agent process lifecycle management.
// Each Domain Agent gets a Go supervisor process that spawns a Pi Agent
// subprocess in RPC mode, manages its Cgroup, monitors health via heartbeats,
// routes context messages as follow_up RPC calls, and handles crash recovery.
package supervisor
