package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	isolation "aion-isolation"
)

func main() {
	policyPath := flag.String("policy", "", "path to an isolation policy JSON file")
	runtimeBase := flag.String("runtime-base", "", "host directory for ephemeral isolation metadata")
	flag.Parse()
	if *policyPath == "" || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: isolation-harness --policy policy.json [--runtime-base dir] -- /absolute/sandbox/command [args...]")
		os.Exit(2)
	}

	data, err := os.ReadFile(*policyPath)
	if err != nil {
		fatal(err)
	}
	var policy isolation.Policy
	if err := json.Unmarshal(data, &policy); err != nil {
		fatal(fmt.Errorf("parse policy: %w", err))
	}
	engine, err := isolation.NewBubblewrapEngine(isolation.BubblewrapConfig{RuntimeBase: *runtimeBase})
	if err != nil {
		fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	workspace, err := engine.Prepare(ctx, policy)
	if err != nil {
		fatal(err)
	}
	if err := runCommand(ctx, workspace, flag.Args()); err != nil {
		fatal(err)
	}
}

func runCommand(ctx context.Context, workspace isolation.Workspace, command []string) (runErr error) {
	defer func() {
		if err := workspace.Close(); runErr == nil && err != nil {
			runErr = err
		}
	}()
	snapshot, _ := json.Marshal(workspace.Snapshot())
	fmt.Fprintf(os.Stderr, "isolation-harness: prepared %s\n", snapshot)
	cmd, err := workspace.Command(ctx, isolation.CommandSpec{Path: command[0], Args: command[1:]})
	if err != nil {
		return err
	}
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd.Run()
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "isolation-harness: %v\n", err)
	os.Exit(1)
}
