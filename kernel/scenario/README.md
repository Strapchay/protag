# Process-Level Scenario Harness

Build tag: `scenario_process`.

This directory contains black-box scenarios that start the real kernel binary,
wait for `.aion/server.json`, and stop the process group during cleanup.

Run with:

```bash
make test-scenarios-process
```

The test is opt-in and skipped unless `AION_PROCESS_SCENARIO=1`. The Make target
builds the kernel binary, sets the binary/config paths, and enables the process
scenario.

This layer is intended for:

- daemon restart and resume,
- CLI preflight/confirm,
- socket and process cleanup,
- real server startup and shutdown.

It is not used by the normal `go test ./...` suite.
