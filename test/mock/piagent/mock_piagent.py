#!/usr/bin/env python3
"""Mock Pi Agent for deterministic testing.

Speaks the JSON-over-stdin/stdout RPC protocol.
Reads JSON commands from stdin, writes JSON events to stdout.

Usage:
  mock_piagent.py --mode rpc [options]
"""
import sys
import json


def emit(event):
    """Write a JSON event to stdout."""
    sys.stdout.write(json.dumps(event) + "\n")
    sys.stdout.flush()


def main():
    emit({"type": "agent_start"})

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            cmd = json.loads(line)
        except json.JSONDecodeError:
            continue

        cmd_type = cmd.get("type", "")
        message = cmd.get("message", "")

        if cmd_type == "prompt":
            emit({"type": "turn_start"})
            emit({
                "type": "message_update",
                "message": f"Mock response to: {message[:80]}"
            })
            emit({"type": "turn_end"})

        elif cmd_type == "follow_up":
            emit({"type": "turn_start"})
            emit({
                "type": "message_update",
                "message": f"Absorbed follow_up: {message[:80]}"
            })
            emit({"type": "turn_end"})

        elif cmd_type == "steer":
            emit({
                "type": "message_update",
                "message": f"Steered: {message[:80]}"
            })

        elif cmd_type == "abort":
            emit({"type": "agent_end"})
            break

        else:
            emit({
                "type": "message_update",
                "message": f"Unknown command type: {cmd_type}"
            })

    emit({"type": "agent_end"})


if __name__ == "__main__":
    main()
