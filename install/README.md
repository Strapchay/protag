# Install Artifacts

This directory contains the install-time artifacts for the Aion Kernel
deployment path that uses systemd and a delegated cgroup subtree.

Files:

- `aion-kernel.service.tmpl`: systemd service template
- `install.sh`: helper script that installs the unit and prepares the cgroup root

The runtime binary still reads `aion.yaml` on startup. The unit only provides
the process envelope, restart behavior, and cgroup delegation.
