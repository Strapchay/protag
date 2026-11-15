# Install Artifacts

This directory contains the install-time artifacts for the Aion Kernel
deployment path that uses systemd and a delegated cgroup subtree.

Files:

- `aion-kernel.service.tmpl`: systemd service template
- `install.sh`: helper script that installs the unit and prepares the cgroup root

The runtime binary still reads `aion.yaml` on startup. The unit only provides
the process envelope, restart behavior, and cgroup delegation.

Cgroup mode policy:

- local development should normally use `AION_CGROUPS_ENABLED=false` and
  `AION_CGROUPS_MODE=disabled`;
- direct manual cgroup experiments can use `AION_CGROUPS_MODE=direct`, which
  degrades if the cgroup root is not writable;
- installed systemd runs use `AION_CGROUPS_MODE=systemd`, which treats missing
  delegation as a real resource-governance failure.
