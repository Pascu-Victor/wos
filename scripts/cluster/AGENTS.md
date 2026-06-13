# Cluster Script Agent Notes

`node_setup.py` is the shared single-node VM layout layer. It owns defaults and
helpers for per-node QEMU arguments, overlay disks, serial/QEMU logs, TCG/KVM
mode selection, debug ports, fw_cfg hostname injection, NIC devices, ivshmem
devices, and `/etc/netdevs` injection.

`cluster_setup.py` should translate cluster topology data into a normalized
node spec and then call `node_setup`; do not reintroduce a second copy of QEMU
argument or overlay construction in `cluster_setup.py`.

Node specs are dictionaries with these main sections:

- `id`, `hostname`, `debug`, and optional `mtu`
- `vm`: memory, CPU count, base disks, overlay directory, log paths, BIOS path,
  and optional debug port overrides
- `nics`: one entry per NIC, including `zone_id`, `tap`, `mac`, `model`,
  `queues`, `vhost`, `driver`, and optional bridge metadata
- `ivshmem`: optional shared-memory devices

The KTEST workflow builds a one-node cluster config from `configs/node.json`
through `node_setup.cluster_config_from_node_spec()`. Keep that path compatible
when evolving cluster topology fields.

If launch logs stop in firmware before kernel output, first inspect the KTEST
or cluster boot disk contents and Limine config path before treating it as a
kernel bug.
