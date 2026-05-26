---
applyTo: "**"
---

when building WOS use the "Build WOS" task
if a WOS build fails during mountfs disk sync (for example in sync_rootfs.sh, mountfs_disk, or a libguestfs/qemu appliance error), treat it as a cluster state problem, not a code failure. this means the cluster or debug VM was not stopped before the build started. prompt the user to stop the cluster/VMs and then rerun the build
debugging must be done by the user. When required prompt the user to run the kernel for debugging
the kernel binary path is build/modules/kern/wos
the init binary path is build/modules/init/init
the testprog binary path is build/modules/testprog/testprog
the httpd binary path is build/modules/httpd/httpd
