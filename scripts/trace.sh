#!/usr/bin/env bash
sudo bpftrace -e 'tracepoint:syscalls:sys_exit_write /strncmp("rocksdb", comm, 7) == 0/ {@ = hist(args->ret) }'
