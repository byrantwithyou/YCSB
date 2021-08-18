#!/usr/bin/env python3

import os
import signal
import argparse
import subprocess
FILE_PATH = os.path.abspath(os.path.dirname(__file__))
SRC_PATH = os.path.normpath(os.path.join(FILE_PATH, ".."))
parser = argparse.ArgumentParser()
parser.add_argument("-c", default=1, type=int,
                    help="the number of client thread")
parser.add_argument(
    "workload", help="the workload from YCSB workload, can be one of a,b,c,d,e")
parser.add_argument("type", help="workload type, can be `load` or `run`")
parser.add_argument("-sstsize", default=64, type=int,
                    help="the size of the sstable in MB")
parser.add_argument("-z", default=0.99, type=float,
                    help="zipfian distribution value(0.5-0.99)")
parser.add_argument("-o", default=5000000, type=int, help="operation count")
parser.add_argument("-r", default=5000000, type=int, help="record count")
parser.add_argument("-d", default="/tmp/ycsb-rocksdb-data",
                    help="directory to hold the database file, default is /tmp/ycsb-rocksdb-data")
parser.add_argument("-f", default="result",
                    help="filename to hold the workload profiling result")
args = parser.parse_args()


def exec_cmd(cmd, out=None, cwd=None):
    p = subprocess.Popen(cmd, shell=True, stdout=out, stderr=out, cwd=cwd)
    p.wait()
    if p.returncode != 0:
        print("command %s is not successful!" % cmd)
        exit(1)
    return p


def recompile():
    global SRC_PATH
    exec_cmd("mvn -pl site.ycsb:rocksdb-binding -am clean package", cwd=SRC_PATH)


def run_ycsb():
    global SRC_PATH
    global args
    global DBDIR
    exec_cmd("sudo -v")
    trace_cmd = "sudo bpftrace -o %s" % args.f + \
        " -e 'tracepoint:syscalls:sys_exit_write /strncmp(" + \
        '"rocksdb", comm, 7) == 0/ {@ = hist(args->ret) }' + r"'"
    p = subprocess.Popen(trace_cmd, shell=True)
    trace_pid = p.pid
    ycsb_cmd = "./bin/ycsb {rl} rocksdb -s -p recordcount={rc} -p operationcount={oc} -P workloads/workload{workload} -p rocksdb.dir={DIR} -threads={C} -p rocksdb.optionsfile=option.ini".format(
        rl=args.type, rc=args.r, oc=args.o, workload=args.workload, DIR=DBDIR, C=args.c)
    print(ycsb_cmd)
    exit(1)
    exec_cmd(ycsb_cmd, cwd=SRC_PATH)
    exec_cmd("sleep 10")
    os.kill(trace_pid, signal.SIGINT)


def handle_err():
    print("argument invalid!")
    exit(1)


def pre_work():
    global SRC_PATH
    exec_cmd("rm -f option.ini", cwd=SRC_PATH)


def generate_option_file():
    global SRC_PATH
    global args
    exec_cmd("cp defaultoption.ini option.ini", cwd=SRC_PATH)
    sstablesize = int(args.sstsize * 1024 * 1024)
    cmd = 'sed -i "s/target_file_size_base=.*/target_file_size_base={sstsize}/g" option.ini'.format(
        sstsize=sstablesize)
    exec_cmd(cmd, cwd=SRC_PATH)


def deal_with_zipfian():
    global SRC_PATH
    global args
    cmd = "sed -i " + \
        r"'s/\(.*\)ZIPFIAN_CONSTANT\ .*/\1ZIPFIAN_CONSTANT\ = " + \
        "{zip};/g'".format(zip=args.z) + " ZipfianGenerator.java"
    ZIP_PATH = os.path.normpath(os.path.join(
        SRC_PATH, "core", "src", "main", "java", "site", "ycsb", "generator"))
    exec_cmd(cmd, cwd=ZIP_PATH)


if __name__ == "__main__":
    pre_work()
    if args.r <= 0 or args.o <= 0 or args.c <= 0 or args.sstsize <= 0 or args.z > 0.99 or args.z < 0.5:
        handle_err()
    if "abcde".find(args.workload) == -1 or "runload".find(args.type) == -1:
        handle_err()
    DBDIR = os.path.abspath(args.d)
    exec_cmd("mkdir -p {DIR}".format(DIR=DBDIR))
    generate_option_file()
    deal_with_zipfian()
    recompile()
    run_ycsb()
