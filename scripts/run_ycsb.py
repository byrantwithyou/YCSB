#!/usr/bin/env python3
import os
import argparse
import subprocess
FILE_PATH = os.path.abspath(os.path.dirname(__file__))
SRC_PATH = os.path.normpath(os.path.join(FILE_PATH, ".."))
parser = argparse.ArgumentParser()
parser.add_argument("-c", default=1, type=int, help="the number of client thread")
parser.add_argument("workload",help="the workload from YCSB workload a,b,c,d,e")
parser.add_argument("type", help="workload type, can be `load` or `run`")
parser.add_argument("-sstsize", default=64, type=int, help="the size of the sstable in MB")
parser.add_argument("-z", default=0.99, type=float, help="zipfian distribution value(0.5-0.99)")
parser.add_argument("-o", default=5000000, type=int, help="operation count")
parser.add_argument("-r", default=5000000, type=int, help="record count")
args = parser.parse_args()
def exec_cmd(cmd, out=None, cwd=None):
    p = subprocess.Popen(cmd, shell=True, stdout=out, stderr=out, cwd=cwd)
    p.wait()
    if p.returncode != 0:
        print("command %s is not successful!" % cmd)
        exit(1)
    return p
def recompile():
    exec_cmd("mvn -pl site.ycsb:rocksdb-binding -am clean package", cwd=SRC_PATH)
def run_ycsb():
    # operation count, record count, option file specify, workload, type, client thread 
    pass

if __name__ == "__main__":
    if args.r <= 0 or args.o <= 0 or args.c <= 0 or args.s <= 0 or args.sstsize <= 0 or args.z > 0.99 or args.z < 0.5:
        print("argument invalid!")
        exit(1)
    if "abcde".find(args.workload) == -1 or "runload".find(args.type) == -1 :
        print("argument invalid!")
        exit(1)
    # 2. generate an option file
        # sstsize
    # 3. deal with zipfian
    recompile()
    run_ycsb()
