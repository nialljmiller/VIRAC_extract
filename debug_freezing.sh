#!/bin/bash
# Usage: ./debug_freeze.sh <JOB_ID> <NODE_NAME>
# Example: ./debug_freeze.sh 233105_1 node113

JOBID=$1
NODE=$2

if [ -z "$JOBID" ] || [ -z "$NODE" ]; then
    echo "Usage: $0 <JOB_ID> <NODE_NAME>"
    exit 1
fi

echo "================================================================="
echo "DIAGNOSING JOB $JOBID ON $NODE"
echo "================================================================="

# 1. Check Load Average (Is the node churning or idle?)
echo "[1] Load Average:"
ssh $NODE "uptime"
echo "-----------------------------------------------------------------"

# 2. Check Memory & Swap (Did we actually hit the limit?)
echo "[2] Memory Usage:"
ssh $NODE "free -h"
echo "-----------------------------------------------------------------"

# 3. Process State Analysis (The most important part)
# We want to know if processes are:
# D = Uninterruptible Sleep (Disk I/O or NFS/BeeGFS Hang)
# R = Running (Spinning CPU)
# S = Sleeping (Waiting for something, potentially Deadlock)
# Z = Zombie (Dead)
echo "[3] Top 5 Python Process States:"
ssh $NODE "ps -eo state,pid,ppid,cmd --sort=-%mem | grep python | head -n 5"
echo "-----------------------------------------------------------------"

# 4. Grab the PID of one worker to trace
WORKER_PID=$(ssh $NODE "pgrep -f 'virac_extractor' | head -n 1")

if [ -z "$WORKER_PID" ]; then
    echo "CRITICAL: No Python processes found! The job died completely."
else
    echo "[4] System Call Trace (Strace) on PID $WORKER_PID"
    echo "    (If this hangs on 'futex', it's a deadlock. If 'read', it's Disk I/O)"
    # trace system calls for 5 seconds
    ssh $NODE "timeout 5s strace -p $WORKER_PID 2>&1 | head -n 10"
fi
echo "-----------------------------------------------------------------"

# 5. Check Kernel Logs for OOM Killer
echo "[5] Recent Kernel Errors (OOM or Filesystem):"
ssh $NODE "dmesg -T | tail -n 5"
echo "================================================================="
