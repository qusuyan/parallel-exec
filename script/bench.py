#! /usr/bin/python3

import os, argparse
import subprocess

from datetime import datetime

VM_TYPES = ["seq_val", "block_stm"]
STATEDB_TYPES = ["basic", "lazy-exec", "check-enough-balance"]

def build_replay():
    print("Building replay tool...")
    subprocess.run(["cargo", "build", "-p", "replay", "--release"], check=True)
    print("Replay tool built.")

def run_bench(trace_file, latency_file, num_threads, out_prefix):
    # Create output directory if it doesn't exist
    os.makedirs(out_prefix, exist_ok=True)

    # Run sequential baseline
    print("Running sequential baseline...")
    cmd = [
        "./target/release/replay",
        "--trace-file", trace_file,
        "--lat-file", latency_file,
        "--threads", str(num_threads),
        "--vm-type", "sequential",
        "--output-file", f"{out_prefix}sequential.csv"
    ]
    subprocess.run(cmd, check=True)

    # Parallel execution engines
    for vm_type in VM_TYPES:
        for statedb_type in STATEDB_TYPES:
            print(f"Running benchmark: VM={vm_type}, StateDB={statedb_type}...")
            cmd = [
                "./target/release/replay",
                "--trace-file", trace_file,
                "--lat-file", latency_file,
                "--threads", str(num_threads),
                "--vm-type", vm_type,
                "--statedb-type", statedb_type,
                "--output-file", f"{out_prefix}{vm_type}_{statedb_type}_{num_threads}.csv"
            ]
            subprocess.run(cmd, check=True)

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="Run benchmarks for parallel execution.")
    argparser.add_argument("--trace-file", type=str, required=True, help="Path to the trace file to replay.")
    argparser.add_argument("--latency-file", type=str, help="Path to the transaction latency CSV file.")
    argparser.add_argument("--out-prefix", type=str, help="Output file prefix for benchmark results.")
    argparser.add_argument("--num-threads", type=int, default=4, help="Number of threads to use for parallel execution.")
    args = argparser.parse_args()

    build_replay()
    run_bench(args.trace_file, args.latency_file, args.num_threads, args.out_prefix or f"out/{datetime.now().strftime('%Y%m%d_%H%M%S')}/")