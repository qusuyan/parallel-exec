#! /usr/bin/python3

import pandas as pd
import matplotlib.pyplot as plt

out_dir = "../../out"

sequential_df = pd.read_csv(f"{out_dir}/sequential.csv")
parallel_basic_df = pd.read_csv(f"{out_dir}/basic.csv")
parallel_lazy_exec_df = pd.read_csv(f"{out_dir}/lazy-exec.csv")
parallel_enough_balance_df = pd.read_csv(f"{out_dir}/check-enough-balance.csv")

sequential_bad_df = pd.read_csv(f"{out_dir}/sequential-bad.csv")
parallel_basic_bad_df = pd.read_csv(f"{out_dir}/basic-bad.csv")
parallel_lazy_exec_bad_df = pd.read_csv(f"{out_dir}/lazy-exec-bad.csv")
parallel_enough_balance_bad_df = pd.read_csv(f"{out_dir}/check-enough-balance-bad.csv")

barwidth = 0.2
xs = range(len(sequential_df))

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 4), sharey=True, gridspec_kw={'width_ratios': [10, 1], 'height_ratios': [1]})
ax1.bar(
    [x - 1.5*barwidth for x in xs],
    sequential_df["execution_time"],
    width=barwidth,
    label="Sequential",
)
ax1.bar(
    [x - 0.5*barwidth for x in xs],
    parallel_basic_df["execution_time"],
    width=barwidth,
    label="Basic Parallel",
)
ax1.bar(
    [x + 0.5*barwidth for x in xs],
    parallel_lazy_exec_df["execution_time"],
    width=barwidth,
    label="Lazy Exec",
)
ax1.bar(
    [x + 1.5*barwidth for x in xs],
    parallel_enough_balance_df["execution_time"],
    width=barwidth,
    label="Enough Balance",
)

ax1.legend(loc="upper right")

ax1.set_xlim(xs[0] - 0.5, xs[-1] + 0.8)
ax1.set_xticks(xs)
ax1.set_xlabel("Block ID")

xs = [len(xs) + i for i in range(len(sequential_bad_df))]

ax2.bar(
    [x - 1.5*barwidth for x in xs],
    sequential_bad_df["execution_time"],
    width=barwidth,
    label="Sequential",
)
ax2.bar(
    [x - 0.5*barwidth for x in xs],
    parallel_basic_bad_df["execution_time"],
    width=barwidth,
    label="Basic Parallel",
)
ax2.bar(
    [x + 0.5*barwidth for x in xs],
    parallel_lazy_exec_bad_df["execution_time"],    
    width=barwidth,
    label="Lazy Exec",
)
ax2.bar(
    [x + 1.5*barwidth for x in xs],
    parallel_enough_balance_bad_df["execution_time"],
    width=barwidth,
    label="Enough Balance",
)

ax2.set_xticks(xs)
ax2.set_xlim(xs[0] - 0.5, xs[-1] + 0.5)

ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: x * 1000))
ax1.set_ylabel("Execution Time (ms)")

fig.subplots_adjust(wspace=0)
fig.savefig("out.pdf", bbox_inches="tight")