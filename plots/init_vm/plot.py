#! /usr/bin/python3

import pandas as pd
import matplotlib.pyplot as plt

out_dir = "../../out"

sequential_df = pd.read_csv(f"{out_dir}/sequential.csv")
seq_val_df = pd.read_csv(f"{out_dir}/lazy-exec.csv")
block_stm_df = pd.read_csv(f"{out_dir}/block-stm.csv")

sequential_bad_df = pd.read_csv(f"{out_dir}/sequential-bad.csv")
seq_val_bad_df = pd.read_csv(f"{out_dir}/lazy-exec-bad.csv")
block_stm_bad_df = pd.read_csv(f"{out_dir}/block-stm-bad.csv")

barwidth = 0.25
xs = range(len(sequential_df))

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 4), sharey=True, gridspec_kw={'width_ratios': [10, 1], 'height_ratios': [1]})
ax1.bar(
    [x - barwidth for x in xs],
    sequential_df["execution_time"],
    width=barwidth,
    label="Sequential",
)
ax1.bar(
    xs,
    seq_val_df["execution_time"],
    width=barwidth,
    label="Sequential Validation",
    color='tab:green'
)
ax1.bar(
    [x + barwidth for x in xs],
    block_stm_df["execution_time"],
    width=barwidth,
    label="Block-STM",
    color='tab:pink'
)

ax1.legend(loc="upper right")

ax1.set_xlim(xs[0] - 0.5, xs[-1] + 0.8)
ax1.set_xticks(xs)
ax1.set_xlabel("Block ID")

xs = [len(xs) + i for i in range(len(sequential_bad_df))]

ax2.bar(
    [x - barwidth for x in xs],
    sequential_bad_df["execution_time"],
    width=barwidth,
    label="Sequential",
)
ax2.bar(
    xs,
    seq_val_bad_df["execution_time"],    
    width=barwidth,
    label="Sequential Validation",
    color='tab:green'
)
ax2.bar(
    [x + barwidth for x in xs],
    block_stm_bad_df["execution_time"],
    width=barwidth,
    label="Block-STM",
    color='tab:pink'
)

ax2.set_xticks(xs)
ax2.set_xlim(xs[0] - 0.5, xs[-1] + 0.5)

ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: x * 1000))
ax1.set_ylabel("Execution Time (ms)")

fig.subplots_adjust(wspace=0)
fig.savefig("out.pdf", bbox_inches="tight")