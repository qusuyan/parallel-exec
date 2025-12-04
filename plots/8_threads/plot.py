import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

sequential = pd.read_csv("../../out/sequential.csv")
seq_val_basic = pd.read_csv("../../out/seq_val_basic_8.csv")
seq_val_check = pd.read_csv("../../out/seq_val_check_enough_balance_8.csv")
block_stm_basic = pd.read_csv("../../out/block_stm_basic_8.csv")
block_stm_check = pd.read_csv("../../out/block_stm_check_enough_balance_8.csv")

seq_val_basic = seq_val_basic.merge(sequential, on="block_id", suffixes=("", "_seq"))
seq_val_basic["speedup_ratio"] = seq_val_basic["execution_time_seq"] / seq_val_basic["execution_time"]
seq_val_basic["speedup_diff"] = seq_val_basic["execution_time_seq"] - seq_val_basic["execution_time"]

seq_val_check = seq_val_check.merge(sequential, on="block_id", suffixes=("", "_seq"))
seq_val_check["speedup_ratio"] = seq_val_check["execution_time_seq"] / seq_val_check["execution_time"]
seq_val_check["speedup_diff"] = seq_val_check["execution_time_seq"] - seq_val_check["execution_time"]

block_stm_basic = block_stm_basic.merge(sequential, on="block_id", suffixes=("", "_seq"))
block_stm_basic["speedup_ratio"] = block_stm_basic["execution_time_seq"] / block_stm_basic["execution_time"]
block_stm_basic["speedup_diff"] = block_stm_basic["execution_time_seq"] - block_stm_basic["execution_time"]

block_stm_check = block_stm_check.merge(sequential, on="block_id", suffixes=("", "_seq"))
block_stm_check["speedup_ratio"] = block_stm_check["execution_time_seq"] / block_stm_check["execution_time"]
block_stm_check["speedup_diff"] = block_stm_check["execution_time_seq"] - block_stm_check["execution_time"]

(fig1, ax1) = plt.subplots()

seq_val_basic = seq_val_basic.sort_values(by="speedup_ratio")
seq_val_check = seq_val_check.sort_values(by="speedup_ratio")
block_stm_basic = block_stm_basic.sort_values(by="speedup_ratio")
block_stm_check = block_stm_check.sort_values(by="speedup_ratio")

cumulative = np.arange(1, len(seq_val_basic) + 1) / len(seq_val_basic)

ax1.plot(seq_val_basic["speedup_ratio"], cumulative, label="Seq Val - Basic", color="tab:blue", ls="--")
ax1.plot(seq_val_check["speedup_ratio"], cumulative, label="Seq Val - Opt", color="tab:blue", ls="-")
ax1.plot(block_stm_basic["speedup_ratio"], cumulative, label="Block STM - Basic", color="tab:orange", ls="--")
ax1.plot(block_stm_check["speedup_ratio"], cumulative, label="Block STM - Opt", color="tab:orange", ls="-")

ax1.axvline(x=1.0, color="black", lw=0.5, label="No Speedup")

ax1.legend()
ax1.set_xlim(0, 6)
ax1.set_xlabel("Speedup over Sequential Execution (%)")
ax1.set_ylabel("Cumulative Density")
fig1.savefig("ratio.pdf")

(fig2, ax2) = plt.subplots()

seq_val_basic = seq_val_basic.sort_values(by="speedup_diff")
seq_val_check = seq_val_check.sort_values(by="speedup_diff")
block_stm_basic = block_stm_basic.sort_values(by="speedup_diff")
block_stm_check = block_stm_check.sort_values(by="speedup_diff")
print(block_stm_check)

ax2.plot(seq_val_basic["speedup_diff"], cumulative, label="Seq Val - Basic", color="tab:blue", ls="--")
ax2.plot(seq_val_check["speedup_diff"], cumulative, label="Seq Val - Opt", color="tab:blue", ls="-")
ax2.plot(block_stm_basic["speedup_diff"], cumulative, label="Block STM - Basic", color="tab:orange", ls="--")
ax2.plot(block_stm_check["speedup_diff"], cumulative, label="Block STM - Opt", color="tab:orange", ls="-")

ax2.axvline(x=0.0, color="black", lw=0.5, label="No Speedup")

ax2.legend()
ax2.set_xlabel("Speedup over Sequential Execution (ms)")
ax2.set_ylabel("Cumulative Density")
ax2.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: x * 1000))
fig2.savefig("difference.pdf")