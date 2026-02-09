use parallel_exec::{
    AccountAddress, Block, MockStore, MockTransaction, StateDBOp, StateDBOpType, StateDBType,
    StateKey, StorageKey, StorageKeyType, Transaction, VMType, Value,
};

use primitive_types::U256;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::{Builder, Runtime};
use tokio::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};

fn vm_configs() -> Vec<(VMType, StateDBType)> {
    let mut configs = vec![(VMType::Sequential, StateDBType::Basic)];
    for vm_type in [VMType::SequentialValidation, VMType::BlockSTM] {
        for statedb_type in [StateDBType::Basic, StateDBType::CheckEnoughBalance] {
            configs.push((vm_type.clone(), statedb_type));
        }
    }
    configs
}

fn bench_rng() -> StdRng {
    let seed = std::env::var("PARALLEL_EXEC_BENCH_SEED")
        .ok()
        .and_then(|s| match s.parse::<u64>() {
            Ok(num) => Some(num),
            Err(_) => {
                eprintln!(
                    "Invalid PARALLEL_EXEC_BENCH_SEED value {}, use random seed",
                    s
                );
                None
            }
        })
        .unwrap_or_else(|| rand::random());
    eprintln!("Using seed {}", seed);
    StdRng::seed_from_u64(seed)
}

fn random_u256<R: Rng>(rng: &mut R) -> U256 {
    let mut bytes = [0u64; 4];
    rng.fill(&mut bytes);
    U256(bytes)
}

pub(crate) fn bench_gen_balance_transfer_txn<R: Rng>(
    rng: &mut R,
    sender_addr: AccountAddress,
    recver_addr: AccountAddress,
    recver_state: StateKey,
    storage: &MockStore,
) -> MockTransaction {
    let mut ops = vec![];
    let mut start_time = Duration::ZERO;

    let (sender_read_time, _) = storage
        .inner()
        .get(&StorageKey {
            account: sender_addr,
            key_type: StorageKeyType::Account,
        })
        .expect("sender account not in storage times");
    let (recver_read_time, _) = storage
        .inner()
        .get(&StorageKey {
            account: recver_addr,
            key_type: StorageKeyType::Account,
        })
        .expect("recver account not in storage times");
    let (recver_code_read_time, _) = storage
        .inner()
        .get(&StorageKey {
            account: recver_addr,
            key_type: StorageKeyType::Code,
        })
        .expect("recver code not in storage times");
    let (recver_state_read_time, _) = storage
        .inner()
        .get(&StorageKey {
            account: recver_addr,
            key_type: StorageKeyType::Account,
        })
        .expect("recver state not in storage times");

    for op_num in 0..13 {
        let compute_time = Duration::from_nanos(rng.random_range(200..2000));
        start_time += compute_time;
        let op = match op_num {
            0 => StateDBOp {
                // check sender exists and nonce matches txn nonce
                op_type: StateDBOpType::GetNonce,
                account: sender_addr,
                start_time,
                storage_time: *sender_read_time,
            },
            1 => StateDBOp {
                // checker sender is a user-owned account
                op_type: StateDBOpType::GetCodeHash,
                account: sender_addr,
                start_time,
                storage_time: *sender_read_time,
            },
            2 => StateDBOp {
                // check if sender has enough balance for gas and balance transfer
                op_type: StateDBOpType::CheckEnoughBalance(U256::from(100000000000000u64)),
                account: sender_addr,
                start_time,
                storage_time: *sender_read_time,
            },
            3 => StateDBOp {
                // reserve gas value
                op_type: StateDBOpType::SubBalance(U256::from(100000000000000u64)),
                account: sender_addr,
                start_time,
                storage_time: *sender_read_time,
            },
            4 => StateDBOp {
                // update account nonce
                op_type: StateDBOpType::SetNonce,
                account: sender_addr,
                start_time,
                storage_time: *sender_read_time,
            },
            5 => StateDBOp {
                // check recver exists
                op_type: StateDBOpType::Exist,
                account: recver_addr,
                start_time,
                storage_time: *recver_read_time,
            },
            6 => StateDBOp {
                // balance transfer
                op_type: StateDBOpType::SubBalance(U256::zero()),
                account: sender_addr,
                start_time,
                storage_time: *sender_read_time,
            },
            7 => StateDBOp {
                // balance transfer
                op_type: StateDBOpType::AddBalance(U256::zero()),
                account: recver_addr,
                start_time,
                storage_time: *recver_read_time,
            },
            8 => StateDBOp {
                // get recver code
                op_type: StateDBOpType::GetCode,
                account: recver_addr,
                start_time,
                storage_time: *recver_code_read_time,
            },
            9 => StateDBOp {
                // check against recver hash
                op_type: StateDBOpType::GetCodeHash,
                account: recver_addr,
                start_time,
                storage_time: *recver_read_time,
            },
            10 => StateDBOp {
                // get recver state
                op_type: StateDBOpType::GetState(recver_state),
                account: recver_addr,
                start_time,
                storage_time: *recver_state_read_time,
            },
            11 => StateDBOp {
                // update recver state
                op_type: StateDBOpType::SetState(recver_state),
                account: recver_addr,
                start_time,
                storage_time: Duration::ZERO,
            },
            12 => StateDBOp {
                // refund unused gas
                op_type: StateDBOpType::AddBalance(U256::from(75000000000000u64)),
                account: sender_addr,
                start_time,
                storage_time: *sender_read_time,
            },
            _ => unreachable!(),
        };
        start_time += op.storage_time;
        ops.push(op);
    }

    MockTransaction::new(start_time, ops)
}

fn bench_prep<R: Rng>(
    rng: &mut R,
    num_accounts: usize,
    num_states_per_account: usize,
) -> (Vec<(AccountAddress, Vec<StateKey>)>, Box<MockStore>) {
    let accounts: Vec<(AccountAddress, Vec<StateKey>)> = (0..num_accounts)
        .map(|_| {
            let addr = random_u256(rng);
            let states = (0..num_states_per_account)
                .map(|_| random_u256(rng))
                .collect();
            (addr, states)
        })
        .collect();

    let storage_times: HashMap<_, _> = accounts
        .iter()
        .flat_map(|(addr, states)| {
            let account_key = StorageKey {
                account: *addr,
                key_type: StorageKeyType::Account,
            };
            let code_key = StorageKey {
                account: *addr,
                key_type: StorageKeyType::Code,
            };
            let state_keys = states.iter().map(|state_key| StorageKey {
                account: *addr,
                key_type: StorageKeyType::State(*state_key),
            });
            [account_key, code_key]
                .into_iter()
                .chain(state_keys)
                .map(|key| {
                    (
                        key,
                        (
                            Duration::from_nanos(rng.random_range(3000..20000)),
                            Some(Value::zero()),
                        ),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let mock_store = Box::new(MockStore::new(storage_times));
    (accounts, mock_store)
}

fn bench_gen_block_zipf<R: Rng>(
    rng: &mut R,
    block_size: usize,
    zipf: f64,
    accounts: &Vec<(AccountAddress, Vec<StateKey>)>,
    storage: &MockStore,
) -> Block {
    let transactions = (0..block_size)
        .map(|_| {
            let sender_idx = Zipf::new(accounts.len() as f64, zipf)
                .expect("error generating zipf")
                .sample(rng) as usize
                - 1;
            let mut recver_idx = Zipf::new((accounts.len() - 1) as f64, zipf)
                .expect("error generating zipf")
                .sample(rng) as usize
                - 1;
            if recver_idx >= sender_idx {
                recver_idx += 1;
            }

            let (sender_addr, _) = &accounts[sender_idx];
            let (recver_addr, recver_states) = &accounts[recver_idx];

            let recver_state_idx = Zipf::new((recver_states.len()) as f64, zipf)
                .expect("error generating zipf")
                .sample(rng) as usize
                - 1;
            let recver_state = &recver_states[recver_state_idx];
            Arc::new(bench_gen_balance_transfer_txn(
                rng,
                *sender_addr,
                *recver_addr,
                *recver_state,
                &storage,
            )) as Arc<dyn Transaction + Sync + Send>
        })
        .collect();
    Block {
        id: rng.random(),
        transactions,
    }
}

fn bench_gen_rt(num_threads: usize) -> (Runtime, Arc<AtomicBool>) {
    let rt = Builder::new_multi_thread()
        .worker_threads(num_threads)
        .max_blocking_threads(1)
        .thread_keep_alive(Duration::from_hours(240)) // do not kill blocking threads
        .build()
        .unwrap();
    let kill_signal = Arc::new(AtomicBool::new(false));
    let kill_signal_clone = kill_signal.clone();
    rt.spawn_blocking(move || while !kill_signal_clone.load(Ordering::Relaxed) {}); // keep the blocking thread busy so it does not get used
    (rt, kill_signal)
}

fn uniform(c: &mut Criterion) {
    let mut rng = bench_rng();
    let (accounts, storage) = bench_prep(&mut rng, 1000, 1000);
    let (rt, kill_signal) = bench_gen_rt(4);

    for (vm_type, statedb_type) in vm_configs() {
        for block_size in [20, 50, 100, 200, 500, 1000] {
            c.bench_with_input(
                BenchmarkId::new(
                    "uniform",
                    format!("{vm_type:?}_{statedb_type:?}_{block_size}"),
                ),
                &(vm_type, statedb_type, block_size),
                |b, &(vm_type, statedb_type, block_size)| {
                    b.iter_batched(
                        || {
                            let block = bench_gen_block_zipf(
                                &mut rng, block_size, 0.0, &accounts, &storage,
                            );
                            let vm = vm_type.new(statedb_type, 0x1000000, storage.clone());
                            (block, vm)
                        },
                        |(block, mut vm)| rt.block_on(async { vm.process(block).await.unwrap() }),
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }

    kill_signal.store(true, Ordering::Relaxed);
}

criterion_group!(benches, uniform);
criterion_main!(benches);
