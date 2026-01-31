use parallel_exec::{
    AccountAddress, Block, MockStore, MockTransaction, StateDBOp, StateDBOpType, StateDBType,
    StorageKey, Transaction, VMType,
};
use primitive_types::U256;

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use std::sync::Arc;
use std::time::Instant;
use tokio::time::Duration;

fn generate_random_statedb_op(
    rng: &mut StdRng,
    accounts: &Vec<AccountAddress>,
    min_start_time: Duration,
    mock_store: &mut MockStore,
) -> StateDBOp {
    let op_type = rng.gen_range(0..=16);
    let address = accounts.choose(rng).unwrap().clone();
    let state_key = accounts.choose(rng).unwrap().clone();
    let amount = U256::from(rng.gen_range(1u64..1000u64));
    let op_type = match op_type {
        0 => StateDBOpType::Exist,
        1 => StateDBOpType::Empty,
        2 => StateDBOpType::GetBalance,
        3 => StateDBOpType::GetNonce,
        4 => StateDBOpType::GetCode,
        5 => StateDBOpType::GetState(state_key),
        6 => StateDBOpType::GetCommittedState(state_key),
        7 => StateDBOpType::HasSelfDestructed,
        8 => StateDBOpType::CheckEnoughBalance(amount),
        9 => StateDBOpType::AddBalance(amount),
        10 => StateDBOpType::SubBalance(amount),
        11 => StateDBOpType::SetBalance(amount),
        12 => StateDBOpType::SetNonce,
        13 => StateDBOpType::SetCode,
        14 => StateDBOpType::SetState(state_key),
        15 => StateDBOpType::SelfDestruct,
        16 => StateDBOpType::CreateAccount,
        _ => unreachable!(),
    };
    let start_time = min_start_time + Duration::from_nanos(rng.gen_range(1000u64..10000u64));

    let storage_time = match &op_type {
        StateDBOpType::GetBalance
            | StateDBOpType::GetNonce
            | StateDBOpType::GetCode
            | StateDBOpType::Exist
            | StateDBOpType::Empty
            | StateDBOpType::HasSelfDestructed
            | StateDBOpType::CheckEnoughBalance(_)
            | StateDBOpType::AddBalance(_)  // treat read-modify-write as read
            | StateDBOpType::SubBalance(_) => {
                let storage_key = StorageKey::Account(address);
            if mock_store.in_cache(&storage_key) {
                Duration::from_secs(0)
            } else {
                Duration::from_nanos(rng.gen_range(10000u64..1000000u64))
            }
        }
        StateDBOpType::GetState(state_key) | StateDBOpType::GetCommittedState(state_key) => {
            let storage_key = StorageKey::State(address, *state_key);
            if mock_store.in_cache(&storage_key) {
                Duration::from_secs(0)
            } else {
                Duration::from_nanos(rng.gen_range(10000u64..1000000u64))
            }
        }
        _ => {
            Duration::ZERO
        }
    };
    let op = StateDBOp {
        account: address,
        op_type,
        start_time,
        storage_time,
    };
    mock_store.record_op(&op);
    op
}

fn generate_random_txn(
    rng: &mut StdRng,
    txn_id: usize,
    txn_size: usize,
    accounts: &Vec<AccountAddress>,
    mock_store: &mut MockStore,
) -> (Arc<dyn Transaction + Send + Sync>, Duration) {
    let mut ops = Vec::with_capacity(txn_size);
    let mut cur_time = Duration::from_secs(0);
    for _ in 0..txn_size {
        let op = generate_random_statedb_op(rng, accounts, cur_time, mock_store);
        cur_time = op.start_time + op.storage_time;
        ops.push(op);
    }
    let exec_time = cur_time + Duration::from_micros(rng.gen_range(0u64..1000u64));
    let txn = MockTransaction::new(txn_id, exec_time, ops);

    (
        Arc::new(txn) as Arc<dyn Transaction + Send + Sync>,
        exec_time,
    )
}

fn generate_random_block(blk_id: u64) -> (Block, MockStore, Duration) {
    let seed = std::env::var("PARALLEL_EXEC_SEED")
        .ok()
        .and_then(|s| match s.parse::<u64>() {
            Ok(num) => Some(num),
            Err(_) => {
                eprintln!("Invalid PARALLEL_EXEC_SEED value {}, use random seed", s);
                None
            }
        })
        .unwrap_or_else(|| rand::random());
    eprintln!("Using seed {}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let mut mock_store = MockStore::new();
    let mut exec_time = Duration::from_secs(0);
    let num_txn = 1;
    let mut transactions = Vec::with_capacity(num_txn);

    for txn_id in 0..num_txn {
        let txn_size = rng.gen_range(30usize..1000);
        let unique_accounts = rng.gen_range(2usize..20);
        let accounts = (0..unique_accounts)
            .map(|_| AccountAddress::from_little_endian(&rng.r#gen::<[u8; 32]>()))
            .collect::<Vec<_>>();

        let (txn, txn_exec_time) =
            generate_random_txn(&mut rng, txn_id, txn_size, &accounts, &mut mock_store);
        exec_time += txn_exec_time;
        transactions.push(txn);
    }

    let block = Block {
        id: blk_id,
        transactions,
    };
    (block, mock_store, exec_time)
}

#[test]
fn test_sequential_runtime_reasonable() {
    for blk_id in 0..1 {
        let (block, mock_store, exec_time) = generate_random_block(blk_id);
        println!("{:?}", mock_store);
        println!("Expected exec time: {:?}", exec_time);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut linear_vm =
                VMType::Sequential.new(StateDBType::Basic, 0x1000000, Box::new(mock_store.clone()));
            let start = Instant::now();
            linear_vm.process(block.clone()).await.unwrap();
            let elapsed = Instant::now() - start;
            let sim_error = if elapsed < exec_time {
                exec_time - elapsed
            } else {
                elapsed - exec_time
            };
            println!(
                "|{} - {}|, {}",
                exec_time.as_nanos(),
                elapsed.as_nanos(),
                sim_error.as_nanos()
            );
            assert!(sim_error < Duration::from_micros(1));
        });
    }
}

#[test]
fn test_block_stm() {
    for blk_id in 0..10 {
        let (block, mock_store, _) = generate_random_block(blk_id);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut linear_vm =
                VMType::Sequential.new(StateDBType::Basic, 0x1000000, Box::new(mock_store.clone()));
            linear_vm.process(block.clone()).await.unwrap();
            let linear_vm_result = linear_vm.commit().await.unwrap();
            let mut block_stm_vm =
                VMType::BlockSTM.new(StateDBType::Basic, 0x1000000, Box::new(mock_store.clone()));
            block_stm_vm.process(block).await.unwrap();
            let block_stm_vm_result = block_stm_vm.commit().await.unwrap();
            assert_eq!(linear_vm_result, block_stm_vm_result);
        });
    }
}

#[test]
fn test_sequential_validation_vm() {
    for blk_id in 0..10 {
        let (block, mock_store, _) = generate_random_block(blk_id);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut linear_vm =
                VMType::Sequential.new(StateDBType::Basic, 0x1000000, Box::new(mock_store.clone()));
            linear_vm.process(block.clone()).await.unwrap();
            let linear_vm_result = linear_vm.commit().await.unwrap();
            let mut seq_val_vm = VMType::SequentialValidation.new(
                StateDBType::Basic,
                0x1000000,
                Box::new(mock_store.clone()),
            );
            seq_val_vm.process(block).await.unwrap();
            let seq_val_vm_result = seq_val_vm.commit().await.unwrap();
            assert_eq!(linear_vm_result, seq_val_vm_result);
        });
    }
}

#[test]
fn test_check_enough_balance() {
    for blk_id in 0..10 {
        let (block, mock_store, _) = generate_random_block(blk_id);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut linear_vm = VMType::SequentialValidation.new(
                StateDBType::Basic,
                0x1000000,
                Box::new(mock_store.clone()),
            );
            linear_vm.process(block.clone()).await.unwrap();
            let linear_vm_result = linear_vm.commit().await.unwrap();
            let mut seq_val_vm = VMType::SequentialValidation.new(
                StateDBType::CheckEnoughBalance,
                0x1000000,
                Box::new(mock_store.clone()),
            );
            seq_val_vm.process(block).await.unwrap();
            let seq_val_vm_result = seq_val_vm.commit().await.unwrap();
            assert_eq!(linear_vm_result, seq_val_vm_result);
        });
    }
}
