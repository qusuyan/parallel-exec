use parallel_exec::{
    AccountAddress, Block, MockTransaction, StateDBOp, StateDBType, Transaction, VMType,
};
use primitive_types::U256;

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use std::sync::Arc;
use tokio::time::Duration;

fn generate_random_statedb_op(rng: &mut StdRng, accounts: &Vec<AccountAddress>) -> StateDBOp {
    let op_type = rng.gen_range(0..=16);
    let address = accounts.choose(rng).unwrap().clone();
    let state_key = accounts.choose(rng).unwrap().clone();
    let amount = U256::from(rng.gen_range(1u64..1000u64));
    match op_type {
        0 => StateDBOp::Exist(address),
        1 => StateDBOp::Empty(address),
        2 => StateDBOp::GetBalance(address),
        3 => StateDBOp::GetNonce(address),
        4 => StateDBOp::GetCode(address),
        5 => StateDBOp::GetState(address, state_key),
        6 => StateDBOp::GetCommittedState(address, state_key),
        7 => StateDBOp::HasSelfDestructed(address),
        8 => StateDBOp::CheckEnoughBalance(address, amount),
        9 => StateDBOp::AddBalance(address, amount),
        10 => StateDBOp::SubBalance(address, amount),
        11 => StateDBOp::SetBalance(address, amount),
        12 => StateDBOp::SetNonce(address),
        13 => StateDBOp::SetCode(address),
        14 => StateDBOp::SetState(address, state_key),
        15 => StateDBOp::SelfDestruct(address),
        16 => StateDBOp::CreateAccount(address),
        _ => unreachable!(),
    }
}

fn generate_random_txn(
    rng: &mut StdRng,
    txn_id: usize,
    txn_size: usize,
    accounts: &Vec<AccountAddress>,
) -> MockTransaction {
    let mut ops = Vec::with_capacity(txn_size);
    for _ in 0..txn_size {
        let op = generate_random_statedb_op(rng, accounts);
        ops.push(op);
    }
    let exec_time = Duration::from_micros(10) * txn_size as u32
        + Duration::from_micros(rng.gen_range(0u64..1000u64));
    MockTransaction::new(txn_id, exec_time, ops)
}

fn generate_random_block(blk_id: u64) -> Block {
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

    let num_txn = rng.gen_range(50usize..200);
    let mut transactions = Vec::with_capacity(num_txn);

    for txn_id in 0..num_txn {
        let txn_size = rng.gen_range(30usize..1000);
        let unique_accounts = rng.gen_range(2usize..20);
        let accounts = (0..unique_accounts)
            .map(|_| AccountAddress::from_little_endian(&rng.r#gen::<[u8; 32]>()))
            .collect::<Vec<_>>();

        let txn = generate_random_txn(&mut rng, txn_id, txn_size, &accounts);
        transactions.push(Arc::new(txn) as Arc<dyn Transaction + Send + Sync>);
    }

    return Block {
        id: blk_id,
        transactions,
    };
}

#[test]
fn test_block_stm() {
    for blk_id in 0..10 {
        let block = generate_random_block(blk_id);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut linear_vm = VMType::Sequential.new(StateDBType::Basic);
            linear_vm.process(block.clone()).await.unwrap();
            let linear_vm_result = linear_vm.commit().await.unwrap();
            let mut block_stm_vm = VMType::BlockSTM.new(StateDBType::Basic);
            block_stm_vm.process(block).await.unwrap();
            let block_stm_vm_result = block_stm_vm.commit().await.unwrap();
            assert_eq!(linear_vm_result, block_stm_vm_result);
        });
    }
}

#[test]
fn test_sequential_validation_vm() {
    for blk_id in 0..10 {
        let block = generate_random_block(blk_id);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut linear_vm = VMType::Sequential.new(StateDBType::Basic);
            linear_vm.process(block.clone()).await.unwrap();
            let linear_vm_result = linear_vm.commit().await.unwrap();
            let mut seq_val_vm = VMType::SequentialValidation.new(StateDBType::Basic);
            seq_val_vm.process(block).await.unwrap();
            let seq_val_vm_result = seq_val_vm.commit().await.unwrap();
            assert_eq!(linear_vm_result, seq_val_vm_result);
        });
    }
}

#[test]
fn test_check_enough_balance() {
    for blk_id in 0..10 {
        let block = generate_random_block(blk_id);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut linear_vm = VMType::SequentialValidation.new(StateDBType::Basic);
            linear_vm.process(block.clone()).await.unwrap();
            let linear_vm_result = linear_vm.commit().await.unwrap();
            let mut seq_val_vm = VMType::SequentialValidation.new(StateDBType::CheckEnoughBalance);
            seq_val_vm.process(block).await.unwrap();
            let seq_val_vm_result = seq_val_vm.commit().await.unwrap();
            assert_eq!(linear_vm_result, seq_val_vm_result);
        });
    }
}
