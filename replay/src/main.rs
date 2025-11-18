use parallel_exec::{Block, MockTransaction, SequentialVM, StateDBOp, VM};

use std::str::FromStr;

use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::runtime::Builder;

use clap::Parser;
use primitive_types::U256;

#[derive(Parser)]
struct Args {
    // Define your command line arguments here
    #[clap(short = 'f', long)]
    trace_file: String,
}

fn parse_op(op: &str, state_key: &str, aux: &str) -> StateDBOp {
    match op {
        "Exist" => StateDBOp::Exist(U256::from_str(state_key).unwrap()),
        "Empty" => StateDBOp::Empty(U256::from_str(state_key).unwrap()),
        "CheckEnoughBalance" => StateDBOp::CheckEnoughBalance(
            U256::from_str(state_key).unwrap(),
            U256::from_str(aux).unwrap(),
        ),
        "GetBalance" => StateDBOp::GetBalance(U256::from_str(state_key).unwrap()),
        "GetNonce" => StateDBOp::GetNonce(U256::from_str(state_key).unwrap()),
        "GetCode" | "GetCodeHash" | "GetCodeSize" => {
            StateDBOp::GetCode(U256::from_str(state_key).unwrap())
        }
        "GetState" => {
            let [account_addr, state_key] = state_key.split('-').collect::<Vec<_>>()[..] else {
                panic!("Invalid state key format for GetState");
            };
            StateDBOp::GetState(
                U256::from_str(account_addr).unwrap(),
                U256::from_str(state_key).unwrap(),
            )
        }
        "GetCommittedState" => {
            let [account_addr, state_key] = state_key.split('-').collect::<Vec<_>>()[..] else {
                panic!("Invalid state key format for GetCommittedState");
            };
            StateDBOp::GetState(
                U256::from_str(account_addr).unwrap(),
                U256::from_str(state_key).unwrap(),
            )
        }
        "HasSelfDestructed" => StateDBOp::HasSelfDestructed(U256::from_str(state_key).unwrap()),
        "AddBalance" => StateDBOp::AddBalance(
            U256::from_str(state_key).unwrap(),
            U256::from_str(aux).unwrap(),
        ),
        "SubBalance" => StateDBOp::SubBalance(
            U256::from_str(state_key).unwrap(),
            U256::from_str(aux).unwrap(),
        ),
        "SetBalance" => StateDBOp::SetBalance(
            U256::from_str(state_key).unwrap(),
            U256::from_str(aux).unwrap(),
        ),
        "SetNonce" => StateDBOp::SetNonce(U256::from_str(state_key).unwrap()),
        "SetCode" => StateDBOp::SetCode(U256::from_str(state_key).unwrap()),
        "SetState" => {
            let [account_addr, state_key] = state_key.split('-').collect::<Vec<_>>()[..] else {
                panic!("Invalid state key format for SetState");
            };
            StateDBOp::SetState(
                U256::from_str(account_addr).unwrap(),
                U256::from_str(state_key).unwrap(),
            )
        }
        "SelfDestruct" => StateDBOp::SelfDestruct(U256::from_str(state_key).unwrap()),
        "CreateAccount" => StateDBOp::CreateAccount(U256::from_str(state_key).unwrap()),
        _ => {
            panic!("Unknown operation: {}", op);
        }
    }
}

fn main() {
    let args = Args::parse();

    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let file = File::open(args.trace_file).await.unwrap();
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        lines.next_line().await.unwrap(); // Skip header

        let mut cur_block = Vec::new();
        let mut cur_txn: Option<(String, String)> = None;
        let mut cur_ops = Vec::new();
        while let Some(line) = lines.next_line().await.unwrap() {
            let [blk_id, txn_id, op, state_key, aux] =
                line.trim().split(',').collect::<Vec<_>>()[..]
            else {
                panic!("Invalid line format");
            };

            match cur_txn.as_mut() {
                Some((cur_blk_id, cur_txn_id)) => {
                    if cur_blk_id != blk_id || cur_txn_id != txn_id {
                        let new_txn = MockTransaction::new(
                            u64::from_str(&cur_txn_id).unwrap(),
                            std::time::Duration::from_secs(0), // TODO
                            std::mem::take(&mut cur_ops),
                        );
                        cur_block
                            .push(Box::new(new_txn)
                                as Box<dyn parallel_exec::Transaction + Send + Sync>);
                    }

                    if cur_blk_id != blk_id {
                        // Process the completed block
                        println!(
                            "Completed block {}: {} transactions",
                            cur_blk_id,
                            cur_block.len()
                        );

                        let new_block = Block {
                            id: u64::from_str(cur_blk_id).unwrap(),
                            transactions: std::mem::take(&mut cur_block),
                        };

                        let vm = SequentialVM::new();
                        vm.process(new_block).await;
                    }

                    *cur_blk_id = blk_id.to_string();
                    *cur_txn_id = txn_id.to_string();
                }
                None => {
                    cur_txn = Some((blk_id.to_string(), txn_id.to_string()));
                }
            }

            let operation = parse_op(op, state_key, aux);
            cur_ops.push(operation);
        }

        // Process the last transaction and block
        if let Some((cur_blk_id, cur_txn_id)) = cur_txn.as_mut() {
            let new_txn = MockTransaction::new(
                u64::from_str(cur_txn_id).unwrap(),
                std::time::Duration::from_secs(0), // TODO
                std::mem::take(&mut cur_ops),
            );
            cur_block.push(Box::new(new_txn) as Box<dyn parallel_exec::Transaction + Send + Sync>);

            // Process the completed block
            println!(
                "Completed block {}: {} transactions",
                cur_blk_id,
                cur_block.len()
            );

            let new_block = Block {
                id: u64::from_str(cur_blk_id).unwrap(),
                transactions: std::mem::take(&mut cur_block),
            };

            let vm = SequentialVM::new();
            vm.process(new_block).await;
        }
    });
}
