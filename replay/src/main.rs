use parallel_exec::{Block, MockTransaction, StateDBOp, StateDBType, VMType};

use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;

use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::runtime::Builder;

use clap::Parser;
use primitive_types::U256;

const GENESIS_ACCOUNT: U256 = U256([0, 0, 0x01000000, 0]);

#[derive(Parser)]
struct Args {
    /// IO trace file
    #[clap(short = 'f', long)]
    trace_file: String,

    /// Latency file
    #[clap(short = 'l', long)]
    lat_file: String,

    /// Number of worker threads
    #[clap(short = 't', long, default_value = "4")]
    threads: usize,

    /// VM type
    #[clap(short = 'e', long, default_value = "sequential")]
    vm_type: VMType,

    /// StateDB type
    #[clap(short = 's', long, default_value = "basic")]
    statedb_type: StateDBType,

    /// Output file for block execution times
    #[clap(short = 'o', long, default_value = "out/block_times.csv")]
    output_file: String,
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

    env_logger::builder()
        .format(move |buf, record| {
            let mut level_style = buf.style();
            let level = match record.level() {
                log::Level::Error => level_style
                    .set_color(env_logger::fmt::Color::Red)
                    .value("ERROR"),
                log::Level::Warn => level_style
                    .set_color(env_logger::fmt::Color::Yellow)
                    .value("WARN "),
                log::Level::Info => level_style
                    .set_color(env_logger::fmt::Color::Green)
                    .value("INFO "),
                log::Level::Debug => level_style
                    .set_color(env_logger::fmt::Color::Blue)
                    .value("DEBUG"),
                log::Level::Trace => level_style
                    .set_color(env_logger::fmt::Color::Magenta)
                    .value("TRACE"),
            };
            let mut target_style = buf.style();
            let target = target_style.set_bold(true).value(record.target());
            let ts = chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.6f");
            writeln!(buf, "{ts} {level} {target}: {}", record.args())
        })
        .init();

    let rt = Builder::new_multi_thread()
        .worker_threads(args.threads)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let op_file = File::open(args.trace_file).await.unwrap();
        let op_reader = BufReader::new(op_file);
        let mut op_lines = op_reader.lines();
        op_lines.next_line().await.unwrap(); // Skip header

        let lat_file = File::open(args.lat_file).await.unwrap();
        let lat_reader = BufReader::new(lat_file);
        let mut lat_lines = lat_reader.lines();
        lat_lines.next_line().await.unwrap(); // Skip header

        let mut out_file = File::create(args.output_file).await.unwrap();
        let mut out_writer = BufWriter::new(&mut out_file);
        out_writer
            .write_all(b"block_id,execution_time\n")
            .await
            .unwrap();

        let mut cur_block = Vec::new();
        let mut cur_txn: Option<(String, String)> = None;
        let mut cur_ops = Vec::new();
        while let Some(line) = op_lines.next_line().await.unwrap() {
            let [blk_id, txn_id, op, state_key, aux] =
                line.trim().split(',').collect::<Vec<_>>()[..]
            else {
                panic!("Invalid line format");
            };

            match cur_txn.as_mut() {
                Some((cur_blk_id, cur_txn_id)) => {
                    if cur_blk_id != blk_id || cur_txn_id != txn_id {
                        let latency_line = lat_lines.next_line().await.unwrap().unwrap();
                        let [lat_blk_id, lat_txn_id, _gas_limit, _gas_used, latency] =
                            latency_line.trim().split(',').collect::<Vec<_>>()[..]
                        else {
                            panic!("Invalid latency line format");
                        };
                        assert_eq!(lat_blk_id, cur_blk_id);
                        assert_eq!(lat_txn_id, cur_txn_id);

                        let new_txn = MockTransaction::new(
                            usize::from_str_radix(&cur_txn_id, 16).unwrap(),
                            std::time::Duration::from_secs_f64(
                                f64::from_str(latency).unwrap() / 1000.0,
                            ),
                            std::mem::take(&mut cur_ops),
                        );
                        cur_block
                            .push(Arc::new(new_txn)
                                as Arc<dyn parallel_exec::Transaction + Send + Sync>);
                    }

                    if cur_blk_id != blk_id {
                        // Process the completed block
                        println!(
                            "Executing block {}: {} transactions",
                            cur_blk_id,
                            cur_block.len()
                        );

                        let blk_id_parsed = u64::from_str(cur_blk_id).unwrap();
                        let new_block = Block {
                            id: blk_id_parsed,
                            transactions: std::mem::take(&mut cur_block),
                        };

                        let mut vm = args.vm_type.new(args.statedb_type);
                        let start = std::time::Instant::now();
                        vm.process(new_block).await.unwrap();
                        let duration = start.elapsed();
                        out_writer
                            .write_all(
                                format!("{},{}\n", blk_id_parsed, duration.as_secs_f64())
                                    .as_bytes(),
                            )
                            .await
                            .unwrap();
                    }

                    *cur_blk_id = blk_id.to_string();
                    *cur_txn_id = txn_id.to_string();
                }
                None => {
                    cur_txn = Some((blk_id.to_string(), txn_id.to_string()));
                }
            }

            let operation = parse_op(op, state_key, aux);
            if !matches!(operation, StateDBOp::AddBalance(addr, _) if addr == GENESIS_ACCOUNT) {
                cur_ops.push(operation);
            }
        }

        // Process the last transaction and block
        if let Some((cur_blk_id, cur_txn_id)) = cur_txn.as_mut() {
            let new_txn = MockTransaction::new(
                usize::from_str_radix(cur_txn_id, 16).unwrap(),
                std::time::Duration::from_secs(0), // TODO
                std::mem::take(&mut cur_ops),
            );
            cur_block.push(Arc::new(new_txn) as Arc<dyn parallel_exec::Transaction + Send + Sync>);

            // Process the completed block
            println!(
                "Executing block {}: {} transactions",
                cur_blk_id,
                cur_block.len()
            );

            let blk_id_parsed = u64::from_str(cur_blk_id).unwrap();
            let new_block = Block {
                id: blk_id_parsed,
                transactions: std::mem::take(&mut cur_block),
            };

            let mut vm = args.vm_type.new(args.statedb_type);
            let start = std::time::Instant::now();
            vm.process(new_block).await.unwrap();
            let duration = start.elapsed();
            out_writer
                .write_all(format!("{},{}\n", blk_id_parsed, duration.as_secs_f64()).as_bytes())
                .await
                .unwrap();
        }

        out_writer.flush().await.unwrap();
    });
}
