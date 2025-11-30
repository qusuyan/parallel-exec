use parallel_exec::{Block, MockTransaction, StateDBOp, StateDBType, VMType};

use std::collections::HashMap;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::runtime::Builder;

use clap::Parser;
use primitive_types::U256;

const GENESIS_ACCOUNT: U256 = U256([0, 0, 0x01000000, 0]);

#[derive(Debug)]
#[allow(unused)]
enum ParseError {
    InvalidOp,
    WrongNumFields { expected: usize, found: usize },
    InvalidHexKey(uint::FromHexError),
}

impl From<uint::FromHexError> for ParseError {
    fn from(err: uint::FromHexError) -> Self {
        ParseError::InvalidHexKey(err)
    }
}

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

fn parse_op(op: &str, state_key: &str, aux: &str) -> Result<StateDBOp, ParseError> {
    match op {
        "Exist" => Ok(StateDBOp::Exist(U256::from_str(state_key)?)),
        "Empty" => Ok(StateDBOp::Empty(U256::from_str(state_key)?)),
        "CheckEnoughBalance" => Ok(StateDBOp::CheckEnoughBalance(
            U256::from_str(state_key)?,
            U256::from_str(aux)?,
        )),
        "GetBalance" => Ok(StateDBOp::GetBalance(U256::from_str(state_key)?)),
        "GetNonce" => Ok(StateDBOp::GetNonce(U256::from_str(state_key)?)),
        "GetCode" | "GetCodeHash" | "GetCodeSize" => {
            Ok(StateDBOp::GetCode(U256::from_str(state_key)?))
        }
        "GetState" => {
            let split_key = state_key.split('-').collect::<Vec<_>>();
            let [account_addr, state_key] = split_key[..] else {
                return Err(ParseError::WrongNumFields {
                    expected: 2,
                    found: split_key.len(),
                });
            };
            Ok(StateDBOp::GetState(
                U256::from_str(account_addr)?,
                U256::from_str(state_key)?,
            ))
        }
        "GetCommittedState" => {
            let split_key = state_key.split('-').collect::<Vec<_>>();
            let [account_addr, state_key] = split_key[..] else {
                return Err(ParseError::WrongNumFields {
                    expected: 2,
                    found: split_key.len(),
                });
            };
            Ok(StateDBOp::GetState(
                U256::from_str(account_addr)?,
                U256::from_str(state_key)?,
            ))
        }
        "HasSelfDestructed" => Ok(StateDBOp::HasSelfDestructed(U256::from_str(state_key)?)),
        "AddBalance" => Ok(StateDBOp::AddBalance(
            U256::from_str(state_key)?,
            U256::from_str(aux)?,
        )),
        "SubBalance" => Ok(StateDBOp::SubBalance(
            U256::from_str(state_key)?,
            U256::from_str(aux)?,
        )),
        "SetBalance" => Ok(StateDBOp::SetBalance(
            U256::from_str(state_key)?,
            U256::from_str(aux)?,
        )),
        "SetNonce" => Ok(StateDBOp::SetNonce(U256::from_str(state_key)?)),
        "SetCode" => Ok(StateDBOp::SetCode(U256::from_str(state_key)?)),
        "SetState" => {
            let split_key = state_key.split('-').collect::<Vec<_>>();
            let [account_addr, state_key] = split_key[..] else {
                return Err(ParseError::WrongNumFields {
                    expected: 2,
                    found: split_key.len(),
                });
            };
            Ok(StateDBOp::SetState(
                U256::from_str(account_addr)?,
                U256::from_str(state_key)?,
            ))
        }
        "SelfDestruct" | "Selfdestruct6780" => {
            Ok(StateDBOp::SelfDestruct(U256::from_str(state_key)?))
        }
        "CreateAccount" => Ok(StateDBOp::CreateAccount(U256::from_str(state_key)?)),
        _ => Err(ParseError::InvalidOp),
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
        let lat_map = parse_lat_csv(&args.lat_file).await;

        let op_file = File::open(args.trace_file).await.unwrap();
        let op_reader = BufReader::new(op_file);
        let mut op_lines = op_reader.lines();
        op_lines.next_line().await.unwrap(); // Skip header
        let mut line_count = 1;

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
            line_count += 1;
            let split_line = line.trim().split(',').collect::<Vec<_>>();
            let [blk_id, txn_id, op, state_key, aux] = split_line[..] else {
                panic!("{line_count}: Invalid line\"{line}\", expected 5 fields",);
            };

            match cur_txn.as_mut() {
                Some((cur_blk_id, cur_txn_id)) => {
                    if cur_blk_id != blk_id || cur_txn_id != txn_id {
                        let txn_id_parsed = usize::from_str_radix(cur_txn_id, 16)
                            .expect(&format!("{line_count}: Invalid txn ID \"{cur_txn_id}\""));
                        let blk_id_parsed = u64::from_str(cur_blk_id)
                            .expect(&format!("{line_count}: Invalid block ID \"{cur_blk_id}\""));

                        if let Some(latency) = lat_map.get(&(blk_id_parsed, txn_id_parsed)) {
                            let new_txn = MockTransaction::new(
                                txn_id_parsed,
                                *latency,
                                std::mem::take(&mut cur_ops),
                            );
                            cur_block.push(Arc::new(new_txn)
                                as Arc<dyn parallel_exec::Transaction + Send + Sync>);
                        } else {
                            log::warn!(
                                "Latency not found for block {cur_blk_id}, txn {cur_txn_id}",
                            );
                        };
                    }

                    if cur_blk_id != blk_id {
                        // Process the completed block
                        log::info!(
                            "Executing block {cur_blk_id}: {} transactions",
                            cur_block.len()
                        );

                        let blk_id_parsed = u64::from_str(cur_blk_id)
                            .expect(&format!("{line_count}: Invalid block ID \"{cur_blk_id}\""));
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

            let operation = match parse_op(op, state_key, aux) {
                Ok(op) => op,
                Err(e) => panic!("{line_count}: Error parsing operation \"{line}\": {e:?}",),
            };
            if !matches!(operation, StateDBOp::AddBalance(addr, _) if addr == GENESIS_ACCOUNT) {
                cur_ops.push(operation);
            }
        }

        // Process the last transaction and block
        if let Some((cur_blk_id, cur_txn_id)) = cur_txn.as_mut() {
            let txn_id_parsed = usize::from_str_radix(cur_txn_id, 16)
                .expect(&format!("{line_count}: Invalid txn ID \"{cur_txn_id}\""));
            let blk_id_parsed = u64::from_str(cur_blk_id)
                .expect(&format!("{line_count}: Invalid block ID \"{cur_blk_id}\"",));

            if let Some(latency) = lat_map.get(&(blk_id_parsed, txn_id_parsed)) {
                let new_txn =
                    MockTransaction::new(txn_id_parsed, *latency, std::mem::take(&mut cur_ops));
                cur_block
                    .push(Arc::new(new_txn) as Arc<dyn parallel_exec::Transaction + Send + Sync>);
            } else {
                log::warn!("Latency not found for block {cur_blk_id}, txn {cur_txn_id}",);
            };

            // Process the completed block
            log::info!(
                "Executing block {cur_blk_id}: {} transactions",
                cur_block.len()
            );

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

async fn parse_lat_csv(path: &str) -> HashMap<(u64, usize), Duration> {
    let lat_file = File::open(path).await.unwrap();
    let lat_reader = BufReader::new(lat_file);
    let mut lat_lines = lat_reader.lines();
    lat_lines.next_line().await.unwrap(); // Skip header
    let mut line_count = 1;

    let mut lat_map = HashMap::new();

    let mut next_print = None;
    while let Some(latency_line) = lat_lines.next_line().await.unwrap() {
        line_count += 1;
        let [lat_blk_id, lat_txn_id, _gas_limit, _gas_used, latency] =
            latency_line.trim().split(',').collect::<Vec<_>>()[..]
        else {
            panic!("{line_count}: Invalid latency line \"{latency_line}\", expected 5 fields",);
        };
        let blk_id = u64::from_str(lat_blk_id)
            .expect(&format!("{line_count}: Invalid block ID \"{lat_blk_id}\""));
        let txn_id = usize::from_str_radix(lat_txn_id, 16)
            .expect(&format!("{line_count}: Invalid txn ID \"{lat_txn_id}\""));
        let latency_ms =
            f64::from_str(latency).expect(&format!("{line_count}: Invalid latency \"{latency}\""));
        lat_map.insert(
            (blk_id, txn_id),
            Duration::from_secs_f64(latency_ms / 1000.0),
        );
        if let Some(next) = next_print {
            if blk_id >= next {
                log::info!("Parsed latencies up to block {blk_id}");
            }
            next_print = Some(next + 100000);
        } else {
            next_print = Some(blk_id / 100000 * 100000 + 100000);
        }
    }

    lat_map
}
