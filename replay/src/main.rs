mod utils;
use utils::{GENESIS_ACCOUNT, parse_lat_csv, parse_op};

use parallel_exec::{
    Block, MockStore, MockTransaction, StateDBOpType, StateDBType, Transaction, VMType,
};

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use clap::Parser;
use tokio::runtime::Builder;

#[derive(Parser)]
struct Args {
    /// IO trace file
    #[clap(short = 'f', long)]
    trace_file: String,

    /// Latency file
    #[clap(short = 'l', long)]
    lat_file: String,

    /// Number of worker threads
    #[clap(short = 't', long, default_value_t = 4)]
    threads: usize,

    /// Cache size for storage (in key-value pairs)
    #[clap(short = 'c', long, default_value_t = 0x1000000)]
    cache_size: usize,

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
        .max_blocking_threads(1)
        .thread_keep_alive(Duration::from_hours(240)) // do not kill blocking threads
        .build()
        .unwrap();
    let kill_signal = Arc::new(AtomicBool::new(false));
    let kill_signal_clone = kill_signal.clone();
    rt.spawn_blocking(move || while !kill_signal_clone.load(Ordering::Relaxed) {}); // keep the blocking thread busy so it does not get used

    let lat_map = parse_lat_csv(&args.lat_file);

    let op_file = File::open(args.trace_file).unwrap();
    let op_reader = BufReader::new(op_file);
    let mut op_lines = op_reader.lines();
    op_lines.next().unwrap().unwrap(); // Skip header
    let mut line_count = 1;

    let mut out_file = File::create(args.output_file).unwrap();
    let mut out_writer = BufWriter::new(&mut out_file);
    out_writer.write_all(b"block_id,execution_time\n").unwrap();

    let mut cur_block = Vec::new();
    let mut cur_store = Box::new(MockStore::new());
    let mut cur_txn: Option<(String, String)> = None;
    let mut cur_ops = Vec::new();
    while let Some(line) = op_lines.next() {
        let line = line.unwrap();
        line_count += 1;
        let split_line = line.trim().split(',').collect::<Vec<_>>();
        let [blk_id, txn_id, start_time, storage_time, op, state_key, aux] = split_line[..] else {
            panic!("{line_count}: Invalid line\"{line}\", expected 5 fields",);
        };

        match cur_txn.as_mut() {
            Some((cur_blk_id, cur_txn_id)) => {
                if cur_blk_id != blk_id || cur_txn_id != txn_id {
                    let txn_id_parsed = usize::from_str_radix(cur_txn_id, 16)
                        .expect(&format!("{line_count}: Invalid txn ID \"{cur_txn_id}\""));
                    let blk_id_parsed = u64::from_str(cur_blk_id)
                        .expect(&format!("{line_count}: Invalid block ID \"{cur_blk_id}\""));

                    let ops = std::mem::take(&mut cur_ops);
                    if let Some(latency) = lat_map.get(&(blk_id_parsed, txn_id_parsed)) {
                        let new_txn = MockTransaction::new(txn_id_parsed, *latency, ops);
                        cur_block.push(Arc::new(new_txn) as Arc<dyn Transaction + Send + Sync>);
                    } else {
                        log::warn!("Latency not found for block {cur_blk_id}, txn {cur_txn_id}",);
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
                    if blk_id_parsed % 100000 == 0 {
                        println!("Processing block {blk_id_parsed}");
                    }
                    let new_block = Block {
                        id: blk_id_parsed,
                        transactions: std::mem::take(&mut cur_block),
                    };
                    let mock_store = std::mem::take(&mut cur_store);

                    let mut vm = args
                        .vm_type
                        .new(args.statedb_type, args.cache_size, mock_store);
                    let start = std::time::Instant::now();
                    rt.block_on(async {
                        vm.process(new_block).await.unwrap();
                    });
                    let duration = start.elapsed();
                    out_writer
                        .write_all(
                            format!("{},{}\n", blk_id_parsed, duration.as_secs_f64()).as_bytes(),
                        )
                        .unwrap();
                }

                *cur_blk_id = blk_id.to_string();
                *cur_txn_id = txn_id.to_string();
            }
            None => {
                cur_txn = Some((blk_id.to_string(), txn_id.to_string()));
            }
        }

        let operation = match parse_op(start_time, storage_time, op, state_key, aux) {
            Ok(op) => op,
            Err(e) => panic!("{line_count}: Error parsing operation \"{line}\": {e:?}",),
        };
        // skip operation for burning gas fee
        if !(operation.account == GENESIS_ACCOUNT
            && matches!(operation.op_type, StateDBOpType::AddBalance(_)))
        {
            cur_store.record_op(&operation);
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
            cur_block.push(Arc::new(new_txn) as Arc<dyn Transaction + Send + Sync>);
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
        let mock_store = std::mem::take(&mut cur_store);

        let mut vm = args
            .vm_type
            .new(args.statedb_type, args.cache_size, mock_store);
        let start = std::time::Instant::now();
        rt.block_on(async {
            vm.process(new_block).await.unwrap();
        });
        let duration = start.elapsed();
        out_writer
            .write_all(format!("{},{}\n", blk_id_parsed, duration.as_secs_f64()).as_bytes())
            .unwrap();
    }

    out_writer.flush().unwrap();
    kill_signal.store(true, Ordering::Relaxed);
}
