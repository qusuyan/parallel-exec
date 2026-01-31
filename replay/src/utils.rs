use parallel_exec::{StateDBOp, StateDBOpType};

use primitive_types::U256;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::str::FromStr;
use std::time::Duration;

pub(crate) const GENESIS_ACCOUNT: U256 = U256([0, 0, 0x01000000, 0]);

#[derive(Debug)]
#[allow(unused)]
pub(crate) enum ParseError {
    InvalidOp,
    WrongNumFields { expected: usize, found: usize },
    InvalidHexKey(uint::FromHexError),
    InvalidNumber(uint::FromDecStrErr),
    InvalidDuration(std::num::ParseIntError),
}

impl From<uint::FromHexError> for ParseError {
    fn from(err: uint::FromHexError) -> Self {
        ParseError::InvalidHexKey(err)
    }
}

impl From<uint::FromDecStrErr> for ParseError {
    fn from(err: uint::FromDecStrErr) -> Self {
        ParseError::InvalidNumber(err)
    }
}

impl From<std::num::ParseIntError> for ParseError {
    fn from(err: std::num::ParseIntError) -> Self {
        ParseError::InvalidDuration(err)
    }
}

pub(crate) fn parse_op(
    start_time: &str,
    storage_time: &str,
    op: &str,
    state_key: &str,
    aux: &str,
) -> Result<StateDBOp, ParseError> {
    let start_time = Duration::from_nanos(start_time.parse()?);
    let storage_time = Duration::from_nanos(storage_time.parse()?);
    match op {
        "Exist" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::Exist,
            start_time,
            storage_time,
        }),
        "Empty" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::Empty,
            start_time,
            storage_time,
        }),
        "CheckEnoughBalance" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::CheckEnoughBalance(U256::from_dec_str(aux)?),
            start_time,
            storage_time,
        }),
        "GetBalance" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::GetBalance,
            start_time,
            storage_time,
        }),
        "GetNonce" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::GetNonce,
            start_time,
            storage_time,
        }),
        "GetCode" | "GetCodeHash" | "GetCodeSize" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::GetCode,
            start_time,
            storage_time,
        }),
        "GetState" => {
            let split_key = state_key.split('-').collect::<Vec<_>>();
            let [account_addr, state_key] = split_key[..] else {
                return Err(ParseError::WrongNumFields {
                    expected: 2,
                    found: split_key.len(),
                });
            };
            Ok(StateDBOp {
                account: U256::from_str(account_addr)?,
                op_type: StateDBOpType::GetState(U256::from_str(state_key)?),
                start_time,
                storage_time,
            })
        }
        "GetCommittedState" => {
            let split_key = state_key.split('-').collect::<Vec<_>>();
            let [account_addr, state_key] = split_key[..] else {
                return Err(ParseError::WrongNumFields {
                    expected: 2,
                    found: split_key.len(),
                });
            };
            Ok(StateDBOp {
                account: U256::from_str(account_addr)?,
                op_type: StateDBOpType::GetState(U256::from_str(state_key)?),
                start_time,
                storage_time,
            })
        }
        "HasSelfDestructed" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::HasSelfDestructed,
            start_time,
            storage_time,
        }),
        "AddBalance" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::AddBalance(U256::from_dec_str(aux)?),
            start_time,
            storage_time,
        }),
        "SubBalance" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::SubBalance(U256::from_dec_str(aux)?),
            start_time,
            storage_time,
        }),
        "SetBalance" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::SetBalance(U256::from_dec_str(aux)?),
            start_time,
            storage_time,
        }),
        "SetNonce" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::SetNonce,
            start_time,
            storage_time,
        }),
        "SetCode" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::SetCode,
            start_time,
            storage_time,
        }),
        "SetState" => {
            let split_key = state_key.split('-').collect::<Vec<_>>();
            let [account_addr, state_key] = split_key[..] else {
                return Err(ParseError::WrongNumFields {
                    expected: 2,
                    found: split_key.len(),
                });
            };
            Ok(StateDBOp {
                account: U256::from_str(account_addr)?,
                op_type: StateDBOpType::SetState(U256::from_str(state_key)?),
                start_time,
                storage_time,
            })
        }
        "SelfDestruct" | "Selfdestruct6780" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::SelfDestruct,
            start_time,
            storage_time,
        }),
        "CreateAccount" => Ok(StateDBOp {
            account: U256::from_str(state_key)?,
            op_type: StateDBOpType::CreateAccount,
            start_time,
            storage_time,
        }),
        _ => Err(ParseError::InvalidOp),
    }
}

pub(crate) fn parse_lat_csv(path: &str) -> HashMap<(u64, usize), Duration> {
    log::debug!("Parsing latency file {}", path);
    let lat_file = File::open(path).unwrap();
    let lat_reader = BufReader::new(lat_file);
    let mut lat_lines = lat_reader.lines();
    lat_lines.next().unwrap().unwrap(); // Skip header
    let mut line_count = 1;

    let mut lat_map = HashMap::new();

    let mut next_print = None;
    while let Some(latency_line) = lat_lines.next() {
        let latency_line = latency_line.unwrap();
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
        let latency_ns =
            u64::from_str(latency).expect(&format!("{line_count}: Invalid latency \"{latency}\""));
        lat_map.insert((blk_id, txn_id), Duration::from_nanos(latency_ns));
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
