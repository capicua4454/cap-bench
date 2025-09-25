use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::config::ConfigToml;

#[derive(Debug, Clone)]
pub struct TransactionData {
    pub timestamp: f64,
    pub signature: String,
    pub start_time: f64,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct SlotData {
    pub timestamp: f64,
    pub start_time: f64,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct AccountData {
    pub timestamp: f64,
    pub signature: String,
    pub start_time: f64,
    pub slot: u64,
}

#[derive(Debug, Clone, Default)]
pub struct Comparator {
    pub data: HashMap<String, HashMap<String, TransactionData>>,
    pub total_transactions: usize,
    pub valid_transactions: usize,
    pub valid_per_endpoint: HashMap<String, usize>, // NEW: valid count per endpoint
}

impl Comparator {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            total_transactions: 0,
            valid_transactions: 0,
            valid_per_endpoint: HashMap::new(),
        }
    }

    pub fn add(&mut self, from: String, data: TransactionData) {
        self.total_transactions += 1;


let is_new_signature = !self.data.contains_key(&data.signature);
let endpoint_map = self.data.entry(data.signature.clone()).or_insert_with(HashMap::new);
let is_new_for_endpoint = !endpoint_map.contains_key(&from);
endpoint_map.insert(from.clone(), data.clone());

// Count unique signatures globally
if is_new_signature {
    self.valid_transactions += 1;
}

        // Count unique signatures per endpoint
        if is_new_for_endpoint {
            *self.valid_per_endpoint.entry(from.clone()).or_insert(0) += 1;
        }

        log::info!(
            "Valid transactions per endpoint: {:?} | Total valid: {} | Total processed: {}",
            self.valid_per_endpoint,
            self.valid_transactions,
            self.total_transactions
        );
    }

    pub fn get_valid_count(&self) -> usize {
        self.valid_transactions
    }

    pub fn get_valid_count_for(&self, endpoint: &str) -> usize {
        *self.valid_per_endpoint.get(endpoint).unwrap_or(&0)
    }
}

pub fn get_current_timestamp() -> f64 {
    let start = SystemTime::now();
    let since_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
    since_epoch.as_secs_f64()
}

pub fn percentile(sorted_data: &[f64], p: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }
    let index = (p * (sorted_data.len() - 1) as f64).round() as usize;
    sorted_data[index]
}

pub fn open_log_file(name: &str) -> std::io::Result<impl Write> {
    let log_filename = format!("transaction_log_{}.txt", name);
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_filename)
}

pub fn write_log_entry(
    file: &mut impl Write,
    timestamp: f64,
    endpoint_name: &str,
    signature: &str,
) -> std::io::Result<()> {
    let log_entry = format!(
        "[{:.3}] [{}] {}\n",
        timestamp,
        endpoint_name,
        signature
    );
    file.write_all(log_entry.as_bytes())
}