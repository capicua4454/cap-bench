use std::{ collections::HashMap, error::Error, sync::{ Arc, Mutex } };

use futures_util::{ stream::StreamExt, sink::SinkExt };
use tokio::{ sync::broadcast, task };
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::{
    geyser::{
        subscribe_update::UpdateOneof,
        SubscribeRequest,
        SubscribeRequestPing,
        SubscribeRequestFilterSlots,
        SubscribeRequestFilterAccounts,
    },
    prelude::{ SubscribeRequestFilterTransactions },
    tonic::transport::ClientTlsConfig,
};

use crate::{
    config::{ Config, Endpoint, StreamType },
    utils::{ Comparator, TransactionData, get_current_timestamp, open_log_file, write_log_entry },
};

use super::GeyserProvider;

pub struct YellowstoneProvider;

impl GeyserProvider for YellowstoneProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        shutdown_tx: broadcast::Sender<()>,
        shutdown_rx: broadcast::Receiver<()>,
        start_time: f64,
        warmup_end_time: f64,
        end_time: f64,
        comparator: Arc<Mutex<Comparator>>
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move {
            process_yellowstone_endpoint(
                endpoint,
                config,
                shutdown_tx,
                shutdown_rx,
                start_time,
                warmup_end_time,
                end_time,
                comparator
            ).await
        })
    }
}

async fn process_yellowstone_endpoint(
    endpoint: Endpoint,
    config: Config,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
    start_time: f64,
    warmup_end_time: f64,
    end_time: f64,
    comparator: Arc<Mutex<Comparator>>
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut log_file = open_log_file(&endpoint.name)?;

    log::info!("[{}] Connecting to endpoint: {}", endpoint.name, endpoint.url);

    let mut client = GeyserGrpcClient::build_from_shared(endpoint.url)?
        .x_token(Some(endpoint.x_token))?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect().await?;

    log::info!("[{}] Connected successfully", endpoint.name);

    let (mut subscribe_tx, mut stream) = client.subscribe().await?;
    let commitment: yellowstone_grpc_proto::geyser::CommitmentLevel = config.commitment.into();

    let mut transactions = HashMap::new();
    let mut slots = HashMap::new();
    let mut accounts = HashMap::new();
    slots.insert("slots".to_string(), SubscribeRequestFilterSlots {
        filter_by_commitment: Some(true),
        interslot_updates: Some(false),
    });
    transactions.insert("account".to_string(), SubscribeRequestFilterTransactions {
        vote: Some(false),
        failed: Some(false),
        signature: None,
        account_include: vec![config.account.clone()],
        account_exclude: vec![],
        account_required: vec![],
    });

    accounts.insert("account".to_string(), SubscribeRequestFilterAccounts {
        account: vec![],
        owner: vec![config.account.clone()],
        filters: vec![],
        nonempty_txn_signature: None,
    });

    let subscribe_request = match config.stream_type {
        StreamType::transactions => SubscribeRequest {
            slots,
            accounts: HashMap::default(),
            transactions,
            transactions_status: HashMap::default(),
            entry: HashMap::default(),
            blocks: HashMap::default(),
            blocks_meta: HashMap::default(),
            commitment: Some(commitment as i32),
            accounts_data_slice: Vec::default(),
            ping: None,
            from_slot: None,
        },
        StreamType::account_updates => SubscribeRequest {
            slots,
            accounts,
            transactions: HashMap::default(),
            transactions_status: HashMap::default(),
            entry: HashMap::default(),
            blocks: HashMap::default(),
            blocks_meta: HashMap::default(),
            commitment: Some(commitment as i32),
            accounts_data_slice: Vec::default(),
            ping: None,
            from_slot: None,
        },
    };
    
    subscribe_tx.send(subscribe_request).await?;

    'ploop: loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                log::info!("[{}] Received stop signal...", endpoint.name);
                break;
            }

            message = stream.next() => {
                let now = get_current_timestamp();

                // Stop after end_time
                if now > end_time {
                    log::info!("[{}] Benchmark duration ended.", endpoint.name);
                    break 'ploop;
                }

                match message {
                    Some(Ok(msg)) => {
                        match msg.update_oneof {
                            Some(UpdateOneof::Transaction(tx_msg)) => {
                                if let Some(tx) = tx_msg.transaction {
                                    let accounts = tx.transaction.clone().unwrap().message.unwrap().account_keys
                                        .iter()
                                        .map(|key| bs58::encode(key).into_string())
                                        .collect::<Vec<String>>();

                                        if let Some(meta) = tx.meta.as_ref() {
                                            if meta.err.is_none() {
                                    if accounts.contains(&config.account) {
                                        // Only record transactions after warmup
                                        //if now >= warmup_end_time {
                                            let timestamp = now;
                                            let signature = bs58::encode(&tx.transaction.unwrap().signatures[0]).into_string();

                                            write_log_entry(&mut log_file, timestamp, &endpoint.name, &signature)?;

                                            let mut comp = comparator.lock().unwrap();
                                            let slot = tx_msg.slot;
                                            comp.add(
                                                endpoint.name.clone(),
                                                TransactionData {
                                                    timestamp,
                                                    signature: signature.clone(),
                                                    start_time,
                                                    slot,
                                                },
                                            );

                                            log::info!("[{:.3}] [{}] {}", timestamp, endpoint.name, signature);
                                        //}
                                    }
                                }
                                }
                                }
                            },
                            Some(UpdateOneof::Account(tx_msg)) => {
                                if let Some(tx) = tx_msg.account {
                                    let owner = bs58::encode(&tx.owner).into_string();
                                        
                                    if owner == config.account {
                                        // Only record transactions after warmup
                                        //if now >= warmup_end_time {
                                            let timestamp = now;
                                            let signature = bs58::encode(tx.txn_signature.as_ref().unwrap()).into_string();

                                            write_log_entry(&mut log_file, timestamp, &endpoint.name, &signature)?;

                                            let mut comp = comparator.lock().unwrap();
                                            let slot = tx_msg.slot;
                                            comp.add(
                                                endpoint.name.clone(),
                                                TransactionData {
                                                    timestamp,
                                                    signature: signature.clone(),
                                                    start_time,
                                                    slot,
                                                },
                                            );

                                            log::info!("[{:.3}] [{}] {}", timestamp, endpoint.name, signature);
                                        //}
                                    }
                               
                                }
                            },
                            Some(UpdateOneof::Slot(slot_status_event)) => {
                                // Just print out the slot status event, similar to Thor
                                log::info!("[{}] SlotStatus: {:?}", endpoint.name, slot_status_event);
                            },

                            _ => {}
                        }
                    },
                    Some(Err(e)) => {
                        log::error!("[{}] Error receiving message: {:?}", endpoint.name, e);
                        break;
                    },
                    None => {
                        log::info!("[{}] Stream closed", endpoint.name);
                        break;
                    }
                }
            }
        }
    }

    log::info!("[{}] Stream closed", endpoint.name);
    Ok(())
}
