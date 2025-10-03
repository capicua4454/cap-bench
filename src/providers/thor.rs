use std::{ collections::HashMap, error::Error, sync::{ Arc, Mutex } };

use futures_util::{ stream::StreamExt, sink::SinkExt };
use tokio::{ sync::broadcast, task };

use prost::Message;
use tonic::transport::Uri;
use tonic::{ Request, Streaming };
use tokio_stream::Stream;

use crate::{
    config::{ Config, Endpoint, StreamType },
    utils::{ Comparator, TransactionData, get_current_timestamp, open_log_file, write_log_entry },
};

use super::GeyserProvider;

pub mod thor_streamer {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    pub mod types {
        include!(concat!(env!("OUT_DIR"), "/thor_streamer.types.rs"));
    }
}

pub mod publisher {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/publisher.rs"));

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("proto_descriptors");
}
use publisher::{
    event_publisher_client::EventPublisherClient,
    //Empty,
    StreamResponse,
    SubscribeWalletRequest,
    SubscribeAccountsRequest,
};
use thor_streamer::types::{
    MessageWrapper,
    SlotStatusEvent,
    TransactionEvent,
    TransactionEventWrapper,
    SubscribeUpdateAccountInfo,
    message_wrapper::EventMessage,
};

pub struct ThorProvider;
impl GeyserProvider for ThorProvider {
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
            process_thor_endpoint(
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

async fn process_thor_endpoint(
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

    let grpc_url = &endpoint.url;
    let grpc_token = &endpoint.x_token;
    let uri = grpc_url.parse::<tonic::transport::Uri>()?;
    let mut client = EventPublisherClient::connect(uri).await?;
    client = client
        .send_compressed(tonic::codec::CompressionEncoding::Gzip)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
    log::info!("[{}] Connected successfully", endpoint.name);

    let mut request = Request::new(());
    request.metadata_mut().insert("authorization", grpc_token.parse()?);
    let mut request_slot = Request::new(());
    request_slot.metadata_mut().insert("authorization", grpc_token.parse()?);
    let mut request_account = Request::new(SubscribeAccountsRequest {
        account_address: config.accounts.clone(),
        owner_address: config.owners.clone(),
    });
    request_account.metadata_mut().insert("authorization", grpc_token.parse()?);

    let mut stream: Streaming<StreamResponse> = match config.stream_type {
        StreamType::transactions => client.subscribe_to_transactions(request).await?.into_inner(),
        StreamType::account_updates =>
            client.subscribe_to_account_updates(request_account).await?.into_inner(),
    };

    let mut slot_stream: Streaming<StreamResponse> = client
        .subscribe_to_slot_status(request_slot).await?
        .into_inner();

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

                if let Some(Ok(msg)) = message {
                    //println!("Received message: {:?}", msg);
                    match MessageWrapper::decode(&*msg.data) {
                        Ok(message_wrapper) => {

                        if let Some(event) = message_wrapper.event_message {
                            
                            match event {
                                EventMessage::Transaction(transaction_event_wrapper) => {
                                    if let Some(transaction_event) = transaction_event_wrapper.transaction {
                                        if let Some(transaction) = transaction_event.transaction.as_ref() {
                                            if let Some(message) = transaction.message.as_ref() {
                                                let accounts: Vec<String> = message.account_keys
                                                    .iter()
                                                    .map(|key| bs58::encode(key).into_string())
                                                    .collect();
                                                if let Some(meta) = transaction_event.transaction_status_meta.as_ref() {
                                                    if !meta.is_status_err && accounts.iter().any(|account| config.accounts.contains(account)) {
                                                        let timestamp = now;
                                                        let signature = bs58::encode(&transaction_event.signature).into_string();
                                                        write_log_entry(&mut log_file, timestamp, &endpoint.name, &signature)?;
                                                        let mut comp = comparator.lock().unwrap();
                                                        let slot = transaction_event.slot;
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
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                EventMessage::AccountUpdate(update_account_event) => {
                                  
                                        //let owner = bs58::encode(update_account_event.owner).into_string();
                                        //if owner == config.account {
                                            let timestamp = now;
                                            let signature = match update_account_event.txn_signature.as_ref() {
                                                Some(sig_bytes) => bs58::encode(sig_bytes).into_string(),
                                                None => String::new(),
                                            };
                                            write_log_entry(&mut log_file, timestamp, &endpoint.name, &signature)?;
                                            let mut comp = comparator.lock().unwrap();
                                            let slot = update_account_event.slot.unwrap().slot;
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
                                EventMessage::Slot(slot_status_event) => {
                                    log::info!("[{:.3}] [{}] SlotStatus: {:?}", now, endpoint.name, slot_status_event);
                                }
                            }
                        }
                    },
        Err(err) => {
            // Print the error to understand why decoding fails
            println!("Failed to decode MessageWrapper: {}", err);
        }
    }




                }
            }
            slot_message = slot_stream.next() => {
                let now = get_current_timestamp();

                if now > end_time {
                    log::info!("[{}] Benchmark duration ended (slot stream).", endpoint.name);
                    break 'ploop;
                }

                if let Some(Ok(msg)) = slot_message {
                    // Decode and handle slot status event here
                    if let Ok(message_wrapper) = MessageWrapper::decode(&*msg.data) {
                        if let Some(EventMessage::Slot(slot_status_event)) = message_wrapper.event_message {
                            // You can process slot_status_event as needed
                            log::info!("[{:.3}] [{}] SlotStatus: {:?}", now, endpoint.name, slot_status_event);
                        }
                    }
                }
            }



            
        }
    }

    log::info!("[{}] Stream closed", endpoint.name);
    Ok(())
}
