pub mod redis_handler;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::types::{AccountId, BlockHeight};
use inindexer::near_indexer_primitives::views::{ActionView, ReceiptEnumView};
use inindexer::near_indexer_primitives::CryptoHash;
use inindexer::near_indexer_primitives::StreamerMessage;
use inindexer::near_utils::{
    EventLogData, FtBurnEvent, FtBurnLog, FtMintEvent, FtMintLog, FtTransferEvent, FtTransferLog,
};
use inindexer::{IncompleteTransaction, Indexer, TransactionReceipt};

#[async_trait]
pub trait FtEventHandler: Send + Sync {
    async fn handle_mint(&mut self, mint: FtMintEvent, context: EventContext);
    async fn handle_transfer(&mut self, transfer: FtTransferEvent, context: EventContext);
    async fn handle_burn(&mut self, burn: FtBurnEvent, context: EventContext);
}

pub struct FtIndexer<T: FtEventHandler + Send + Sync + 'static>(pub T);

#[async_trait]
impl<T: FtEventHandler + Send + Sync + 'static> Indexer for FtIndexer<T> {
    type Error = String;

    async fn on_receipt(
        &mut self,
        receipt: &TransactionReceipt,
        transaction: &IncompleteTransaction,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        let get_context_lazy = || {
            let predecessor_id = receipt.receipt.receipt.predecessor_id.clone();
            let contract_id = receipt.receipt.receipt.receiver_id.clone();
            let transaction_id = transaction.transaction.transaction.hash;
            let receipt_id = receipt.receipt.receipt.receipt_id;
            let block_height = receipt.block_height;
            let block_timestamp_nanosec = receipt.block_timestamp_nanosec;
            EventContext {
                transaction_id,
                receipt_id,
                block_height,
                block_timestamp_nanosec,
                predecessor_id,
                contract_id,
            }
        };
        if receipt.is_successful(false) {
            for log in &receipt.receipt.execution_outcome.outcome.logs {
                if let Some(tkn_log) = log.strip_prefix("Transfer ") {
                    if let Some((amount, owners)) = tkn_log.split_once(" from ") {
                        let Ok(amount) = amount.parse::<u128>() else {
                            continue;
                        };
                        if let Some((from, to)) = owners.split_once(" to ") {
                            let Ok(from) = from.parse::<AccountId>() else {
                                continue;
                            };
                            let Ok(to) = to.parse::<AccountId>() else {
                                continue;
                            };
                            let transfer = FtTransferEvent {
                                old_owner_id: from,
                                new_owner_id: to,
                                amount,
                                memo: None,
                            };
                            self.0.handle_transfer(transfer, get_context_lazy()).await;
                        }
                    }
                }
                if !log.contains("nep141") {
                    // Don't even start parsing logs if they don't even contain the NEP-141 standard
                    continue;
                }
                if let Ok(mint_log) = EventLogData::<FtMintLog>::deserialize(log) {
                    if mint_log.validate() {
                        log::debug!("Mint log: {mint_log:?}");
                        for mint in mint_log.data.0 {
                            self.0.handle_mint(mint, get_context_lazy()).await;
                        }
                    }
                }
                if let Ok(transfer_log) = EventLogData::<FtTransferLog>::deserialize(log) {
                    if transfer_log.validate() {
                        log::debug!("Transfer log: {transfer_log:?}");
                        for transfer in transfer_log.data.0 {
                            self.0.handle_transfer(transfer, get_context_lazy()).await;
                        }
                    }
                }
                if let Ok(burn_log) = EventLogData::<FtBurnLog>::deserialize(log) {
                    if burn_log.validate() {
                        log::debug!("Burn log: {burn_log:?}");
                        for burn in burn_log.data.0 {
                            self.0.handle_burn(burn, get_context_lazy()).await;
                        }
                    }
                }
            }

            if let ReceiptEnumView::Action {
                signer_id, actions, ..
            } = &receipt.receipt.receipt.receipt
            {
                for action in actions {
                    match action {
                        ActionView::Transfer { deposit } => {
                            let transfer = FtTransferEvent {
                                old_owner_id: signer_id.clone(),
                                new_owner_id: receipt.receipt.receipt.receiver_id.clone(),
                                amount: *deposit,
                                memo: None,
                            };
                            let predecessor_id = receipt.receipt.receipt.predecessor_id.clone();
                            let transaction_id = transaction.transaction.transaction.hash;
                            let receipt_id = receipt.receipt.receipt.receipt_id;
                            let block_height = receipt.block_height;
                            let block_timestamp_nanosec = receipt.block_timestamp_nanosec;
                            self.0
                                .handle_transfer(
                                    transfer,
                                    EventContext {
                                        transaction_id,
                                        receipt_id,
                                        block_height,
                                        block_timestamp_nanosec,
                                        predecessor_id,
                                        contract_id: "near".parse().unwrap(),
                                    },
                                )
                                .await;
                        }
                        ActionView::FunctionCall { deposit, .. } => {
                            if *deposit > 1 {
                                let transfer = FtTransferEvent {
                                    old_owner_id: signer_id.clone(),
                                    new_owner_id: receipt.receipt.receipt.receiver_id.clone(),
                                    amount: *deposit,
                                    memo: None,
                                };
                                let predecessor_id = receipt.receipt.receipt.predecessor_id.clone();
                                let transaction_id = transaction.transaction.transaction.hash;
                                let receipt_id = receipt.receipt.receipt.receipt_id;
                                let block_height = receipt.block_height;
                                let block_timestamp_nanosec = receipt.block_timestamp_nanosec;
                                self.0
                                    .handle_transfer(
                                        transfer,
                                        EventContext {
                                            transaction_id,
                                            receipt_id,
                                            block_height,
                                            block_timestamp_nanosec,
                                            predecessor_id,
                                            contract_id: "near".parse().unwrap(),
                                        },
                                    )
                                    .await;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EventContext {
    pub transaction_id: CryptoHash,
    pub receipt_id: CryptoHash,
    pub block_height: BlockHeight,
    pub block_timestamp_nanosec: u128,
    pub predecessor_id: AccountId,
    pub contract_id: AccountId,
}
