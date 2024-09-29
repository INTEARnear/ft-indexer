use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_utils::{FtBurnEvent, FtMintEvent, FtTransferEvent};
use intear_events::events::ft::{
    ft_burn::FtBurnEventData, ft_mint::FtMintEventData, ft_transfer::FtTransferEventData,
};
use redis::aio::ConnectionManager;

use crate::{EventContext, FtEventHandler};

pub struct PushToRedisStream {
    mint_stream: RedisEventStream<FtMintEventData>,
    transfer_stream: RedisEventStream<FtTransferEventData>,
    burn_stream: RedisEventStream<FtBurnEventData>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize) -> Self {
        Self {
            mint_stream: RedisEventStream::new(
                connection.clone(),
                intear_events::events::ft::ft_mint::FtMintEvent::ID,
            ),
            transfer_stream: RedisEventStream::new(
                connection.clone(),
                intear_events::events::ft::ft_transfer::FtTransferEvent::ID,
            ),
            burn_stream: RedisEventStream::new(
                connection.clone(),
                intear_events::events::ft::ft_burn::FtBurnEvent::ID,
            ),
            max_stream_size,
        }
    }
}

#[async_trait]
impl FtEventHandler for PushToRedisStream {
    async fn handle_mint(&mut self, mint: FtMintEvent, context: EventContext) {
        self.mint_stream
            .emit_event(
                context.block_height,
                FtMintEventData {
                    owner_id: mint.owner_id,
                    amount: mint.amount,
                    memo: mint.memo,
                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,
                    token_id: context.contract_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit mint event");
    }

    async fn handle_transfer(&mut self, transfer: FtTransferEvent, context: EventContext) {
        self.transfer_stream
            .emit_event(
                context.block_height,
                FtTransferEventData {
                    old_owner_id: transfer.old_owner_id,
                    new_owner_id: transfer.new_owner_id,
                    amount: transfer.amount,
                    memo: transfer.memo,
                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,
                    token_id: context.contract_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit transfer event");
    }

    async fn handle_burn(&mut self, burn: FtBurnEvent, context: EventContext) {
        self.burn_stream
            .emit_event(
                context.block_height,
                FtBurnEventData {
                    owner_id: burn.owner_id,
                    amount: burn.amount,
                    memo: burn.memo,
                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,
                    token_id: context.contract_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit burn event");
    }
}
