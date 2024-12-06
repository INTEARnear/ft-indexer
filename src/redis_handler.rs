use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::{near_indexer_primitives::types::BlockHeight, near_utils};
use intear_events::events::ft::{
    ft_burn::FtBurnEvent, ft_mint::FtMintEvent, ft_transfer::FtTransferEvent,
};
use redis::aio::ConnectionManager;

use crate::{EventContext, FtEventHandler};

pub struct PushToRedisStream {
    mint_stream: RedisEventStream<FtMintEvent>,
    transfer_stream: RedisEventStream<FtTransferEvent>,
    burn_stream: RedisEventStream<FtBurnEvent>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize) -> Self {
        Self {
            mint_stream: RedisEventStream::new(connection.clone(), FtMintEvent::ID),
            transfer_stream: RedisEventStream::new(connection.clone(), FtTransferEvent::ID),
            burn_stream: RedisEventStream::new(connection.clone(), FtBurnEvent::ID),
            max_stream_size,
        }
    }
}

#[async_trait]
impl FtEventHandler for PushToRedisStream {
    async fn handle_mint(&mut self, mint: near_utils::FtMintEvent, context: EventContext) {
        self.mint_stream.add_event(FtMintEvent {
            owner_id: mint.owner_id,
            amount: mint.amount,
            memo: mint.memo,
            transaction_id: context.transaction_id,
            receipt_id: context.receipt_id,
            block_height: context.block_height,
            block_timestamp_nanosec: context.block_timestamp_nanosec,
            token_id: context.contract_id,
        });
    }

    async fn handle_transfer(
        &mut self,
        transfer: near_utils::FtTransferEvent,
        context: EventContext,
    ) {
        self.transfer_stream.add_event(FtTransferEvent {
            old_owner_id: transfer.old_owner_id,
            new_owner_id: transfer.new_owner_id,
            amount: transfer.amount,
            memo: transfer.memo,
            transaction_id: context.transaction_id,
            receipt_id: context.receipt_id,
            block_height: context.block_height,
            block_timestamp_nanosec: context.block_timestamp_nanosec,
            token_id: context.contract_id,
        });
    }

    async fn handle_burn(&mut self, burn: near_utils::FtBurnEvent, context: EventContext) {
        self.burn_stream.add_event(FtBurnEvent {
            owner_id: burn.owner_id,
            amount: burn.amount,
            memo: burn.memo,
            transaction_id: context.transaction_id,
            receipt_id: context.receipt_id,
            block_height: context.block_height,
            block_timestamp_nanosec: context.block_timestamp_nanosec,
            token_id: context.contract_id,
        });
    }

    async fn flush_events(&mut self, block_height: BlockHeight) {
        self.mint_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush mint stream");
        self.transfer_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush transfer stream");
        self.burn_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush burn stream");
    }
}
