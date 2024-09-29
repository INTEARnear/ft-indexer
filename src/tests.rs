use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::types::AccountId,
    near_utils::{FtBurnEvent, FtMintEvent, FtTransferEvent},
    neardata_server::NeardataServerProvider,
    run_indexer, BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};

use ft_indexer::{EventContext, FtEventHandler, FtIndexer};

#[tokio::test]
async fn detects_mints() {
    struct TestHandler {
        mint_events: HashMap<AccountId, Vec<(FtMintEvent, EventContext)>>,
    }

    #[async_trait]
    impl FtEventHandler for TestHandler {
        async fn handle_mint(&mut self, mint: FtMintEvent, context: EventContext) {
            self.mint_events
                .entry(context.predecessor_id.clone())
                .or_insert_with(Vec::new)
                .push((mint, context));
        }

        async fn handle_transfer(&mut self, _transfer: FtTransferEvent, _context: EventContext) {}

        async fn handle_burn(&mut self, _burn: FtBurnEvent, _context: EventContext) {}
    }

    let handler = TestHandler {
        mint_events: HashMap::new(),
    };

    let mut indexer = FtIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(129_190_044..=129_190_047),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .mint_events
            .get(&"honeybot.near".parse::<AccountId>().unwrap())
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn detects_transfers() {
    struct TestHandler {
        transfer_events: HashMap<AccountId, Vec<(FtTransferEvent, EventContext)>>,
    }

    #[async_trait]
    impl FtEventHandler for TestHandler {
        async fn handle_mint(&mut self, _mint: FtMintEvent, _context: EventContext) {}

        async fn handle_transfer(&mut self, transfer: FtTransferEvent, context: EventContext) {
            let entry = self
                .transfer_events
                .entry(context.predecessor_id.clone())
                .or_insert_with(Vec::new);
            entry.push((transfer, context));
        }

        async fn handle_burn(&mut self, _burn: FtBurnEvent, _context: EventContext) {}
    }

    let handler = TestHandler {
        transfer_events: HashMap::new(),
    };

    let mut indexer = FtIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(129_190_058..=129_190_068),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .transfer_events
            .get(&"dca.deltatrade.near".parse::<AccountId>().unwrap())
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn detects_tkn_transfers() {
    struct TestHandler {
        transfer_events: HashMap<AccountId, Vec<(FtTransferEvent, EventContext)>>,
    }

    #[async_trait]
    impl FtEventHandler for TestHandler {
        async fn handle_mint(&mut self, _mint: FtMintEvent, _context: EventContext) {}

        async fn handle_transfer(&mut self, transfer: FtTransferEvent, context: EventContext) {
            let entry = self
                .transfer_events
                .entry(context.predecessor_id.clone())
                .or_insert_with(Vec::new);
            entry.push((transfer, context));
        }

        async fn handle_burn(&mut self, _burn: FtBurnEvent, _context: EventContext) {}
    }

    let handler = TestHandler {
        transfer_events: HashMap::new(),
    };

    let mut indexer = FtIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(129_163_629..=129_163_632),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .transfer_events
            .get(&"intelbot.near".parse::<AccountId>().unwrap())
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn detects_burns() {
    struct TestHandler {
        burn_events: HashMap<AccountId, Vec<(FtBurnEvent, EventContext)>>,
    }

    #[async_trait]
    impl FtEventHandler for TestHandler {
        async fn handle_mint(&mut self, _mint: FtMintEvent, _context: EventContext) {}

        async fn handle_transfer(&mut self, _transfer: FtTransferEvent, _context: EventContext) {}

        async fn handle_burn(&mut self, burn: FtBurnEvent, context: EventContext) {
            self.burn_events
                .entry(context.predecessor_id.clone())
                .or_insert_with(Vec::new)
                .push((burn, context));
        }
    }

    let handler = TestHandler {
        burn_events: HashMap::new(),
    };

    let mut indexer = FtIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(129_186_699..=129_186_710),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .burn_events
            .get(&"shit.0xshitzu.near".parse::<AccountId>().unwrap())
            .unwrap()
            .len(),
        1
    );
}