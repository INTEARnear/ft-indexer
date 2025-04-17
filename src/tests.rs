use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::types::{AccountId, BlockHeight}, near_utils::{FtBurnEvent, FtMintEvent, FtTransferEvent}, neardata::NeardataProvider, run_indexer, BlockRange, IndexerOptions, PreprocessTransactionsSettings
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

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        mint_events: HashMap::new(),
    };

    let mut indexer = FtIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 129_190_044,
                end_exclusive: Some(129_190_047),
            })
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

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        transfer_events: HashMap::new(),
    };

    let mut indexer = FtIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 129_190_058,
                end_exclusive: Some(129_190_068),
            })
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

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        transfer_events: HashMap::new(),
    };

    let mut indexer = FtIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 129_163_629,
                end_exclusive: Some(129_163_632),
            })
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

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        burn_events: HashMap::new(),
    };

    let mut indexer = FtIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 129_186_699,
                end_exclusive: Some(129_186_710),
            })
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

#[tokio::test]
async fn detects_native_near_transfers() {
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

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        transfer_events: HashMap::new(),
    };

    let mut indexer = FtIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 131_214_339,
                end_exclusive: Some(131_214_342),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .transfer_events
            .get(&"slimedragon.near".parse::<AccountId>().unwrap())
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn detects_native_near_function_transfers() {
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

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        transfer_events: HashMap::new(),
    };

    let mut indexer = FtIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 131_103_427,
                end_exclusive: Some(131_103_430),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .transfer_events
            .get(&"fiery_drone.user.intear.near".parse::<AccountId>().unwrap())
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn does_not_detect_fee_refunds() {
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

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        transfer_events: HashMap::new(),
    };

    let mut indexer = FtIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 131_103_427,
                end_exclusive: Some(131_103_430),
            })
        },
    )
    .await
    .unwrap();

    assert!(indexer
        .0
        .transfer_events
        .get(&"system".parse::<AccountId>().unwrap())
        .is_none());
}
