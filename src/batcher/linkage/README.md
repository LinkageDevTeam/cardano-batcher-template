# Linkage Finance Batcher

### Example Implementation for Index Fund Operations

This module shows how to use the cardano-batcher-template for a real DApp. It handles all operations for the example contracts of the Linkage Finance index funds on Cardano [Stable Gain Example](https://github.com/LinkageDevTeam/linkage-stable-gain-contracts).

## What is Linkage Finance?

Linkage Finance lets users create and hold **index funds** on Cardano. An index fund holds a basket of tokens. Users can:

- **Deposit** tokens into a fund and receive fund tokens
- **Withdraw** tokens by burning their fund tokens
- **Collect royalties** (fund creators only) from fees

The batcher handles these operations off-chain and submits them to the blockchain.

## Features

| Feature | Description |
|---------|-------------|
| **Deposit Processing** | Build transactions to mint fund tokens |
| **Withdraw Processing** | Build transactions to burn fund tokens |
| **Royalty Collection** | Build transactions for creators to collect fees |
| **Fund Scanning** | Find and track all fund UTXOs on-chain |
| **Reference Scripts** | Use reference scripts for smaller transactions |
| **Operation Queuing** | Queue multiple operations for processing |

## Quick Start

### Configuration

Copy the example environment file:

```bash
cp linkage.env.example .env
```

Edit `.env` with your settings:

```bash
# Required
LINKAGE_NETWORK=preprod
LINKAGE_BLOCKFROST_PROJECT_ID=preprodXXX...  # Get from blockfrost.io

# Optional
LINKAGE_SIGNING_KEY_PATH=./keys/signing.skey
LINKAGE_BATCH_SIZE_MAX=5
```

### Scan for Funds

See all existing funds on the network:

```bash
python scripts/run_linkage_demo.py scan
```

Output:
```
Found 3 fund(s):

Fund #1
  Token Name: 746573745f66756e...
  Name: TestFund
  Royalty Factor: 3
  Accrued Royalty: 150
```

### Queue a Deposit

Add a deposit operation to the queue:

```bash
python scripts/run_linkage_demo.py deposit \
    --fund 746573745f66756e... \
    --address addr_test1qz... \
    --multiple 10
```

### Queue a Withdrawal

Add a withdrawal operation to the queue:

```bash
python scripts/run_linkage_demo.py withdraw \
    --fund 746573745f66756e... \
    --address addr_test1qz... \
    --amount 1000
```

### Run as a Service

Start the batcher to process operations continuously:

```bash
python scripts/run_linkage_demo.py run
```

The batcher will:
1. Scan for fund UTXOs
2. Check for pending operations
3. Build and submit transactions
4. Repeat every 30 seconds

Press `Ctrl+C` to stop.

## Using in Python

```python
from batcher.linkage import LinkageBatcher, LinkageConfig

# Create config from environment
config = LinkageConfig.from_env()

# Create batcher
batcher = LinkageBatcher(config)

# Initialize (connects to blockchain)
await batcher.initialize()

# Queue some operations
batcher.queue_deposit(
    fund_token_name="746573745f66756e...",
    user_address="addr_test1qz...",
    multiple=10,
)

batcher.queue_withdrawal(
    fund_token_name="746573745f66756e...",
    user_address="addr_test1qz...",
    amount=1000,
)

# Run once to process pending operations
await batcher.start(run_once=True)

# Or run continuously
await batcher.start()
```

## Components

```
src/batcher/linkage/
├── types.py      # Data types for fund datums and redeemers
├── config.py     # Configuration with network defaults
├── scanner.py    # Find and parse fund UTXOs
├── builder.py    # Build deposit/withdraw/royalty transactions
└── batcher.py    # Main service that runs everything
```

| Component | What it does |
|-----------|--------------|
| **Types** | Defines the data structures used by Linkage smart contracts |
| **Config** | Handles settings like network, addresses, and API keys |
| **Scanner** | Finds fund UTXOs on-chain and parses their datums |
| **Builder** | Creates the actual transactions for each operation type |
| **Batcher** | Coordinates everything and runs the main processing loop |

## How Operations Work

### Deposit

When a user deposits tokens into a fund:

1. Scanner finds the current fund UTXO
2. Builder calculates how many tokens the user needs to send
3. Builder creates a transaction that:
   - Consumes the old fund UTXO
   - Adds the deposited tokens
   - Mints new fund tokens for the user
   - Creates a new fund UTXO with updated state
4. User signs and submits the transaction

### Withdrawal

When a user withdraws from a fund:

1. Scanner finds the current fund UTXO
2. Builder calculates which tokens the user will receive
3. Builder creates a transaction that:
   - Consumes the old fund UTXO
   - Burns the user's fund tokens
   - Sends underlying tokens to the user
   - Creates a new fund UTXO (minus withdrawn tokens)
4. User signs and submits the transaction

### Royalty Collection

When a fund creator collects royalties:

1. Scanner finds the current fund UTXO
2. Builder checks how much royalty has accrued
3. Builder creates a transaction that:
   - Consumes the old fund UTXO
   - Sends accrued fund tokens to the creator
   - Updates the royalty counter in the datum
4. Creator signs and submits the transaction

## Configuration Options

| Variable | Description | Default |
|----------|-------------|---------|
| `LINKAGE_NETWORK` | Network to use (preprod, mainnet) | preprod |
| `LINKAGE_BLOCKFROST_PROJECT_ID` | Blockfrost API key | - |
| `LINKAGE_SIGNING_KEY_PATH` | Path to signing key file | - |
| `LINKAGE_BATCH_SIZE_MIN` | Minimum operations per batch | 1 |
| `LINKAGE_BATCH_SIZE_MAX` | Maximum operations per batch | 5 |
| `LINKAGE_BATCH_INTERVAL` | Seconds between processing cycles | 30 |
| `LINKAGE_USE_OGMIOS` | Use Ogmios instead of Blockfrost | false |

## Datum Structure

The batcher parses Linkage fund datums that look like this:

```
IndexParameters
├── immutable_params
│   ├── index_tokens     # List of tokens in the fund
│   ├── index_name       # Name of the fund
│   ├── creator          # Public key hash of creator
│   ├── fund_token_factor # Base factor for calculations
│   └── royalty_factor   # Fee percentage numerator
└── accrued_royalty      # Uncollected royalty amount
```

## Redeemers

Each operation uses a different redeemer:

| Operation | Redeemer | CONSTR_ID |
|-----------|----------|-----------|
| Deposit | MintRedeemer | 2 |
| Withdraw | BurnRedeemer | 1 |
| Collect Royalty | RoyaltyWithdrawalRedeemer | 3 |

## Running Tests

```bash
# Run Linkage batcher tests
pytest tests/test_linkage_batcher.py -v

# Expected: 34 tests passed
```

Tests cover:
- Type creation and serialization
- Datum parsing and validation
- Configuration handling
- Operation queuing
- Batch processing flow

## Extending for Your Own DApp

This implementation shows how to build on the base batcher. To create your own:

1. **Define your types** in `types.py` - match your smart contract datums
2. **Create your config** in `config.py` - add your contract addresses
3. **Build your scanner** in `scanner.py` - parse your datum format
4. **Write your builder** in `builder.py` - construct your transactions
5. **Create your batcher** in `batcher.py` - wire everything together

The base batcher template provides:
- Node connections (Blockfrost, Ogmios)
- Request pooling and state management
- Transaction signing
- Database persistence

You just need to add your DApp-specific logic.

## Network Addresses (Preprod)

| Contract | Address |
|----------|---------|
| Fund Validator | `addr_test1wz5mnvsn3wupa2nvsg56s0rc30khypzxksrctmx2hysf2pcxt3ntm` |
| Reference Scripts | `addr_test1vplssjr39yv2mqavm2ap2wk0wz6jpge9afvdgjn7hn2l8ggfvxgws` |
| Factory | `addr_test1wzeacx32pnf53uawkwrvuyprwsm3ftfnp47mq8cvfs60ngcw2nxmr` |

## Troubleshooting

### "Fund not found"
The fund token name might be wrong. Run `python scripts/run_linkage_demo.py scan` to see all available funds.

### "Reference script UTXO not found"
The reference scripts might not be deployed. Check the reference script address has UTXOs with scripts attached.

### "Blockfrost API error"
Check your API key is correct and has access to the right network (preprod vs mainnet).

### "Transaction failed"
The fund UTXO might have changed since scanning. The batcher will retry automatically.

