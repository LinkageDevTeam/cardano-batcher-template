# cardano-batcher-template

### Streamlining Cardano DApp Development with Open-Source Batcher

**Project Catalyst Information**

* **Project ID:** 1200217
* **Proposal Link:** [Streamlining Cardano DApp Development with Open-Source Batcher](https://projectcatalyst.io/funds/12/cardano-open-developers/streamlining-cardano-dapp-development-with-open-source-batcher)

## Overview

This project provides a **reference batcher architecture** for Cardano decentralized applications (DApps).

Batchers are **off-chain services** that collect multiple user requests (such as token transfers, trades, or fund withdrawals) and combine them into a single on-chain transaction. This improves throughput, reduces blockchain load, and significantly lowers transaction fees by amortizing costs.

Currently, each DApp team typically develops its own batcher from scratch, which duplicates effort and results in inconsistent quality. This project aims to provide an **open-source, reusable, and modular batcher framework** that can be adapted across Cardano DApps.

## Testnet Transaction Evidence

The following transactions demonstrate the core functionality on **Cardano Preprod Testnet**:

### Transaction 1: Create Batch Request UTXOs
- **Hash:** `4c71a2f2d6ef58286ddb39cd2eb87364f4f86bd3e998caf7cfb024dcbdd3c3e1`
- **Explorer:** [View on CardanoScan](https://preprod.cardanoscan.io/transaction/4c71a2f2d6ef58286ddb39cd2eb87364f4f86bd3e998caf7cfb024dcbdd3c3e1)
- **Description:** Created 3 UTXOs (5 ADA each) simulating batch requests

### Transaction 2: Batch Processing Transaction
- **Hash:** `8e46cb6777202f79b2293e6d6172d052fa0cde30d7ae5559ab9a2f34bfd298c2`
- **Explorer:** [View on CardanoScan](https://preprod.cardanoscan.io/transaction/8e46cb6777202f79b2293e6d6172d052fa0cde30d7ae5559ab9a2f34bfd298c2)
- **Description:** Batch transaction processing identified requests


## Features

 **Request Identification** - Scan blockchain for batch requests at a script address  
**Multiple Matching Strategies** - FIFO, by-type, by-value, or greedy batch formation  
 **Transaction Construction** - Build and sign batching transactions  
 **State Management** - Persistent request and batch tracking with SQLite  
 **Multiple Backends** - Support for Blockfrost API and Ogmios  
 **Docker Support** - Containerized deployment  
 **Comprehensive Testing** - Test suite for core functionalities  

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/cardano-batcher-template.git
cd cardano-batcher-template

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e ".[dev]"
```

### Configuration

Copy the example environment file and configure:

```bash
cp env.example .env
```

Edit `.env` with your settings:

```bash
# Required
BATCHER_SCRIPT_ADDRESS=addr_test1qz...       # Address to monitor
BATCHER_BLOCKFROST_PROJECT_ID=preprodXXX...  # Get from blockfrost.io

# Optional
BATCHER_NETWORK=preprod
BATCHER_BATCH_SIZE_MIN=2
BATCHER_BATCH_SIZE_MAX=10
```

### Running the Batcher

```bash
# Run the batcher service
cardano-batcher run \
    --script-address addr_test1qz... \
    --network preprod \
    --blockfrost-project-id your_project_id \
    --signing-key ./keys/signing.skey

# One-time scan for requests
cardano-batcher scan \
    --script-address addr_test1qz... \
    --blockfrost-project-id your_project_id
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=batcher --cov-report=html

# Run specific test file
pytest tests/test_request_identification.py -v
pytest tests/test_batch_ordering.py -v
```

### Components

| Component | Description |
|-----------|-------------|
| **Scanner** | Monitors script address for new batch request UTXOs |
| **Request Pool** | Manages pending requests and their lifecycle |
| **Matcher** | Groups requests into batches using configurable strategies |
| **Transaction Builder** | Constructs and signs batching transactions |
| **Node Interface** | Abstracts blockchain access (Blockfrost/Ogmios) |
| **Database** | Persists state for recovery and monitoring |

## Matching Strategies

The batcher supports multiple strategies for grouping requests:

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **FIFO** | First-in-first-out ordering | Fair processing, simple DApps |
| **By Type** | Group by request type | DEXs with multiple order types |
| **By Value** | Group by value range | Fair fee distribution |
| **Greedy** | Maximize batch value | Fee optimization |
| **Custom** | Implement your own | DApp-specific logic |

```python
from batcher.engine.matcher import RequestMatcher, MatchingStrategy

# Use FIFO strategy
matcher = RequestMatcher(strategy=MatchingStrategy.FIFO)

# Use greedy strategy
matcher = RequestMatcher(strategy=MatchingStrategy.GREEDY)

# Custom strategy
class MyStrategy(MatchingStrategyBase):
    def match(self, requests, min_batch_size, max_batch_size):
        # Your custom logic
        pass

matcher = RequestMatcher(
    strategy=MatchingStrategy.CUSTOM,
    custom_strategy=MyStrategy()
)
```

## Customization

### Custom Datum Parser

Implement your own datum parser to handle your DApp's specific datum format:

```python
from batcher.engine.scanner import DatumParser

class MyDatumParser(DatumParser):
    def is_valid_request(self, datum):
        """Check if datum represents a valid request."""
        return datum.get("type") == "my_dapp_request"
    
    def parse_request_details(self, datum, utxo):
        """Extract request details from datum."""
        return (
            datum.get("requester_address"),
            datum.get("request_type"),
            {"amount": datum.get("amount")}
        )

# Use custom parser
from batcher.engine.scanner import RequestScanner

scanner = RequestScanner(
    node=node,
    script_address="addr_test1...",
    datum_parser=MyDatumParser()
)
```

### Custom Transaction Builder

Implement custom transaction logic for your DApp:

```python
from batcher.tx.builder import TransactionSpecBuilder, BatchTransactionSpec

class MyTxBuilder(TransactionSpecBuilder):
    def build_spec(self, batch, batcher_address, protocol_params):
        """Build transaction specification for your DApp."""
        # Your custom transaction logic
        inputs = [...]
        outputs = [...]
        
        return BatchTransactionSpec(
            inputs=inputs,
            outputs=outputs,
            script_inputs=[(utxo, redeemer), ...],
        )
```

## Docker Deployment

### Using Docker

```bash
# Build image
docker build -t cardano-batcher .

# Run container
docker run -d \
    --name batcher \
    -e BATCHER_SCRIPT_ADDRESS=addr_test1... \
    -e BATCHER_BLOCKFROST_PROJECT_ID=your_id \
    -v ./keys:/app/keys:ro \
    -v batcher-data:/app/data \
    cardano-batcher run \
    --script-address addr_test1... \
    --signing-key /app/keys/signing.skey
```

### Using Docker Compose

```bash
# Configure environment
cp env.example .env
# Edit .env with your settings

# Start batcher
docker-compose up -d

# View logs
docker-compose logs -f batcher

# Stop
docker-compose down
```

## Unit Test Overview

We have added multiple test to cver the core functionalities of our batcher software. The tests include

### Request Identification Tests (19 tests)
- Create requests from UTXOs
- Parse various datum formats
- Scan for new requests
- Detect spent/cancelled requests
- Full identification flow

### Batch Ordering Tests (25 tests)
- FIFO ordering
- Group by type
- Group by value range
- Greedy (maximize value)
- Custom strategies
- Pool integration

### Transaction Construction Tests (9 tests)
- Sign transactions
- Build batch transactions
- Submit to network
- Confirm transactions

Run tests to verify:

```bash
# Run all unit tests
pytest -v

# Expected output: 53 passed
```

## Design Objectives

The reference batcher is designed with the following goals:

* **Reusability** – adaptable across many DApp types (DEXs, fund managers, marketplaces).
* **Modularity** – separated into clear components (node integration, state manager, matching engine, etc.).
* **Reliability** – graceful handling of transaction failures and retries.
* **Security** – protection of private keys and safe transaction management.
* **Transparency** – open-source, well-documented, and community-driven.

## System Architecture

The batcher will consist of the following components:

* **Node Integration Layer** – connects to a Cardano node or API (e.g., Ogmios, Blockfrost).
* **Request Pool & State Manager** – tracks pending requests and updates spent/canceled UTXOs.
* **Batching Logic / Matching Engine** – decides how to group requests (e.g., trade matching, fund disbursements).
* **Transaction Construction & Signing** – builds, signs, and submits transactions to the Cardano blockchain.
* **Logging & Monitoring** – records operations and errors, with support for operator alerts.

The batcher follows a continuous cycle:

1. Listen for new requests (on-chain or via API).
2. Collect and store requests in a pool.
3. Match and group requests into a batch.
4. Construct and sign the transaction.
5. Submit to the Cardano network.
6. Confirm success and mark requests as completed.

## Tools & Libraries

### Programming Language

* **Python 3.10+** – chosen for simplicity, accessibility, and large community support.

### Cardano Node & Network Access

* **Cardano Node** – provides blockchain access for production deployment.
* **Cardano Testnet** – used for testing and development.
* **Blockfrost API** – quick access to UTXO and transaction data during development.
* **Ogmios** – lightweight WebSocket/JSON interface for efficient blockchain queries and transaction submission.

### Cardano Python SDK

* **PyCardano** – Python library for building, signing, and submitting transactions.

### Development & Deployment

* **GitHub** – source code hosting and collaboration.
* **Git** – version control.
* **Docker** – containerized deployment for consistent environments across machines.
* **pytest** – testing framework with async support.

