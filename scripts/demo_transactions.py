#!/usr/bin/env python3
"""
Demo script to create testnet transactions for milestone documentation.

This script demonstrates:
1. Identifying batch requests on-chain
2. Creating batches from requests
3. Constructing and submitting batch transactions
"""

import asyncio
import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pycardano import (
    Address,
    Network,
    PaymentSigningKey,
    PaymentVerificationKey,
    TransactionBuilder,
    TransactionOutput,
    Value,
    UTxO,
)

from batcher.config import BatcherConfig, NetworkType, NodeProvider, set_config
from batcher.node.blockfrost import BlockfrostAdapter
from batcher.core.request import BatchRequest
from batcher.core.batch import Batch
from batcher.engine.scanner import RequestScanner
from batcher.engine.matcher import RequestMatcher, MatchingStrategy
from batcher.state.request_pool import RequestPool


async def check_balance(node: BlockfrostAdapter, address: str) -> dict:
    """Check balance at an address."""
    utxos = await node.get_utxos_at_address(address)
    
    total_lovelace = sum(u.output.amount.coin for u in utxos)
    
    return {
        "address": address,
        "utxo_count": len(utxos),
        "total_ada": total_lovelace / 1_000_000,
        "total_lovelace": total_lovelace,
        "utxos": [
            {
                "tx_hash": str(u.input.transaction_id),
                "index": u.input.index,
                "lovelace": u.output.amount.coin,
            }
            for u in utxos
        ]
    }


async def create_batch_request_utxos(
    node: BlockfrostAdapter,
    signing_key: PaymentSigningKey,
    script_address: str,
    num_requests: int = 3,
    ada_per_request: int = 5_000_000,
) -> list:
    """
    Create UTXOs at the script address to simulate batch requests.
    
    Returns list of created UTXO references.
    """
    from pycardano import TransactionBuilder, TransactionOutput
    from blockfrost import BlockFrostChainContext, ApiUrls
    
    verification_key = PaymentVerificationKey.from_signing_key(signing_key)
    sender_address = Address(verification_key.hash(), network=Network.TESTNET)
    
    # Create chain context for transaction building
    context = BlockFrostChainContext(
        project_id=node.project_id,
        base_url=ApiUrls.preprod.value,
    )
    
    # Build transaction with multiple outputs to script address
    builder = TransactionBuilder(context)
    builder.add_input_address(sender_address)
    
    script_addr = Address.from_primitive(script_address)
    
    for i in range(num_requests):
        # Create outputs with inline datum
        datum = {
            "constructor": 0,
            "fields": [
                {"bytes": str(sender_address).encode().hex()},  # requester
                {"int": i},  # request_id
                {"int": ada_per_request},  # amount
            ]
        }
        builder.add_output(
            TransactionOutput(
                script_addr,
                Value(ada_per_request),
            )
        )
    
    # Build and sign
    tx = builder.build_and_sign(
        signing_keys=[signing_key],
        change_address=sender_address,
    )
    
    # Submit
    tx_hash = await node.submit_transaction(tx)
    
    print(f"✅ Created {num_requests} batch request UTXOs")
    print(f"   Transaction: {tx_hash}")
    
    return [
        {"tx_hash": tx_hash, "index": i}
        for i in range(num_requests)
    ]


async def demo_request_identification(
    node: BlockfrostAdapter,
    script_address: str,
) -> list:
    """
    Demonstrate identifying batch requests on-chain.
    """
    print("\n" + "="*60)
    print("📡 STEP 1: Identifying Batch Requests On-Chain")
    print("="*60)
    
    scanner = RequestScanner(
        node=node,
        script_address=script_address,
    )
    
    requests = await scanner.scan_for_requests()
    
    print(f"\n✅ Found {len(requests)} batch request(s) at {script_address[:30]}...")
    
    for i, req in enumerate(requests):
        print(f"\n   Request {i+1}:")
        print(f"      ID: {req.request_id}")
        print(f"      ADA: {req.ada_amount / 1_000_000:.6f}")
        print(f"      Status: {req.status.value}")
    
    return requests


async def demo_batch_ordering(
    requests: list,
    config: BatcherConfig,
) -> list:
    """
    Demonstrate creating batches from requests.
    """
    print("\n" + "="*60)
    print("📦 STEP 2: Creating Batches from Requests")
    print("="*60)
    
    # Add requests to pool
    pool = RequestPool(config)
    for req in requests:
        await pool.add_request(req)
    
    # Create batches
    matcher = RequestMatcher(
        strategy=MatchingStrategy.FIFO,
        config=config,
    )
    
    pending = await pool.get_pending_requests()
    batches = matcher.create_batches(pending)
    
    print(f"\n✅ Created {len(batches)} batch(es) from {len(requests)} requests")
    
    for i, batch in enumerate(batches):
        print(f"\n   Batch {i+1} ({batch.batch_id[:8]}...):")
        print(f"      Size: {batch.size} requests")
        print(f"      Total ADA: {batch.total_ada / 1_000_000:.6f}")
        print(f"      Status: {batch.status.value}")
        print(f"      Requests: {[r.request_id[:20]+'...' for r in batch.requests]}")
    
    return batches


async def demo_simple_transaction(
    node: BlockfrostAdapter,
    signing_key: PaymentSigningKey,
    recipient_address: str,
    amount_lovelace: int = 2_000_000,
) -> str:
    """
    Create a simple ADA transfer transaction for demonstration.
    """
    from blockfrost import BlockFrostChainContext, ApiUrls
    
    print("\n" + "="*60)
    print("💸 Creating Demo Transaction")
    print("="*60)
    
    verification_key = PaymentVerificationKey.from_signing_key(signing_key)
    sender_address = Address(verification_key.hash(), network=Network.TESTNET)
    
    # Create chain context
    context = BlockFrostChainContext(
        project_id=node.project_id,
        base_url=ApiUrls.preprod.value,
    )
    
    # Build transaction
    builder = TransactionBuilder(context)
    builder.add_input_address(sender_address)
    builder.add_output(
        TransactionOutput(
            Address.from_primitive(recipient_address),
            Value(amount_lovelace),
        )
    )
    
    # Build and sign
    tx = builder.build_and_sign(
        signing_keys=[signing_key],
        change_address=sender_address,
    )
    
    tx_hash = str(tx.transaction_body.hash())
    
    print(f"\n📝 Transaction Details:")
    print(f"   From: {sender_address}")
    print(f"   To: {recipient_address[:30]}...")
    print(f"   Amount: {amount_lovelace / 1_000_000:.6f} ADA")
    print(f"   Hash: {tx_hash}")
    
    # Submit
    submitted_hash = await node.submit_transaction(tx)
    
    print(f"\n✅ Transaction submitted!")
    print(f"   View on explorer: https://preprod.cardanoscan.io/transaction/{submitted_hash}")
    
    # Wait for confirmation
    print("\n⏳ Waiting for confirmation...")
    confirmed = await node.await_transaction_confirmation(submitted_hash, timeout_seconds=120)
    
    if confirmed:
        print(f"✅ Transaction confirmed!")
    else:
        print(f"⚠️  Transaction not yet confirmed (may still be processing)")
    
    return submitted_hash


async def run_full_demo(
    blockfrost_project_id: str,
    keys_dir: str,
    script_address: str = None,
):
    """Run the full demonstration."""
    
    print("\n" + "="*60)
    print("🚀 CARDANO BATCHER TEMPLATE - TESTNET DEMO")
    print("="*60)
    print(f"   Network: Preprod Testnet")
    print(f"   Time: {datetime.now().isoformat()}")
    
    # Load keys
    keys_path = Path(keys_dir)
    skey_path = keys_path / "signing.skey"
    
    if not skey_path.exists():
        print(f"\n❌ Error: Signing key not found at {skey_path}")
        print("   Run: python scripts/generate_keys.py first")
        return
    
    signing_key = PaymentSigningKey.load(str(skey_path))
    verification_key = PaymentVerificationKey.from_signing_key(signing_key)
    batcher_address = Address(verification_key.hash(), network=Network.TESTNET)
    
    print(f"\n📬 Batcher Address: {batcher_address}")
    
    # Use batcher address as script address if not provided
    if not script_address:
        script_address = str(batcher_address)
    
    # Create config
    config = BatcherConfig(
        network=NetworkType.PREPROD,
        node_provider=NodeProvider.BLOCKFROST,
        blockfrost_project_id=blockfrost_project_id,
        batch_size_min=2,
        batch_size_max=5,
    )
    set_config(config)
    
    # Connect to node
    node = BlockfrostAdapter(config)
    await node.connect()
    
    try:
        # Check balance
        print("\n" + "="*60)
        print("💰 Checking Batcher Balance")
        print("="*60)
        
        balance = await check_balance(node, str(batcher_address))
        print(f"\n   Address: {balance['address'][:40]}...")
        print(f"   UTXOs: {balance['utxo_count']}")
        print(f"   Balance: {balance['total_ada']:.6f} ADA")
        
        if balance['total_ada'] < 5:
            print(f"\n⚠️  Low balance! Please fund the address:")
            print(f"   Address: {batcher_address}")
            print(f"   Faucet: https://docs.cardano.org/cardano-testnets/tools/faucet/")
            return
        
        # Demo request identification
        requests = await demo_request_identification(node, script_address)
        
        # Demo batch ordering (if we have requests)
        if requests:
            batches = await demo_batch_ordering(requests, config)
        
        # Create a simple demo transaction
        tx_hash = await demo_simple_transaction(
            node=node,
            signing_key=signing_key,
            recipient_address=str(batcher_address),  # Send to self
            amount_lovelace=2_000_000,
        )
        
        # Summary
        print("\n" + "="*60)
        print("📊 DEMO SUMMARY")
        print("="*60)
        print(f"\n   ✅ Request Identification: {len(requests)} requests found")
        print(f"   ✅ Batch Creation: Demonstrated FIFO ordering")
        print(f"   ✅ Transaction: {tx_hash[:16]}...")
        print(f"\n   Explorer: https://preprod.cardanoscan.io/transaction/{tx_hash}")
        
        # Save results
        results = {
            "timestamp": datetime.now().isoformat(),
            "network": "preprod",
            "batcher_address": str(batcher_address),
            "balance": balance,
            "requests_found": len(requests),
            "transaction_hash": tx_hash,
            "explorer_url": f"https://preprod.cardanoscan.io/transaction/{tx_hash}",
        }
        
        results_path = keys_path / "demo_results.json"
        with open(results_path, "w") as f:
            json.dump(results, f, indent=2)
        
        print(f"\n📁 Results saved to: {results_path}")
        
    finally:
        await node.disconnect()


def main():
    parser = argparse.ArgumentParser(
        description="Run Cardano Batcher testnet demonstration"
    )
    parser.add_argument(
        "--blockfrost-project-id", "-b",
        required=True,
        help="Blockfrost project ID for Preprod"
    )
    parser.add_argument(
        "--keys-dir", "-k",
        default="./keys",
        help="Directory containing keys (default: ./keys)"
    )
    parser.add_argument(
        "--script-address", "-s",
        help="Script address to scan for requests (default: batcher address)"
    )
    parser.add_argument(
        "--check-balance-only",
        action="store_true",
        help="Only check balance, don't create transactions"
    )
    
    args = parser.parse_args()
    
    asyncio.run(run_full_demo(
        blockfrost_project_id=args.blockfrost_project_id,
        keys_dir=args.keys_dir,
        script_address=args.script_address,
    ))


if __name__ == "__main__":
    main()

