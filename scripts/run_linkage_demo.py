#!/usr/bin/env python3
"""
Linkage Finance Batcher Demo Script.

This script demonstrates how to use the Linkage Finance batcher
to process deposits, withdrawals, and royalty collections.

Usage:
    python scripts/run_linkage_demo.py --help
    
    # Scan for funds
    python scripts/run_linkage_demo.py scan
    
    # Queue a deposit
    python scripts/run_linkage_demo.py deposit \
        --fund <fund_token_name> \
        --address <user_address> \
        --multiple 10
    
    # Queue a withdrawal
    python scripts/run_linkage_demo.py withdraw \
        --fund <fund_token_name> \
        --address <user_address> \
        --amount 1000
    
    # Run the batcher service
    python scripts/run_linkage_demo.py run

Environment Variables:
    LINKAGE_NETWORK: Network (preprod, mainnet, devnet)
    LINKAGE_BLOCKFROST_PROJECT_ID: Blockfrost API key
    LINKAGE_SIGNING_KEY_PATH: Path to signing key (optional)
"""

import argparse
import asyncio
import json
import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from batcher.linkage import (
    LinkageBatcher,
    LinkageConfig,
    LinkageRequestType,
)


async def scan_funds(config: LinkageConfig):
    """Scan and display all funds."""
    print(f"\n{'='*60}")
    print("Scanning for Linkage Finance Funds")
    print(f"Network: {config.network.value}")
    print(f"Fund Address: {config.fund_address}")
    print(f"{'='*60}\n")
    
    batcher = LinkageBatcher(config)
    
    try:
        await batcher.initialize()
        
        funds = batcher.get_cached_funds()
        
        if not funds:
            print("No funds found.")
            return
        
        print(f"Found {len(funds)} fund(s):\n")
        
        for i, fund in enumerate(funds, 1):
            print(f"Fund #{i}")
            print(f"  Token Name: {fund['fund_token_name']}")
            print(f"  Name: {fund.get('fund_name', 'N/A')}")
            print(f"  Royalty Factor: {fund['royalty_factor']}")
            print(f"  Fund Token Factor: {fund['fund_token_factor']}")
            print(f"  Accrued Royalty: {fund['accrued_royalty']}")
            print(f"  Request ID: {fund['request_id']}")
            print()
        
    finally:
        await batcher.shutdown()


async def deposit(
    config: LinkageConfig,
    fund_token_name: str,
    user_address: str,
    multiple: int,
):
    """Queue and process a deposit."""
    print(f"\n{'='*60}")
    print("Queueing Deposit Operation")
    print(f"{'='*60}\n")
    
    print(f"Fund: {fund_token_name[:32]}...")
    print(f"User: {user_address[:40]}...")
    print(f"Multiple: {multiple}")
    print()
    
    batcher = LinkageBatcher(config)
    
    try:
        await batcher.initialize()
        
        # Queue the deposit
        operation = batcher.queue_deposit(
            fund_token_name=fund_token_name,
            user_address=user_address,
            multiple=multiple,
        )
        
        print(f"Operation ID: {operation.operation_id}")
        print(f"Status: {operation.status}")
        
        # Process one cycle
        print("\nProcessing...")
        await batcher._run_cycle()
        
        # Check result
        result = batcher.get_operation_status(operation.operation_id)
        
        print(f"\nFinal Status: {result.status}")
        
        if result.status == "completed":
            print(f"Transaction Hash: {result.transaction_hash}")
        elif result.status == "awaiting_signature":
            print("Transaction built but requires user signature.")
            if "unsigned_tx_cbor" in result.metadata:
                print(f"Unsigned TX CBOR (first 100 chars): {result.metadata['unsigned_tx_cbor'][:100]}...")
        elif result.status == "failed":
            print(f"Error: {result.error_message}")
        
    finally:
        await batcher.shutdown()


async def withdraw(
    config: LinkageConfig,
    fund_token_name: str,
    user_address: str,
    amount: int,
):
    """Queue and process a withdrawal."""
    print(f"\n{'='*60}")
    print("Queueing Withdrawal Operation")
    print(f"{'='*60}\n")
    
    print(f"Fund: {fund_token_name[:32]}...")
    print(f"User: {user_address[:40]}...")
    print(f"Amount: {amount}")
    print()
    
    batcher = LinkageBatcher(config)
    
    try:
        await batcher.initialize()
        
        operation = batcher.queue_withdrawal(
            fund_token_name=fund_token_name,
            user_address=user_address,
            amount=amount,
        )
        
        print(f"Operation ID: {operation.operation_id}")
        print(f"Status: {operation.status}")
        
        print("\nProcessing...")
        await batcher._run_cycle()
        
        result = batcher.get_operation_status(operation.operation_id)
        
        print(f"\nFinal Status: {result.status}")
        
        if result.status == "completed":
            print(f"Transaction Hash: {result.transaction_hash}")
        elif result.status == "awaiting_signature":
            print("Transaction built but requires user signature.")
        elif result.status == "failed":
            print(f"Error: {result.error_message}")
        
    finally:
        await batcher.shutdown()


async def run_service(config: LinkageConfig):
    """Run the batcher as a continuous service."""
    print(f"\n{'='*60}")
    print("Starting Linkage Finance Batcher Service")
    print(f"Network: {config.network.value}")
    print(f"Batch Interval: {config.batch_interval_seconds}s")
    print(f"{'='*60}\n")
    
    batcher = LinkageBatcher(config)
    
    # Register callbacks
    def on_completed(op):
        print(f"✓ Operation {op.operation_id[:8]}... completed: {op.transaction_hash}")
    
    def on_failed(op):
        print(f"✗ Operation {op.operation_id[:8]}... failed: {op.error_message}")
    
    batcher.on_operation_completed(on_completed)
    batcher.on_operation_failed(on_failed)
    
    try:
        print("Initializing...")
        await batcher.initialize()
        
        print(f"Cached {len(batcher.get_cached_funds())} fund(s)")
        print("\nBatcher running. Press Ctrl+C to stop.\n")
        
        await batcher.start()
        
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await batcher.shutdown()
        print("Batcher stopped.")


async def show_stats(config: LinkageConfig):
    """Show batcher statistics."""
    print(f"\n{'='*60}")
    print("Linkage Batcher Statistics")
    print(f"{'='*60}\n")
    
    batcher = LinkageBatcher(config)
    
    try:
        await batcher.initialize()
        
        stats = batcher.get_stats()
        
        print(json.dumps(stats, indent=2))
        
    finally:
        await batcher.shutdown()


def main():
    parser = argparse.ArgumentParser(
        description="Linkage Finance Batcher Demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    
    parser.add_argument(
        "--network",
        choices=["preprod", "mainnet", "devnet"],
        default=os.getenv("LINKAGE_NETWORK", "preprod"),
        help="Cardano network to use",
    )
    parser.add_argument(
        "--blockfrost-id",
        default=os.getenv("LINKAGE_BLOCKFROST_PROJECT_ID"),
        help="Blockfrost project ID",
    )
    parser.add_argument(
        "--signing-key",
        default=os.getenv("LINKAGE_SIGNING_KEY_PATH"),
        help="Path to signing key",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Scan command
    subparsers.add_parser("scan", help="Scan for funds")
    
    # Deposit command
    deposit_parser = subparsers.add_parser("deposit", help="Queue a deposit")
    deposit_parser.add_argument("--fund", required=True, help="Fund token name (hex)")
    deposit_parser.add_argument("--address", required=True, help="User address")
    deposit_parser.add_argument("--multiple", type=int, required=True, help="Deposit multiple")
    
    # Withdraw command
    withdraw_parser = subparsers.add_parser("withdraw", help="Queue a withdrawal")
    withdraw_parser.add_argument("--fund", required=True, help="Fund token name (hex)")
    withdraw_parser.add_argument("--address", required=True, help="User address")
    withdraw_parser.add_argument("--amount", type=int, required=True, help="Fund tokens to burn")
    
    # Run command
    subparsers.add_parser("run", help="Run the batcher service")
    
    # Stats command
    subparsers.add_parser("stats", help="Show batcher statistics")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Build config
    from batcher.linkage.config import LinkageNetwork
    
    config = LinkageConfig(
        network=LinkageNetwork(args.network),
        blockfrost_project_id=args.blockfrost_id or "",
        signing_key_path=args.signing_key,
    )
    
    # Run command
    if args.command == "scan":
        asyncio.run(scan_funds(config))
    
    elif args.command == "deposit":
        asyncio.run(deposit(
            config,
            fund_token_name=args.fund,
            user_address=args.address,
            multiple=args.multiple,
        ))
    
    elif args.command == "withdraw":
        asyncio.run(withdraw(
            config,
            fund_token_name=args.fund,
            user_address=args.address,
            amount=args.amount,
        ))
    
    elif args.command == "run":
        asyncio.run(run_service(config))
    
    elif args.command == "stats":
        asyncio.run(show_stats(config))


if __name__ == "__main__":
    main()

