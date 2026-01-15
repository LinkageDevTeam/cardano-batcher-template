"""
Command-line interface for the Cardano Batcher.

Provides commands for running and managing the batcher service.
"""

import argparse
import asyncio
import sys
from pathlib import Path

import structlog

from batcher import __version__
from batcher.config import BatcherConfig, NetworkType, NodeProvider, set_config
from batcher.core.batcher import Batcher
from batcher.engine.matcher import MatchingStrategy


def setup_logging(level: str = "INFO", json_format: bool = False) -> None:
    """Configure structured logging."""
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer() if json_format
            else structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    import logging
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level.upper()),
        stream=sys.stdout,
    )


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        prog="cardano-batcher",
        description="Reference batcher for Cardano DApps",
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Run command
    run_parser = subparsers.add_parser("run", help="Run the batcher service")
    run_parser.add_argument(
        "--script-address",
        required=True,
        help="Script address to monitor for batch requests",
    )
    run_parser.add_argument(
        "--network",
        choices=["mainnet", "preprod", "preview"],
        default="preprod",
        help="Cardano network (default: preprod)",
    )
    run_parser.add_argument(
        "--provider",
        choices=["blockfrost", "ogmios"],
        default="blockfrost",
        help="Node provider (default: blockfrost)",
    )
    run_parser.add_argument(
        "--blockfrost-project-id",
        help="Blockfrost project ID",
    )
    run_parser.add_argument(
        "--signing-key",
        help="Path to signing key file",
    )
    run_parser.add_argument(
        "--batch-min",
        type=int,
        default=2,
        help="Minimum batch size (default: 2)",
    )
    run_parser.add_argument(
        "--batch-max",
        type=int,
        default=10,
        help="Maximum batch size (default: 10)",
    )
    run_parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Batch interval in seconds (default: 30)",
    )
    run_parser.add_argument(
        "--strategy",
        choices=["fifo", "by_type", "by_value", "greedy"],
        default="fifo",
        help="Matching strategy (default: fifo)",
    )
    run_parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    run_parser.add_argument(
        "--log-json",
        action="store_true",
        help="Output logs in JSON format",
    )
    
    # Scan command (one-time scan)
    scan_parser = subparsers.add_parser("scan", help="Scan for requests once")
    scan_parser.add_argument(
        "--script-address",
        required=True,
        help="Script address to scan",
    )
    scan_parser.add_argument(
        "--network",
        choices=["mainnet", "preprod", "preview"],
        default="preprod",
    )
    scan_parser.add_argument(
        "--blockfrost-project-id",
        help="Blockfrost project ID",
    )
    scan_parser.add_argument(
        "--log-level",
        default="INFO",
    )
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Show batcher status")
    
    return parser


async def run_batcher(args: argparse.Namespace) -> None:
    """Run the batcher service."""
    # Create configuration
    config = BatcherConfig(
        network=NetworkType(args.network),
        node_provider=NodeProvider(args.provider),
        blockfrost_project_id=args.blockfrost_project_id,
        wallet_signing_key_path=args.signing_key,
        batch_size_min=args.batch_min,
        batch_size_max=args.batch_max,
        batch_interval_seconds=args.interval,
        request_script_address=args.script_address,
        log_level=args.log_level,
        log_json=args.log_json,
    )
    set_config(config)
    
    # Create batcher
    strategy = MatchingStrategy(args.strategy)
    batcher = Batcher(
        script_address=args.script_address,
        config=config,
        matching_strategy=strategy,
    )
    
    # Set up signal handlers
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        print("\nShutting down...")
        batcher.stop()
    
    try:
        import signal
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)
    except NotImplementedError:
        pass  # Signals not available on Windows
    
    # Run batcher
    print(f"Starting Cardano Batcher v{__version__}")
    print(f"Network: {args.network}")
    print(f"Script Address: {args.script_address[:30]}...")
    print(f"Strategy: {args.strategy}")
    print()
    
    await batcher.start()


async def scan_requests(args: argparse.Namespace) -> None:
    """One-time scan for requests."""
    from batcher.node.blockfrost import BlockfrostAdapter
    from batcher.engine.scanner import RequestScanner
    
    config = BatcherConfig(
        network=NetworkType(args.network),
        blockfrost_project_id=args.blockfrost_project_id,
    )
    
    node = BlockfrostAdapter(config)
    await node.connect()
    
    scanner = RequestScanner(
        node=node,
        script_address=args.script_address,
        config=config,
    )
    
    print(f"Scanning for requests at {args.script_address[:30]}...")
    print()
    
    requests = await scanner.scan_for_requests()
    
    if not requests:
        print("No batch requests found.")
    else:
        print(f"Found {len(requests)} batch request(s):")
        print()
        for req in requests:
            print(f"  Request ID: {req.request_id}")
            print(f"    ADA Amount: {req.ada_amount / 1_000_000:.6f} ADA")
            print(f"    Type: {req.request_type or 'unknown'}")
            print(f"    Requester: {req.requester_address[:30] + '...' if req.requester_address else 'unknown'}")
            print()
    
    await node.disconnect()


def main() -> None:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Setup logging
    log_level = getattr(args, "log_level", "INFO")
    log_json = getattr(args, "log_json", False)
    setup_logging(log_level, log_json)
    
    # Run appropriate command
    if args.command == "run":
        asyncio.run(run_batcher(args))
    elif args.command == "scan":
        asyncio.run(scan_requests(args))
    elif args.command == "status":
        print("Status command not yet implemented")
        sys.exit(1)


if __name__ == "__main__":
    main()


