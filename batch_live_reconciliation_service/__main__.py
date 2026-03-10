"""Allow running as: python -m batch_live_reconciliation_service"""

from batch_live_reconciliation_service.cli.main import main

if __name__ == "__main__":
    raise SystemExit(main())
