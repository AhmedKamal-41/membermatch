"""End-to-end pipeline orchestrator.

Chains the three stages (bronze ingest -> silver transform -> gold
materialization) against a single connection, then prints a summary
suitable for CI logs and interview demos.
"""

from __future__ import annotations

from pathlib import Path

from membermatch.golden import run_golden_materialization
from membermatch.ingest import get_connection, ingest_all, truncate_all_layers
from membermatch.transform import run_silver_transform

PROJECT_ROOT = Path(__file__).parent.parent.parent
SOURCES_DIR = PROJECT_ROOT / "data" / "sources"


def run_pipeline(conn, sources_dir: Path = SOURCES_DIR) -> dict:
    """Runs the full 3-stage pipeline against the given connection.
    Truncates all layers first for idempotency. Returns a stats dict
    with bronze/silver/gold row counts."""
    truncate_all_layers(conn)
    stats = {}
    stats["bronze"] = ingest_all(conn, sources_dir)
    stats["silver"] = {"row_count": run_silver_transform(conn)}
    stats["gold"] = run_golden_materialization(conn)
    return stats


def main() -> None:
    with get_connection() as conn:
        stats = run_pipeline(conn)
        bar = "=" * 60
        print(bar)
        print("MemberMatch pipeline complete")
        print(bar)
        bronze_total = sum(stats["bronze"].values())
        print(f"Bronze:    {bronze_total} rows")
        for source, count in stats["bronze"].items():
            print(f"  {source}: {count}")
        print(f"Silver:    {stats['silver']['row_count']} rows (typed/cleansed)")
        print(f"Gold:      {stats['gold']['gold_row_count']} rows (unique persons)")
        print(f"Lineage:   {stats['gold']['lineage_row_count']} rows")
        reduction = bronze_total / stats["gold"]["gold_row_count"]
        print(f"Reduction: {reduction:.2f}x")


if __name__ == "__main__":
    main()
