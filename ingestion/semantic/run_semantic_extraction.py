#!/usr/bin/env python3
"""
Semantic Extraction Pipeline for OBC Knowledge Graph.

WORKFLOW 2: Runs AFTER regex_html_generator_divB.py (Workflow 1)

This script:
1. Reads existing Sentence nodes from Neo4j
2. Uses LLM to extract semantic concepts from each sentence
3. Creates semantic nodes (Topic, BuildingType, etc.) if not exist
4. Creates APPLIES_TO relationships linking semantic nodes to sentences

Usage:
    python run_semantic_extraction.py [--limit N] [--batch-size N] [--dry-run]

Safe to run multiple times - uses MERGE for all operations.
"""

import argparse
import sys
from pathlib import Path


def ensure_project_root_on_path():
    """Add project root to path for imports."""
    current = Path(__file__).resolve()
    project_root = next(
        (p for p in current.parents if (p / "ingestion" / "__init__.py").exists()),
        None,
    )
    if project_root and str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))


ensure_project_root_on_path()

from ingestion.shared.config.settings import NEO4J_CONFIG, LLM_CONFIG
from ingestion.semantic.semantic_extractor import SemanticExtractor
from ingestion.semantic.semantic_linker import SemanticLinker


def print_progress(current: int, total: int, ref: str = ""):
    """Print progress bar."""
    pct = (current / total) * 100 if total > 0 else 0
    bar_len = 40
    filled = int(bar_len * current / total) if total > 0 else 0
    bar = "=" * filled + "-" * (bar_len - filled)
    ref_str = f" [{ref}]" if ref else ""
    print(f"\r[{bar}] {current}/{total} ({pct:.1f}%){ref_str}", end="", flush=True)


def run_extraction(
    limit: int = None,
    batch_size: int = 10,
    dry_run: bool = False,
    skip_linked: bool = True,
):
    """
    Run the semantic extraction pipeline.

    Args:
        limit: Optional limit on sentences to process
        batch_size: Number of sentences between saves
        dry_run: If True, extract but don't write to Neo4j
        skip_linked: Skip sentences that already have semantic links
    """
    print("=" * 60)
    print("SEMANTIC EXTRACTION PIPELINE - WORKFLOW 2")
    print("=" * 60)

    # Validate config
    if not NEO4J_CONFIG["uri"]:
        print("[ERROR] NEO4J_URI not configured. Check your .env file.", flush=True)
        return

    if not LLM_CONFIG["api_key"]:
        print("[ERROR] API_KEY not configured. Check your .env file.", flush=True)
        return

    print(f"\n[CONFIG]")
    print(f"  Neo4j: {NEO4J_CONFIG['uri'][:30]}...")
    print(f"  LLM Provider: {LLM_CONFIG['provider']}")
    print(f"  Limit: {limit or 'None (all sentences)'}")
    print(f"  Batch size: {batch_size}")
    print(f"  Dry run: {dry_run}")
    print(f"  Skip already linked: {skip_linked}")

    # Initialize components
    print("\n[INIT] Connecting to Neo4j...", flush=True)
    linker = SemanticLinker(
        uri=NEO4J_CONFIG["uri"],
        user=NEO4J_CONFIG["user"],
        password=NEO4J_CONFIG["password"],
        database=NEO4J_CONFIG.get("database", "neo4j"),
    )

    # Show initial stats
    print("\n[STATS] Before extraction:", flush=True)
    stats = linker.get_extraction_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")

    if not dry_run:
        # Initialize constraints and seed data
        print("\n[INIT] Creating constraints...", flush=True)
        linker.init_constraints()

        print("[INIT] Seeding semantic nodes...", flush=True)
        linker.seed_semantic_nodes()

    # Fetch sentences
    print("\n[FETCH] Loading sentences from Neo4j...", flush=True)
    sentences = linker.get_sentences_for_extraction(
        limit=limit,
        skip_already_linked=skip_linked,
    )
    print(f"[FETCH] Found {len(sentences)} sentences to process", flush=True)

    if not sentences:
        print("\n[DONE] No sentences to process.", flush=True)
        linker.close()
        return

    # Initialize extractor
    extractor = SemanticExtractor(
        provider=LLM_CONFIG["provider"],
        api_key=LLM_CONFIG["api_key"],
    )

    # Process sentences
    print("\n[EXTRACT] Starting extraction...", flush=True)
    processed = 0
    linked = 0
    errors = 0

    for i, sentence in enumerate(sentences):
        ref = sentence["ref"]
        text = sentence["text"]

        print_progress(i + 1, len(sentences), ref)

        try:
            # Extract concepts
            concepts = extractor.extract(text, ref)

            if concepts:
                # Count non-empty categories
                has_concepts = any(
                    len(concepts.get(k, [])) > 0
                    for k in ["topics", "building_types", "occupancy_classes",
                             "construction_types", "space_types"]
                )

                if has_concepts and not dry_run:
                    linker.link_concepts_to_node(ref, concepts)
                    linked += 1

            processed += 1

        except Exception as e:
            errors += 1
            print(f"\n[ERROR] Failed on {ref}: {e}", flush=True)

        # Progress update every batch
        if (i + 1) % batch_size == 0:
            print(f"\n  Checkpoint: {processed} processed, {linked} linked, {errors} errors", flush=True)

    print("\n")  # New line after progress bar

    # Final stats
    print("\n[STATS] After extraction:", flush=True)
    if not dry_run:
        stats = linker.get_extraction_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")

    print(f"\n[SUMMARY]")
    print(f"  Processed: {processed}")
    print(f"  Linked: {linked}")
    print(f"  Errors: {errors}")

    linker.close()
    print("\n[DONE] Extraction complete.", flush=True)


def main():
    parser = argparse.ArgumentParser(
        description="Run semantic extraction on OBC sentences"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="Limit number of sentences to process (default: all)"
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=10,
        help="Batch size for progress updates (default: 10)"
    )
    parser.add_argument(
        "--dry-run", "-d",
        action="store_true",
        help="Extract concepts but don't write to Neo4j"
    )
    parser.add_argument(
        "--reprocess",
        action="store_true",
        help="Reprocess sentences that already have semantic links"
    )

    args = parser.parse_args()

    run_extraction(
        limit=args.limit,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        skip_linked=not args.reprocess,
    )


if __name__ == "__main__":
    main()
