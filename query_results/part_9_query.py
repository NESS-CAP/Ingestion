#!/usr/bin/env python3
"""
Query script to get all nodes under Part 9 and save results to file.
"""

import json
import sys
from pathlib import Path
import os

# Change to project root
os.chdir(Path(__file__).parent.parent)

# Add to path
sys.path.insert(0, str(Path.cwd()))

from ingestion.src.core.graph_manager import GraphManager
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def query_part_9():
    """Query all nodes under Part 9 and save to text file"""

    graph = GraphManager()

    try:
        # First, find all Part nodes to see what refs exist
        logger.info("Finding available Part nodes...")
        parts = graph.execute_query("MATCH (p:Part) RETURN p.ref, substring(p.text, 0, 100) LIMIT 20")

        logger.info(f"Available Parts: {parts}")

        # Query to get all nodes under Part 9 (Housing and Small Buildings)
        # Since Part nodes don't have direct children, query sections that start with 9.x
        logger.info("Querying all nodes under Part 9 (Housing and Small Buildings)...")

        results = graph.execute_query("""
            MATCH (s:Section)-[*0..3]->(n)
            WHERE s.ref STARTS WITH "node_1166" OR s.ref STARTS WITH "node_1167"
               OR s.text CONTAINS "9.1" OR s.text CONTAINS "9.2" OR s.text CONTAINS "9.3"
            RETURN
              labels(n)[0] as type,
              n.ref as ref,
              n.text as text,
              n.html_class as html_class,
              n.number as number
            ORDER BY n.ref
        """)

        # Format results
        output_lines = []
        output_lines.append("=" * 100)
        output_lines.append("PART 9 - ALL NODES QUERY RESULTS")
        output_lines.append("=" * 100)
        output_lines.append(f"\nTotal nodes found: {len(results)}\n")
        output_lines.append("-" * 100)

        # Group by type for better readability
        by_type = {}
        for result in results:
            node_type = result.get('type', 'Unknown')
            if node_type not in by_type:
                by_type[node_type] = []
            by_type[node_type].append(result)

        # Print summary
        output_lines.append("\nSUMMARY BY NODE TYPE:")
        output_lines.append("-" * 100)
        for node_type in sorted(by_type.keys()):
            count = len(by_type[node_type])
            output_lines.append(f"{node_type:20} : {count:5} nodes")

        output_lines.append("\n" + "=" * 100)
        output_lines.append("DETAILED RESULTS:")
        output_lines.append("=" * 100 + "\n")

        # Print detailed results grouped by type
        for node_type in sorted(by_type.keys()):
            output_lines.append(f"\n{'='*100}")
            output_lines.append(f"TYPE: {node_type}")
            output_lines.append(f"{'='*100}\n")

            for i, result in enumerate(by_type[node_type], 1):
                output_lines.append(f"[{i}] REF: {result.get('ref', 'N/A')}")
                output_lines.append(f"    NUMBER: {result.get('number', 'N/A')}")
                output_lines.append(f"    HTML_CLASS: {result.get('html_class', 'N/A')}")

                text = result.get('text', '')
                if text:
                    # Truncate long text
                    if len(text) > 200:
                        output_lines.append(f"    TEXT: {text[:200]}...")
                    else:
                        output_lines.append(f"    TEXT: {text}")

                output_lines.append("")

        # Write to file
        output_file = Path(__file__).parent / "part_9_results.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))

        logger.info(f"Results saved to {output_file}")

        # Also save as JSON for easy processing
        json_file = Path(__file__).parent / "part_9_results.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)

        logger.info(f"JSON results saved to {json_file}")

        print(f"\nQuery complete!")
        print(f"Text results: {output_file}")
        print(f"JSON results: {json_file}")
        print(f"Total nodes found: {len(results)}")

    except Exception as e:
        logger.error(f"Error querying Part 9: {e}")
        raise
    finally:
        graph.close()

if __name__ == "__main__":
    query_part_9()
