"""
HTML E-Laws Ingestion Pipeline Orchestrator

3-Stage Pipeline:
1. Load HTML and chunk using LangChain's semantic header splitter
2. Extract fine-grained clauses, subclauses, items using GPT-4o
3. Ingest into Neo4j with individual nodes for each clause/item

Results:
- Thousands of granular nodes instead of one per section
- Full clause-level queryability
- Embeddings for semantic search
- Proper hierarchical relationships
"""

import logging
import sys
import json
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Add root to path
sys.path.insert(0, str(Path(__file__).parents[3]))

from stage1_html_loader import HTMLLoader
from stage2_gpt_extraction import GPTContentExtractor
from stage3_neo4j_html_ingestion import Neo4jHTMLIngester
from ingestion.shared.src.core.graph_manager import GraphManager
from ingestion.shared.config.sources import ELAWS_OBC_HTML_URL

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment
load_dotenv()


class HTMLIngestPipeline:
    """Orchestrates the 3-stage HTML ingestion pipeline"""

    def __init__(self, data_dir: str = "data"):
        """
        Initialize pipeline.

        Args:
            data_dir: Directory for input/output files
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

    def run(self, skip_stages: list = None) -> Dict[str, Any]:
        """
        Run the complete pipeline.

        Args:
            skip_stages: List of stage numbers to skip (1, 2, or 3)

        Returns:
            Summary of pipeline execution
        """
        skip_stages = skip_stages or []
        results = {
            "stage1": None,
            "stage2": None,
            "stage3": None,
            "success": True,
            "errors": []
        }

        try:
            # Stage 1: Load and chunk HTML
            if 1 not in skip_stages:
                logger.info("=" * 80)
                logger.info("STAGE 1: Loading HTML and chunking with semantic awareness")
                logger.info("=" * 80)

                stage1_result = self._run_stage1()
                results["stage1"] = stage1_result

                if not stage1_result["success"]:
                    logger.error("Stage 1 failed")
                    results["success"] = False
                    return results

            # Stage 2: GPT extraction
            if 2 not in skip_stages:
                logger.info("=" * 80)
                logger.info("STAGE 2: Extracting fine-grained clauses with GPT-4o")
                logger.info("=" * 80)

                stage2_result = self._run_stage2()
                results["stage2"] = stage2_result

                if not stage2_result["success"]:
                    logger.error("Stage 2 failed")
                    results["success"] = False
                    return results

            # Stage 3: Neo4j ingestion
            if 3 not in skip_stages:
                logger.info("=" * 80)
                logger.info("STAGE 3: Ingesting fine-grained data into Neo4j")
                logger.info("=" * 80)

                stage3_result = self._run_stage3()
                results["stage3"] = stage3_result

                if not stage3_result["success"]:
                    logger.error("Stage 3 failed")
                    results["success"] = False
                    return results

            # Summary
            logger.info("=" * 80)
            logger.info("PIPELINE COMPLETE")
            logger.info("=" * 80)
            self._print_summary(results)

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            results["success"] = False
            results["errors"].append(str(e))

        return results

    def _run_stage1(self) -> Dict[str, Any]:
        """Run Stage 1: HTML loading and chunking"""
        result = {
            "success": False,
            "chunks_created": 0,
            "total_content_size": 0,
            "output_file": None
        }

        try:
            loader = HTMLLoader(url=ELAWS_OBC_HTML_URL)

            logger.info(f"Loading HTML from {ELAWS_OBC_HTML_URL}")
            chunks = loader.load_from_url()

            output_file = self.data_dir / "html_chunks.json"
            loader.save_chunks(chunks, str(output_file))

            result["success"] = True
            result["chunks_created"] = sum(c.get("total_chunks", 0) for c in chunks)
            result["output_file"] = str(output_file)

            logger.info(f"Stage 1 complete: {result['chunks_created']} chunks created")

            return result

        except Exception as e:
            logger.error(f"Stage 1 failed: {e}", exc_info=True)
            result["error"] = str(e)
            return result

    def _run_stage2(self) -> Dict[str, Any]:
        """Run Stage 2: GPT-based extraction"""
        result = {
            "success": False,
            "documents_processed": 0,
            "clauses_extracted": 0,
            "definitions_extracted": 0,
            "output_file": None
        }

        try:
            chunks_file = self.data_dir / "html_chunks.json"

            if not chunks_file.exists():
                raise FileNotFoundError(f"Chunks file not found: {chunks_file}")

            with open(chunks_file, "r", encoding="utf-8") as f:
                chunks_data = json.load(f)

            chunks = chunks_data.get("documents", [])
            logger.info(f"Processing {len(chunks)} chunks")

            extractor = GPTContentExtractor()
            extracted = extractor.extract_batch(chunks)

            output_file = self.data_dir / "gpt_extracted.json"
            extractor.save_extracted(extracted, str(output_file))

            # Calculate stats
            total_clauses = 0
            total_definitions = 0

            for doc in extracted:
                for chunk in doc.get("extracted_chunks", []):
                    extracted_data = chunk.get("extracted", {})
                    total_clauses += len(extracted_data.get("clauses", []))
                    total_definitions += len(extracted_data.get("definitions", []))

            result["success"] = True
            result["documents_processed"] = len(extracted)
            result["clauses_extracted"] = total_clauses
            result["definitions_extracted"] = total_definitions
            result["output_file"] = str(output_file)

            logger.info(
                f"Stage 2 complete: {total_clauses} clauses, "
                f"{total_definitions} definitions extracted"
            )

            return result

        except Exception as e:
            logger.error(f"Stage 2 failed: {e}", exc_info=True)
            result["error"] = str(e)
            return result

    def _run_stage3(self) -> Dict[str, Any]:
        """Run Stage 3: Neo4j ingestion"""
        result = {
            "success": False,
            "nodes_created": 0,
            "relationships_created": 0,
            "clauses_created": 0,
            "output_file": None
        }

        try:
            extracted_file = self.data_dir / "gpt_extracted.json"

            if not extracted_file.exists():
                raise FileNotFoundError(f"Extracted file not found: {extracted_file}")

            with open(extracted_file, "r", encoding="utf-8") as f:
                extracted_data = json.load(f)

            documents = extracted_data if isinstance(extracted_data, list) else [extracted_data]

            logger.info(f"Connecting to Neo4j and ingesting {len(documents)} documents")

            graph = GraphManager()
            ingester = Neo4jHTMLIngester(graph)
            stats = ingester.ingest(documents)

            graph.close()

            output_file = self.data_dir / "ingestion_stats_html.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(stats, f, indent=2)

            result["success"] = stats.get("success", False)
            result["nodes_created"] = stats.get("nodes_created", 0)
            result["relationships_created"] = stats.get("relationships_created", 0)
            result["clauses_created"] = stats.get("clauses_created", 0)
            result["output_file"] = str(output_file)

            if stats.get("errors"):
                result["errors"] = stats["errors"]
                logger.warning(f"Ingestion completed with {len(stats['errors'])} errors")

            logger.info(
                f"Stage 3 complete: {result['nodes_created']} nodes, "
                f"{result['clauses_created']} clauses created"
            )

            return result

        except Exception as e:
            logger.error(f"Stage 3 failed: {e}", exc_info=True)
            result["error"] = str(e)
            return result

    def _print_summary(self, results: Dict[str, Any]):
        """Print pipeline summary"""
        logger.info("PIPELINE SUMMARY")
        logger.info("-" * 80)

        if results["stage1"]:
            s1 = results["stage1"]
            logger.info(f"Stage 1 - HTML Loading:")
            logger.info(f"  Chunks created: {s1.get('chunks_created', 0)}")
            logger.info(f"  Output: {s1.get('output_file', 'N/A')}")

        if results["stage2"]:
            s2 = results["stage2"]
            logger.info(f"Stage 2 - GPT Extraction:")
            logger.info(f"  Clauses extracted: {s2.get('clauses_extracted', 0)}")
            logger.info(f"  Definitions extracted: {s2.get('definitions_extracted', 0)}")
            logger.info(f"  Output: {s2.get('output_file', 'N/A')}")

        if results["stage3"]:
            s3 = results["stage3"]
            logger.info(f"Stage 3 - Neo4j Ingestion:")
            logger.info(f"  Nodes created: {s3.get('nodes_created', 0)}")
            logger.info(f"  Clauses created: {s3.get('clauses_created', 0)}")
            logger.info(f"  Relationships: {s3.get('relationships_created', 0)}")
            logger.info(f"  Output: {s3.get('output_file', 'N/A')}")

        logger.info("-" * 80)
        logger.info(f"Overall Success: {results['success']}")


def main():
    """Run the pipeline"""
    import argparse

    parser = argparse.ArgumentParser(description="HTML E-Laws Ingestion Pipeline")
    parser.add_argument(
        "--skip-stages",
        type=int,
        nargs="+",
        default=[],
        help="Stages to skip (1, 2, or 3)"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data",
        help="Directory for input/output files"
    )

    args = parser.parse_args()

    pipeline = HTMLIngestPipeline(data_dir=args.data_dir)
    results = pipeline.run(skip_stages=args.skip_stages)

    return 0 if results["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
