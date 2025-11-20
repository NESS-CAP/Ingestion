"""
Simplified HTML E-Laws Ingestion Pipeline

2-Stage Pipeline (no unnecessary stages):
1. Extract - HTML → JSON with clauses/sections
2. Ingest - JSON → Neo4j with fine-grained nodes

That's it. No middle enrichment stage.
"""

import logging
import sys
import json
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parents[3]))

from stage1_html_extraction import HTMLExtractor
from ingestion.shared.src.core.graph_manager import GraphManager
from ingestion.shared.src.core.embeddings import EmbeddingManager
from ingestion.shared.config.sources import ELAWS_OBC_HTML_URL

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


class SimplifiedIngestPipeline:
    """2-stage pipeline: Extract → Ingest"""

    def __init__(self, data_dir: str = "data", use_gpt: bool = True):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.use_gpt = use_gpt

    def run(self, skip_stages: list = None) -> Dict[str, Any]:
        """Run the 2-stage pipeline"""
        skip_stages = skip_stages or []
        results = {
            "stage1_extraction": None,
            "stage2_ingestion": None,
            "success": True,
            "errors": []
        }

        try:
            # Stage 1: Extract
            if 1 not in skip_stages:
                logger.info("=" * 80)
                logger.info("STAGE 1: Extract HTML → Structured Sections & Clauses")
                logger.info("=" * 80)

                stage1_result = self._run_stage1()
                results["stage1_extraction"] = stage1_result

                if not stage1_result["success"]:
                    logger.error("Stage 1 failed")
                    results["success"] = False
                    return results

            # Stage 2: Ingest to Neo4j
            if 2 not in skip_stages:
                logger.info("=" * 80)
                logger.info("STAGE 2: Ingest → Neo4j Fine-Grained Nodes")
                logger.info("=" * 80)

                stage2_result = self._run_stage2()
                results["stage2_ingestion"] = stage2_result

                if not stage2_result["success"]:
                    logger.error("Stage 2 failed")
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
        """Stage 1: Extract HTML to structured JSON"""
        result = {
            "success": False,
            "sections_extracted": 0,
            "clauses_extracted": 0,
            "output_file": None
        }

        try:
            extractor = HTMLExtractor(use_gpt=self.use_gpt)

            logger.info(f"Extracting from {ELAWS_OBC_HTML_URL}")
            extraction = extractor.extract_from_url(ELAWS_OBC_HTML_URL)

            output_file = self.data_dir / "extracted.json"
            extractor.save_extraction(extraction, str(output_file))

            result["success"] = True
            result["sections_extracted"] = extraction.get("total_sections", 0)
            result["clauses_extracted"] = extraction.get("total_clauses", 0)
            result["output_file"] = str(output_file)

            logger.info(f"Stage 1 complete: {result['clauses_extracted']} clauses in {result['sections_extracted']} sections")

            return result

        except Exception as e:
            logger.error(f"Stage 1 failed: {e}", exc_info=True)
            result["error"] = str(e)
            return result

    def _run_stage2(self) -> Dict[str, Any]:
        """Stage 2: Ingest extracted data to Neo4j"""
        result = {
            "success": False,
            "nodes_created": 0,
            "relationships_created": 0,
            "clauses_ingested": 0,
            "output_file": None
        }

        try:
            extracted_file = self.data_dir / "extracted.json"

            if not extracted_file.exists():
                raise FileNotFoundError(f"Extracted file not found: {extracted_file}")

            with open(extracted_file, "r", encoding="utf-8") as f:
                extraction = json.load(f)

            logger.info(f"Connecting to Neo4j and ingesting {extraction['total_sections']} sections")

            graph = GraphManager()
            embedding_manager = EmbeddingManager()

            stats = self._ingest_extraction_to_neo4j(
                extraction, graph, embedding_manager
            )

            graph.close()

            output_file = self.data_dir / "ingestion_stats.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(stats, f, indent=2)

            result["success"] = stats.get("success", False)
            result["nodes_created"] = stats.get("nodes_created", 0)
            result["relationships_created"] = stats.get("relationships_created", 0)
            result["clauses_ingested"] = stats.get("clauses_ingested", 0)
            result["output_file"] = str(output_file)

            logger.info(
                f"Stage 2 complete: {result['nodes_created']} nodes, "
                f"{result['clauses_ingested']} clauses ingested"
            )

            return result

        except Exception as e:
            logger.error(f"Stage 2 failed: {e}", exc_info=True)
            result["error"] = str(e)
            return result

    def _ingest_extraction_to_neo4j(
        self, extraction: Dict[str, Any], graph: GraphManager, em: EmbeddingManager
    ) -> Dict[str, Any]:
        """Ingest extracted data into Neo4j"""
        stats = {
            "success": True,
            "nodes_created": 0,
            "relationships_created": 0,
            "clauses_ingested": 0,
            "errors": []
        }

        try:
            # Create regulation and hierarchy
            reg_query = """
            CREATE (r:Regulation {
                regulation_id: '332/12',
                title: 'Building Code',
                abbreviation: 'O. Reg. 332/12',
                source_url: 'https://www.ontario.ca/laws/regulation/120332'
            })
            RETURN id(r) as neo4j_id
            """
            reg_result = graph.execute_query(reg_query, {})
            reg_id = reg_result[0]["neo4j_id"] if reg_result else None
            stats["nodes_created"] += 1

            # Create Division and Part (simplified)
            div_query = """
            CREATE (d:Division {division_id: 'A', title: 'Compliance and Objectives'})
            RETURN id(d) as neo4j_id
            """
            div_result = graph.execute_query(div_query, {})
            div_id = div_result[0]["neo4j_id"] if div_result else None
            stats["nodes_created"] += 1

            # Link division to regulation
            if reg_id and div_id:
                graph.execute_query(
                    "MATCH (r) WHERE id(r) = $reg_id MATCH (d) WHERE id(d) = $div_id CREATE (r)-[:HAS_DIVISION]->(d)",
                    {"reg_id": reg_id, "div_id": div_id}
                )
                stats["relationships_created"] += 1

            part_query = """
            CREATE (p:Part {part_number: '3', title: 'Fire Protection, Occupant Safety and Accessibility'})
            RETURN id(p) as neo4j_id
            """
            part_result = graph.execute_query(part_query, {})
            part_id = part_result[0]["neo4j_id"] if part_result else None
            stats["nodes_created"] += 1

            # Link part to division
            if div_id and part_id:
                graph.execute_query(
                    "MATCH (d) WHERE id(d) = $div_id MATCH (p) WHERE id(p) = $part_id CREATE (d)-[:HAS_PART]->(p)",
                    {"div_id": div_id, "part_id": part_id}
                )
                stats["relationships_created"] += 1

            # Process sections and clauses
            sections = extraction.get("sections", [])
            logger.info(f"Processing {len(sections)} sections")

            for section_idx, section in enumerate(sections):
                try:
                    section_number = section.get("section_number", "")
                    section_title = section.get("title", "")
                    clauses = section.get("extracted_clauses", [])

                    # Create section node
                    section_query = """
                    CREATE (s:Section {
                        section_number: $number,
                        title: $title
                    })
                    RETURN id(s) as neo4j_id
                    """
                    section_result = graph.execute_query(section_query, {
                        "number": section_number,
                        "title": section_title
                    })
                    section_id = section_result[0]["neo4j_id"] if section_result else None

                    if section_id:
                        stats["nodes_created"] += 1

                        # Link section to part
                        if part_id:
                            graph.execute_query(
                                "MATCH (p) WHERE id(p) = $part_id MATCH (s) WHERE id(s) = $section_id CREATE (p)-[:HAS_SECTION]->(s)",
                                {"part_id": part_id, "section_id": section_id}
                            )
                            stats["relationships_created"] += 1

                        # Create clause nodes
                        for clause_idx, clause in enumerate(clauses):
                            try:
                                clause_number = clause.get("number", "")
                                clause_text = clause.get("text", "")

                                # Generate embedding
                                embedding = em.embed_text(clause_text)

                                # Create clause node
                                clause_query = """
                                CREATE (c:Clause {
                                    clause_number: $number,
                                    text: $text,
                                    embedding: $embedding
                                })
                                RETURN id(c) as neo4j_id
                                """
                                clause_result = graph.execute_query(clause_query, {
                                    "number": clause_number,
                                    "text": clause_text[:1000],
                                    "embedding": embedding
                                })
                                clause_id = clause_result[0]["neo4j_id"] if clause_result else None

                                if clause_id:
                                    stats["nodes_created"] += 1
                                    stats["clauses_ingested"] += 1

                                    # Link clause to section
                                    graph.execute_query(
                                        "MATCH (s) WHERE id(s) = $section_id MATCH (c) WHERE id(c) = $clause_id CREATE (s)-[:HAS_CLAUSE]->(c)",
                                        {"section_id": section_id, "clause_id": clause_id}
                                    )
                                    stats["relationships_created"] += 1

                                    # Create nested items (subclauses)
                                    nested_items = clause.get("nested_items", [])
                                    for item in nested_items:
                                        item_number = item.get("number", "")
                                        item_text = item.get("text", "")

                                        item_embedding = em.embed_text(item_text)

                                        item_query = """
                                        CREATE (i:SubClause {
                                            number: $number,
                                            text: $text,
                                            embedding: $embedding
                                        })
                                        RETURN id(i) as neo4j_id
                                        """
                                        item_result = graph.execute_query(item_query, {
                                            "number": item_number,
                                            "text": item_text[:1000],
                                            "embedding": item_embedding
                                        })
                                        item_id = item_result[0]["neo4j_id"] if item_result else None

                                        if item_id:
                                            stats["nodes_created"] += 1

                                            # Link to parent clause
                                            graph.execute_query(
                                                "MATCH (c) WHERE id(c) = $clause_id MATCH (i) WHERE id(i) = $item_id CREATE (c)-[:HAS_SUBCLAUSE]->(i)",
                                                {"clause_id": clause_id, "item_id": item_id}
                                            )
                                            stats["relationships_created"] += 1

                            except Exception as e:
                                logger.warning(f"Error creating clause {clause_idx}: {e}")
                                stats["errors"].append(str(e))
                                continue

                except Exception as e:
                    logger.warning(f"Error processing section {section_idx}: {e}")
                    stats["errors"].append(str(e))
                    continue

            logger.info(f"Ingestion complete: {stats['nodes_created']} nodes, {stats['clauses_ingested']} clauses")

        except Exception as e:
            stats["success"] = False
            stats["errors"].append(str(e))
            logger.error(f"Ingestion failed: {e}", exc_info=True)

        return stats

    def _print_summary(self, results: Dict[str, Any]):
        """Print pipeline summary"""
        logger.info("PIPELINE SUMMARY")
        logger.info("-" * 80)

        if results["stage1_extraction"]:
            s1 = results["stage1_extraction"]
            logger.info(f"Stage 1 - HTML Extraction:")
            logger.info(f"  Sections: {s1.get('sections_extracted', 0)}")
            logger.info(f"  Clauses: {s1.get('clauses_extracted', 0)}")
            logger.info(f"  Output: {s1.get('output_file', 'N/A')}")

        if results["stage2_ingestion"]:
            s2 = results["stage2_ingestion"]
            logger.info(f"Stage 2 - Neo4j Ingestion:")
            logger.info(f"  Nodes created: {s2.get('nodes_created', 0)}")
            logger.info(f"  Clauses ingested: {s2.get('clauses_ingested', 0)}")
            logger.info(f"  Relationships: {s2.get('relationships_created', 0)}")
            logger.info(f"  Output: {s2.get('output_file', 'N/A')}")

        logger.info("-" * 80)
        logger.info(f"Overall Success: {results['success']}")


def main():
    """Run the simplified pipeline"""
    import argparse

    parser = argparse.ArgumentParser(description="Simplified HTML E-Laws Ingestion Pipeline")
    parser.add_argument(
        "--skip-stages",
        type=int,
        nargs="+",
        default=[],
        help="Stages to skip (1 or 2)"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data",
        help="Directory for input/output files"
    )
    parser.add_argument(
        "--no-gpt",
        action="store_true",
        help="Skip GPT and use structural extraction only (faster, less accurate)"
    )

    args = parser.parse_args()

    pipeline = SimplifiedIngestPipeline(
        data_dir=args.data_dir,
        use_gpt=not args.no_gpt
    )
    results = pipeline.run(skip_stages=args.skip_stages)

    return 0 if results["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
