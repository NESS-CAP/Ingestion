"""
Main orchestration script: Stage 1 + Stage 2 pipeline
Reads PDF from pdf_read_with_GPT/data/
Outputs enriched JSON to ingestion/data/
"""

import logging
import json
import os
import sys
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Import local modules (stages 1 and 2 don't depend on ingestion core)
from stage1_extraction import Stage1Extractor
from stage2_enrichment import Stage2Enrichment

# Add root ingestion directory to path for imports BEFORE stage3 import
sys.path.insert(0, str(Path(__file__).parents[3]))
# Now we can safely import stage3 which depends on ingestion core modules
from stage3_neo4j_ingestion import Neo4jOBCIngester

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_pdf_path(input_dir: str = "data") -> Optional[str]:
    """Find PDF file in input directory"""
    input_path = Path(input_dir)

    if not input_path.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        return None

    # Look for PDF files
    pdf_files = list(input_path.glob("*.pdf"))

    if not pdf_files:
        logger.error(f"No PDF files found in {input_dir}")
        return None

    if len(pdf_files) > 1:
        logger.warning(f"Found {len(pdf_files)} PDFs, using first: {pdf_files[0].name}")

    return str(pdf_files[0])


def main():
    """Run the complete pipeline"""

    # Load environment variables
    load_dotenv()

    # Configuration
    input_dir = "data"  # pdf_read_with_GPT/data/
    output_dir = "../data"  # ingestion/data/ (relative to pdf_read_with_GPT)

    # Ensure output directory exists
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Get PDF file
    pdf_path = get_pdf_path(input_dir)
    if not pdf_path:
        logger.error("Cannot proceed without PDF file")
        return False

    logger.info(f"Using PDF: {pdf_path}")

    try:
        # ===== STAGE 1: LOCAL EXTRACTION =====
        logger.info("=" * 60)
        logger.info("STAGE 1: LOCAL EXTRACTION")
        logger.info("=" * 60)

        extractor = Stage1Extractor(pdf_path)
        extracted_data = extractor.extract()

        # Save stage 1 output for reference
        stage1_output = output_path / "stage1_extracted.json"
        with open(stage1_output, 'w', encoding='utf-8') as f:
            json.dump(extracted_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Stage 1 output saved to: {stage1_output}")

        # ===== STAGE 2: SEMANTIC ENRICHMENT =====
        logger.info("")
        logger.info("=" * 60)
        logger.info("STAGE 2: SEMANTIC ENRICHMENT WITH OPENAI VISION")
        logger.info("=" * 60)

        # Get OpenAI API key
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.error("OPENAI_API_KEY environment variable not set")
            logger.warning("Skipping Stage 2 enrichment, saving Stage 1 output only")
            enriched_data = extracted_data
        else:
            enricher = Stage2Enrichment(api_key=api_key)
            enriched_data = enricher.enrich(pdf_path, extracted_data)

        # ===== SAVE FINAL OUTPUT =====
        logger.info("")
        logger.info("=" * 60)
        logger.info("SAVING FINAL OUTPUT")
        logger.info("=" * 60)

        output_file = output_path / "obc_enriched.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(enriched_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Final output saved to: {output_file}")
        logger.info(f"  - {enriched_data['metadata']['total_sections']} sections")
        logger.info(f"  - {enriched_data['metadata']['total_tables']} tables")
        logger.info(f"  - {enriched_data['metadata']['total_images']} images")

        # Save metadata
        metadata_file = output_path / "obc_metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(enriched_data['metadata'], f, indent=2)

        # ===== STAGE 3: NEO4J INGESTION =====
        logger.info("")
        logger.info("=" * 60)
        logger.info("STAGE 3: NEO4J INGESTION")
        logger.info("=" * 60)

        try:
            from ingestion.src.core.graph_manager import GraphManager

            graph = GraphManager()
            ingester = Neo4jOBCIngester(graph)
            ingestion_stats = ingester.ingest(enriched_data)

            # Save ingestion stats
            stats_file = output_path / "ingestion_stats.json"
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(ingestion_stats, f, indent=2)

            logger.info(f"Ingestion completed:")
            logger.info(f"  - Nodes created: {ingestion_stats.get('nodes_created', 0)}")
            logger.info(f"  - Relationships created: {ingestion_stats.get('relationships_created', 0)}")
            if ingestion_stats.get('errors'):
                logger.warning(f"  - Errors: {len(ingestion_stats['errors'])}")

            graph.close()

        except Exception as e:
            logger.warning(f"Stage 3 (Neo4j ingestion) failed: {e}")
            logger.warning("Continuing without Neo4j ingestion")

        logger.info("")
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Output location: {output_path.absolute()}")

        return True

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
