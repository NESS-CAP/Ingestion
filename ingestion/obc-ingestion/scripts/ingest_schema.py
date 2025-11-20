#!/usr/bin/env python3
"""
Schema-Based Document Ingestion Pipeline

Extracts entities and relationships from text using OpenAI's gpt-4o-mini
and builds a Neo4j knowledge graph based on a defined schema.

Usage:
    python3 scripts/ingest_schema.py
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# Add parent directory to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from ingestion.shared.src.core.graph_manager import GraphManager
from ingestion.shared.src.core.schema_extractor import SchemaExtractor
from ingestion.shared.src.core.schema_graph_builder import SchemaGraphBuilder
from ingestion.shared.src.core.schema import Schema, NodeDef, PropertyDef, RelDef
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_legal_schema() -> Schema:
    """Create a legal document schema for extraction"""
    schema = Schema("Legal Document Schema")

    # Node types
    schema.add_node(NodeDef(
        label="Organization",
        properties=[
            PropertyDef("name", "string", required=True),
            PropertyDef("type", "string"),
            PropertyDef("role", "string"),
        ],
        description="Company or organization involved in agreement"
    ))

    schema.add_node(NodeDef(
        label="Agreement",
        properties=[
            PropertyDef("title", "string", required=True),
            PropertyDef("type", "string"),
            PropertyDef("effective_date", "string"),
            PropertyDef("expiration_date", "string"),
        ],
        description="Legal agreement document"
    ))

    schema.add_node(NodeDef(
        label="Clause",
        properties=[
            PropertyDef("title", "string", required=True),
            PropertyDef("content", "string"),
            PropertyDef("section", "string"),
        ],
        description="Specific clause or provision in agreement"
    ))

    schema.add_node(NodeDef(
        label="Obligation",
        properties=[
            PropertyDef("description", "string", required=True),
            PropertyDef("party", "string"),
            PropertyDef("deadline", "string"),
        ],
        description="Specific obligation or requirement"
    ))

    # Relationship types
    schema.add_relationship(RelDef(
        type="PARTY_TO",
        source_label="Organization",
        target_label="Agreement",
        description="Organization is party to agreement"
    ))

    schema.add_relationship(RelDef(
        type="CONTAINS",
        source_label="Agreement",
        target_label="Clause",
        description="Agreement contains clause"
    ))

    schema.add_relationship(RelDef(
        type="DEFINES",
        source_label="Clause",
        target_label="Obligation",
        description="Clause defines obligation"
    ))

    schema.add_relationship(RelDef(
        type="APPLIES_TO",
        source_label="Obligation",
        target_label="Organization",
        description="Obligation applies to organization"
    ))

    return schema


def ingest_document(text: str, schema: Schema, graph: GraphManager, builder: SchemaGraphBuilder) -> dict:
    """
    Ingest a single document using schema extraction.

    Args:
        text: Document text to process
        schema: Schema definition for extraction
        graph: Graph manager for Neo4j
        builder: Graph builder for constructing the knowledge graph

    Returns:
        Statistics about ingestion (nodes and relationships created)
    """
    logger.info("=" * 60)
    logger.info("SCHEMA EXTRACTION")
    logger.info("=" * 60)

    try:
        # Extract entities and relationships
        extractor = SchemaExtractor(schema)
        logger.info("Calling OpenAI API for extraction...")
        extracted = extractor.extract_from_text(text)

        logger.info(f"✓ Extraction complete")
        logger.info(f"  - Entities found: {len(extracted['nodes'])}")
        logger.info(f"  - Relationships found: {len(extracted['relationships'])}")

        # Build graph from extracted data
        logger.info("\n" + "=" * 60)
        logger.info("BUILDING KNOWLEDGE GRAPH")
        logger.info("=" * 60)

        if extracted['nodes']:
            result = builder.build_graph(extracted)

            logger.info(f"Graph construction complete:")
            logger.info(f"  - Nodes created: {result['nodes_created']}")
            logger.info(f"  - Relationships created: {result['relationships_created']}")

            return result
        else:
            logger.warning("No entities extracted")
            return {"nodes_created": 0, "relationships_created": 0}

    except Exception as e:
        logger.error(f"Error during ingestion: {e}")
        raise


def load_documents_from_data_folder(data_dir: str = None) -> dict:
    """
    Load all text files from the data folder.

    Args:
        data_dir: Path to data directory (defaults to project_root/data)

    Returns:
        Dictionary mapping filenames to document text
    """
    if data_dir is None:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')

    documents = {}
    data_path = Path(data_dir)

    if not data_path.exists():
        logger.warning(f"Data directory not found: {data_dir}")
        return documents

    # Load all .txt files
    txt_files = list(data_path.glob('*.txt'))

    if not txt_files:
        logger.warning(f"No text files found in {data_dir}")
        return documents

    logger.info(f"Found {len(txt_files)} document(s) in {data_dir}")

    for txt_file in sorted(txt_files):
        try:
            with open(txt_file, 'r', encoding='utf-8') as f:
                text = f.read()
                documents[txt_file.name] = text
                logger.info(f"  - Loaded {txt_file.name} ({len(text)} characters)")
        except Exception as e:
            logger.error(f"Failed to load {txt_file.name}: {e}")

    return documents


def main():
    """Main ingestion workflow"""
    logger.info("=" * 60)
    logger.info("Neo4j Schema-Based Document Ingestion Pipeline")
    logger.info("=" * 60)
    logger.info(f"Started at: {datetime.now().isoformat()}\n")

    try:
        # Initialize schema and graph
        logger.info("Initializing pipeline...")
        schema = create_legal_schema()
        graph = GraphManager()
        builder = SchemaGraphBuilder(graph, schema)
        logger.info("✓ Pipeline initialized\n")

        # Print schema
        logger.info("=" * 60)
        logger.info("SCHEMA DEFINITION")
        logger.info("=" * 60)
        schema.print_schema()
        logger.info("")

        # Load documents from data folder
        logger.info("=" * 60)
        logger.info("LOADING DOCUMENTS")
        logger.info("=" * 60 + "\n")

        documents = load_documents_from_data_folder()

        # If no documents found, use sample for demonstration
        if not documents:
            logger.info("Using sample document for demonstration...\n")
            sample_text = """
        SERVICE AGREEMENT

        This Service Agreement (the "Agreement") is entered into as of January 1, 2024,
        by and between Acme Corporation, a Delaware corporation ("Client"), and
        TechVendor Inc., a California corporation ("Vendor").

        1. Services
        Vendor agrees to provide software development services including system design,
        implementation, and deployment. Services commence on January 15, 2024 and
        continue through December 31, 2024.

        2. Obligations
        Vendor shall:
        - Deliver weekly status reports by Friday of each week
        - Maintain 99.9% system uptime during business hours
        - Provide 24-hour emergency support for critical issues
        - Complete all deliverables by June 30, 2024

        Client shall:
        - Pay invoices within 30 days of receipt
        - Provide necessary access and information for project completion
        - Designate a project manager as primary contact

        3. Confidentiality
        Both parties agree to maintain confidentiality of proprietary information
        for a period of 3 years after agreement termination.
        """
            documents = {"sample_service_agreement.txt": sample_text}

        # Process each document
        logger.info("=" * 60)
        logger.info("PROCESSING DOCUMENTS")
        logger.info("=" * 60 + "\n")

        total_nodes = 0
        total_relationships = 0

        for doc_name, doc_text in documents.items():
            logger.info(f"Processing: {doc_name}")
            logger.info(f"  Size: {len(doc_text)} characters\n")

            result = ingest_document(doc_text, schema, graph, builder)
            total_nodes += result['nodes_created']
            total_relationships += result['relationships_created']

            logger.info("")

        # Display final statistics
        logger.info("=" * 60)
        logger.info("FINAL GRAPH STATISTICS")
        logger.info("=" * 60)

        stats = graph.get_graph_stats()
        logger.info(f"Total Nodes: {stats['total_nodes']}")
        logger.info(f"Total Relationships: {stats['total_relationships']}")
        logger.info(f"\nNode types:")
        for node_type in stats.get('node_types', []):
            logger.info(f"  - {node_type['label']}: {node_type['count']}")

        # Cleanup
        graph.close()

        logger.info("\n" + "=" * 60)
        logger.info("✓ Ingestion Complete!")
        logger.info(f"Finished at: {datetime.now().isoformat()}")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
