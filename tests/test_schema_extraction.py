#!/usr/bin/env python3
"""
Test script for schema extraction with OpenAI API calls.
Demonstrates:
1. Clearing the Neo4j database
2. Extracting entities and relationships using OpenAI
3. Building a knowledge graph from extracted data
4. Tracking API usage and costs
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, project_root)

from src.core.graph_manager import GraphManager
from src.core.schema_extractor import SchemaExtractor
from src.core.schema_graph_builder import SchemaGraphBuilder
from src.core.schema import Schema, NodeDef, PropertyDef, RelDef
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_sample_schema() -> Schema:
    """Create a sample legal document schema"""
    schema = Schema("Legal Document Schema")

    # Node types
    schema.add_node(NodeDef(
        label="Organization",
        properties=[
            PropertyDef("name", "string", required=True),
            PropertyDef("type", "string"),  # e.g., "Vendor", "Client", "Partner"
            PropertyDef("role", "string"),  # Role in the agreement
        ],
        description="Company or organization involved in agreement"
    ))

    schema.add_node(NodeDef(
        label="Agreement",
        properties=[
            PropertyDef("title", "string", required=True),
            PropertyDef("type", "string"),  # e.g., "Service Agreement", "NDA"
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


def clear_database():
    """Clear all data from Neo4j database"""
    logger.info("=" * 60)
    logger.info("CLEARING DATABASE")
    logger.info("=" * 60)

    graph = GraphManager()

    # Get pre-clear stats
    pre_stats = graph.get_graph_stats()
    logger.info(f"Before: {pre_stats['total_nodes']} nodes, {pre_stats['total_relationships']} relationships")

    # Clear all data
    graph.delete_all(confirm=True)

    # Verify clear
    post_stats = graph.get_graph_stats()
    logger.info(f"After: {post_stats['total_nodes']} nodes, {post_stats['total_relationships']} relationships")

    graph.close()
    logger.info("Database cleared successfully\n")


def run_schema_extraction_demo():
    """Run schema extraction demo with sample legal documents"""

    logger.info("=" * 60)
    logger.info("Neo4j Schema Extraction Demo with OpenAI")
    logger.info("=" * 60)
    logger.info(f"Started at: {datetime.now().isoformat()}\n")

    # Step 1: Clear database
    clear_database()

    # Step 2: Create schema
    logger.info("=" * 60)
    logger.info("SCHEMA DEFINITION")
    logger.info("=" * 60)

    schema = create_sample_schema()
    schema.print_schema()
    logger.info("")

    # Step 3: Initialize graph and extractor
    logger.info("=" * 60)
    logger.info("INITIALIZING COMPONENTS")
    logger.info("=" * 60)

    graph = GraphManager()
    extractor = SchemaExtractor(schema)
    builder = SchemaGraphBuilder(graph, schema)

    logger.info("✓ GraphManager initialized")
    logger.info("✓ SchemaExtractor initialized (using gpt-4o-mini)")
    logger.info("✓ SchemaGraphBuilder initialized\n")

    # Step 4: Sample legal documents for extraction
    sample_documents = [
        {
            "title": "Service Agreement",
            "content": """
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
        },
        {
            "title": "Non-Disclosure Agreement",
            "content": """
MUTUAL NON-DISCLOSURE AGREEMENT

This Non-Disclosure Agreement ("NDA") is entered into between:
Acme Corporation ("Disclosing Party") and Strategic Partners LLC ("Receiving Party"),
effective January 1, 2024.

1. Confidential Information
The Disclosing Party may disclose proprietary business information, trade secrets,
technical data, and strategic plans to the Receiving Party.

2. Obligations of Receiving Party
The Receiving Party shall:
- Restrict access to confidential information to authorized personnel only
- Protect confidential information using same standards applied to its own
- Return or destroy all confidential materials within 30 days of termination
- Maintain confidentiality for 5 years after disclosure

3. Permitted Disclosures
Confidential information may be disclosed if required by law, with prior notice
to Disclosing Party.
"""
        }
    ]

    # Step 5: Extract from documents
    logger.info("=" * 60)
    logger.info("SCHEMA EXTRACTION FROM DOCUMENTS")
    logger.info("=" * 60 + "\n")

    total_api_calls = 0
    extracted_data_all = {
        "nodes": [],
        "relationships": []
    }

    for doc_idx, document in enumerate(sample_documents, 1):
        logger.info(f"[Document {doc_idx}] {document['title']}")
        logger.info("-" * 60)

        try:
            # Extract entities and relationships from document
            logger.info(f"  Calling OpenAI (gpt-4o-mini) for extraction...")
            extracted = extractor.extract_from_text(document['content'])
            total_api_calls += 1

            logger.info(f"  ✓ Extraction complete")
            logger.info(f"    - Entities found: {len(extracted['nodes'])}")
            logger.info(f"    - Relationships found: {len(extracted['relationships'])}")

            # Log extracted entities
            for node in extracted['nodes']:
                logger.info(f"      * {node['label']}: {node.get('properties', {}).get('name', node.get('properties', {}).get('title', 'N/A'))}")

            # Add to consolidated results
            extracted_data_all['nodes'].extend(extracted['nodes'])
            extracted_data_all['relationships'].extend(extracted['relationships'])

        except Exception as e:
            logger.error(f"  ✗ Extraction failed: {e}")

        logger.info("")

    # Step 6: Build graph from extracted data
    logger.info("=" * 60)
    logger.info("BUILDING KNOWLEDGE GRAPH")
    logger.info("=" * 60 + "\n")

    if extracted_data_all['nodes']:
        build_result = builder.build_graph(extracted_data_all)

        logger.info(f"Graph construction complete:")
        logger.info(f"  - Nodes created: {build_result['nodes_created']}")
        logger.info(f"  - Relationships created: {build_result['relationships_created']}")
        logger.info("")
    else:
        logger.warning("No data extracted - skipping graph building")
        logger.info("")

    # Step 7: Display graph statistics
    logger.info("=" * 60)
    logger.info("FINAL GRAPH STATISTICS")
    logger.info("=" * 60)

    final_stats = graph.get_graph_stats()
    logger.info(f"Total Nodes: {final_stats['total_nodes']}")
    logger.info(f"Total Relationships: {final_stats['total_relationships']}")
    logger.info(f"\nNode types:")
    for node_type in final_stats.get('node_types', []):
        logger.info(f"  - {node_type['label']}: {node_type['count']}")
    logger.info("")

    # Step 8: API usage and cost summary
    logger.info("=" * 60)
    logger.info("API USAGE AND COST SUMMARY")
    logger.info("=" * 60)

    logger.info(f"\nOpenAI API Calls Made:")
    logger.info(f"  - Model: gpt-4o-mini")
    logger.info(f"  - Total calls: {total_api_calls}")
    logger.info(f"  - Per document: {total_api_calls / len(sample_documents):.1f} calls\n")

    # Cost estimation (as of 2024)
    # gpt-4o-mini: $0.15 per 1M input tokens, $0.60 per 1M output tokens
    # Rough estimates: ~500 input tokens per document, ~300 output tokens per document
    avg_input_tokens = 500
    avg_output_tokens = 300
    input_cost_per_1m = 0.15  # dollars
    output_cost_per_1m = 0.60  # dollars

    total_input_tokens = total_api_calls * avg_input_tokens
    total_output_tokens = total_api_calls * avg_output_tokens

    input_cost = (total_input_tokens / 1_000_000) * input_cost_per_1m
    output_cost = (total_output_tokens / 1_000_000) * output_cost_per_1m
    total_cost = input_cost + output_cost

    logger.info(f"Estimated Token Usage:")
    logger.info(f"  - Input tokens: {total_input_tokens:,} (~${input_cost:.6f})")
    logger.info(f"  - Output tokens: {total_output_tokens:,} (~${output_cost:.6f})")
    logger.info(f"  - Total estimated cost: ~${total_cost:.6f}")
    logger.info(f"\nNote: Actual costs may vary based on token count accuracy")

    # Cleanup
    graph.close()

    logger.info("\n" + "=" * 60)
    logger.info("Schema Extraction Demo Complete!")
    logger.info(f"Finished at: {datetime.now().isoformat()}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_schema_extraction_demo()
