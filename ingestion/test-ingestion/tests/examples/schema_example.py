#!/usr/bin/env python3
"""
Example: Schema-driven graph extraction from legal document
"""

import os
from dotenv import load_dotenv
from ingestion.shared.src.core.schema_extractor import SchemaExtractor
from ingestion.shared.src.core.schema_graph_builder import SchemaGraphBuilder
from ingestion.shared.src.core.graph_manager import GraphManager
from ingestion.shared.config.schemas import create_agreement_schema

load_dotenv()


def main():
    """Demo: Extract graph from legal text based on schema"""

    # Create schema
    schema = create_agreement_schema()
    print("Created schema:")
    schema.print_schema()

    # Example legal document text
    legal_text = """
    SERVICE AGREEMENT

    This Service Agreement (the "Agreement") is entered into effective January 1, 2025,
    between Acme Corporation ("Vendor"), a company located at 123 Business Ave, New York, NY,
    and Global Industries Inc. ("Client"), a company located at 456 Corporate Blvd, Los Angeles, CA.

    Section 1: Services
    Vendor shall provide software development services to Client for a duration of 12 months.

    Section 2: Compensation
    Client agrees to pay Vendor $50,000 USD per month for the services rendered.
    Payments are due within 30 days of invoice.

    Section 3: Deliverables
    Vendor is obligated to deliver:
    - Quarterly progress reports
    - Code repository access
    - Technical documentation

    Section 4: Confidentiality
    Both parties agree to maintain strict confidentiality of all proprietary information.
    This obligation survives for 2 years after agreement termination.

    Section 5: Term
    This Agreement begins on January 1, 2025 and expires on December 31, 2025.
    Either party may renew for an additional 12-month term.

    Signatures:
    Acme Corporation: John Smith, CEO
    Global Industries Inc.: Jane Doe, CTO
    """

    # Initialize extractor
    print("\nInitializing schema-based extractor...")
    extractor = SchemaExtractor(schema, openai_api_key=os.getenv("OPENAI_API_KEY"))

    # Extract entities and relationships
    print("\nExtracting entities and relationships from legal text...")
    extracted = extractor.extract_from_text(legal_text)

    print(f"\nExtracted {len(extracted['nodes'])} entities:")
    for node in extracted['nodes']:
        print(f"  - {node['label']}: {node['properties']}")

    print(f"\nExtracted {len(extracted['relationships'])} relationships:")
    for rel in extracted['relationships']:
        print(f"  - {rel['source_id']} -[{rel['type']}]-> {rel['target_id']}")

    # Build Neo4j graph
    print("\nBuilding Neo4j graph...")
    try:
        graph = GraphManager()

        # Create vector index (one-time setup)
        graph.create_vector_index()

        # Build graph from extracted data
        builder = SchemaGraphBuilder(graph, schema)
        stats = builder.build_graph(extracted)

        print(f"\nGraph building complete:")
        print(f"  Nodes created: {stats['nodes_created']}")
        print(f"  Relationships created: {stats['relationships_created']}")

        # Print graph stats
        builder.print_graph_stats()

        # Query the graph
        print("\nQuerying the graph...")
        cypher = """
        MATCH (org:Organization)-[r:PARTY_TO]->(agreement:Agreement)
        RETURN org.name as organization, agreement.title as agreement
        """
        results = graph.execute_query(cypher)
        print(f"\nParties to agreements:")
        for r in results:
            print(f"  {r['organization']} - {r['agreement']}")

        graph.close()

    except Exception as e:
        print(f"Error building graph: {e}")
        print("Make sure your Neo4j instance is running and credentials are correct.")


if __name__ == "__main__":
    main()
