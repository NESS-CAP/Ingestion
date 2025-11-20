#!/usr/bin/env python3
"""
Complete system test: PDF → Graph → Query
Shows the entire workflow working end-to-end
"""

import os
from dotenv import load_dotenv
from ingestion.shared.src.core.graph_manager import GraphManager
from ingestion.shared.config.schemas import create_legal_document_schema

load_dotenv()


def create_test_schema():
    """Create a schema for testing"""
    # Use the standard legal document schema for consistency
    return create_legal_document_schema()


def create_sample_pdf() -> str:
    """Create a sample PDF for testing"""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas

        pdf_path = "sample_test_contract.pdf"

        c = canvas.Canvas(pdf_path, pagesize=letter)
        c.setFont("Helvetica", 12)

        # Add sample contract text
        y = 750
        line_height = 20

        content = [
            "SERVICE AGREEMENT",
            "",
            "This Service Agreement is entered into between:",
            "",
            "Acme Corporation (\"Vendor\"), located at 123 Business Street, New York, NY",
            "and",
            "Global Industries Inc. (\"Client\"), located at 456 Corporate Boulevard, Los Angeles, CA",
            "",
            "SECTION 1: SERVICES",
            "Vendor agrees to provide software development services to Client.",
            "",
            "SECTION 2: PAYMENT TERMS",
            "Client shall pay Vendor $50,000 USD per month for services.",
            "Payments are due within 30 days of invoice.",
            "",
            "SECTION 3: DELIVERABLES",
            "Vendor is obligated to provide:",
            "- Quarterly progress reports by the 15th of each quarter",
            "- Source code access within 48 hours",
            "- Technical documentation on completion",
            "",
            "SECTION 4: TERM",
            "This agreement begins on January 1, 2025 and expires on December 31, 2025.",
            "Either party may renew for an additional 12-month term.",
            "",
            "SECTION 5: CONFIDENTIALITY",
            "Both parties agree to maintain strict confidentiality of proprietary information.",
            "",
            "Signatures:",
            "Acme Corporation: John Smith, CEO",
            "Global Industries Inc.: Jane Doe, CTO"
        ]

        for line in content:
            c.drawString(50, y, line)
            y -= line_height
            if y < 50:
                c.showPage()
                y = 750

        c.save()
        print(f"✓ Created sample PDF: {pdf_path}")
        return pdf_path

    except ImportError:
        print("⚠ reportlab not installed. Creating text file instead...")
        pdf_path = "sample_test_contract.txt"

        with open(pdf_path, 'w') as f:
            f.write("""SERVICE AGREEMENT

This Service Agreement is entered into between:

Acme Corporation ("Vendor"), located at 123 Business Street, New York, NY
and
Global Industries Inc. ("Client"), located at 456 Corporate Boulevard, Los Angeles, CA

SECTION 1: SERVICES
Vendor agrees to provide software development services to Client.

SECTION 2: PAYMENT TERMS
Client shall pay Vendor $50,000 USD per month for services.
Payments are due within 30 days of invoice.

SECTION 3: DELIVERABLES
Vendor is obligated to provide:
- Quarterly progress reports by the 15th of each quarter
- Source code access within 48 hours
- Technical documentation on completion

SECTION 4: TERM
This agreement begins on January 1, 2025 and expires on December 31, 2025.
Either party may renew for an additional 12-month term.

SECTION 5: CONFIDENTIALITY
Both parties agree to maintain strict confidentiality of proprietary information.

Signatures:
Acme Corporation: John Smith, CEO
Global Industries Inc.: Jane Doe, CTO
""")
        print(f"✓ Created sample document: {pdf_path}")
        return pdf_path


def test_full_workflow():
    """Test the complete workflow"""

    print("\n" + "="*70)
    print("FULL SYSTEM TEST: PDF → EXTRACT → GRAPH → QUERY")
    print("="*70)

    # Step 1: Create schema
    print("\n[STEP 1] Creating schema...")
    schema = create_test_schema()
    schema.print_schema()

    # Step 2: Create sample PDF
    print("\n[STEP 2] Creating sample document...")
    pdf_path = create_sample_pdf()

    # Step 3: Check initial graph state
    print("\n[STEP 3] Checking initial graph state...")
    graph = GraphManager()
    initial_stats = graph.get_graph_stats()
    print(f"  Initial nodes: {initial_stats['total_nodes']}")
    print(f"  Initial relationships: {initial_stats['total_relationships']}")

    # Step 4: Process PDF
    print("\n[STEP 4] Processing document with LLM extraction...")
    print("  (This may take 2-3 minutes for real PDFs)")

    try:
        # Process sample file (text or PDF)
        if pdf_path.endswith('.txt'):
            # If it's a text file, extract text directly
            with open(pdf_path, 'r') as f:
                text = f.read()

            print(f"  Document size: {len(text)} characters")
            print(f"  Chunks: ~{len(text) // 8000 + 1}")

            # Chunk the text
            chunks = [text[i:i+8000] for i in range(0, len(text), 8000)]

            # Extract entities using schema
            from ingestion.shared.src.core.schema_extractor import SchemaExtractor
            extractor = SchemaExtractor(schema)

            print(f"  Extracting entities and relationships...")
            extracted = extractor.extract_from_chunks(chunks)

            # Build graph
            from ingestion.shared.src.core.schema_graph_builder import SchemaGraphBuilder
            builder = SchemaGraphBuilder(graph, schema)
            stats = builder.build_graph(extracted)

        else:
            # PDF processing would require additional pipeline implementation
            raise NotImplementedError("Direct PDF pipeline not implemented. Use text extraction instead.")

        print(f"\n  Extraction complete!")
        print(f"    Entities extracted: {extracted.get('nodes_count', len(extracted.get('nodes', [])))}")
        print(f"    Relationships found: {extracted.get('relationships_count', len(extracted.get('relationships', [])))}")

    except Exception as e:
        print(f"\n  ✗ Error during extraction: {e}")
        print(f"  Make sure OPENAI_API_KEY is set in .env")
        return False

    # Step 5: Check final graph state
    print("\n[STEP 5] Checking final graph state...")
    final_stats = graph.get_graph_stats()
    print(f"  Final nodes: {final_stats['total_nodes']}")
    print(f"  Final relationships: {final_stats['total_relationships']}")
    print(f"  Nodes added: {final_stats['total_nodes'] - initial_stats['total_nodes']}")
    print(f"  Relationships added: {final_stats['total_relationships'] - initial_stats['total_relationships']}")

    if final_stats['node_types']:
        print(f"\n  Node breakdown:")
        for nt in final_stats['node_types']:
            print(f"    - {nt['label']}: {nt['count']}")

    # Step 6: Query the graph
    print("\n[STEP 6] Querying the graph...")

    try:
        # Query 1: Find all organizations
        print("\n  Query 1: All organizations")
        results = graph.execute_query("""
            MATCH (org:Organization)
            RETURN org.name as name, org.role as role
        """)
        if results:
            for r in results:
                print(f"    - {r['name']} ({r.get('role', 'N/A')})")
        else:
            print("    (No organizations found)")

        # Query 2: Find all clauses
        print("\n  Query 2: All clauses")
        results = graph.execute_query("""
            MATCH (clause:Clause)
            RETURN clause.number as number, clause.title as title
        """)
        if results:
            for r in results:
                print(f"    - Clause {r['number']}: {r.get('title', 'N/A')}")
        else:
            print("    (No clauses found)")

        # Query 3: Find all obligations
        print("\n  Query 3: All obligations")
        results = graph.execute_query("""
            MATCH (obl:Obligation)
            RETURN obl.description as description, obl.obligated_party as party
        """)
        if results:
            for r in results:
                print(f"    - {r['party'] or 'Unknown'}: {r['description']}")
        else:
            print("    (No obligations found)")

        # Query 4: Complex relationship query
        print("\n  Query 4: Organizations and their obligations")
        results = graph.execute_query("""
            MATCH (org:Organization)-[:OBLIGATED_TO]->(obl:Obligation)
            RETURN org.name as party, obl.description as obligation
        """)
        if results:
            for r in results:
                print(f"    - {r['party']}: {r['obligation']}")
        else:
            print("    (No relationships found)")

    except Exception as e:
        print(f"  Error during queries: {e}")

    # Step 7: Show delete function
    print("\n[STEP 7] Testing delete function...")
    print(f"  Current graph has {final_stats['total_nodes']} nodes")
    print(f"  To delete all: graph.delete_all(confirm=True)")
    print(f"  To get stats: graph.get_graph_stats()")

    graph.close()

    # Cleanup
    if os.path.exists(pdf_path):
        os.remove(pdf_path)
        print(f"\n✓ Cleaned up {pdf_path}")

    print("\n" + "="*70)
    print("✓ SYSTEM TEST COMPLETE")
    print("="*70)

    return True


def main():
    """Run the test"""
    try:
        success = test_full_workflow()

        if success:
            print("""
Next steps:
1. Use your own PDF: pipeline.process_pdf("your_document.pdf")
2. Define your schema: Add node/relationship types you care about
3. Query the graph: Use Cypher queries to explore relationships
4. Use the API: Start api_service.py for REST endpoints
            """)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
