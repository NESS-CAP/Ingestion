#!/usr/bin/env python3
"""
Test script to verify Neo4j connection and basic setup.
Run this before running main.py to debug connection issues.
"""

import sys
from ingestion.shared.config.settings import NEO4J_CONFIG
from ingestion.shared.src.core.graph_manager import GraphManager
from ingestion.shared.src.core.embeddings import EmbeddingManager

def test_neo4j_connection():
    """Test Neo4j database connection"""
    print("Testing Neo4j Connection...")
    print(f"  URI: {NEO4J_CONFIG['uri']}")
    print(f"  User: {NEO4J_CONFIG['user']}")

    try:
        graph = GraphManager()

        # Try a simple query
        result = graph.execute_query("RETURN 'Neo4j is connected!' as message")
        if result:
            print(f"  ✓ Connected! Message: {result[0].get('message')}")
        else:
            print("  ✗ Connected but no response")

        graph.close()
        return True
    except Exception as e:
        print(f"  ✗ Connection failed: {e}")
        print("\n  Troubleshooting:")
        print("    1. Check your .env file has correct NEO4J_URI and password")
        print("    2. Verify Neo4j instance is running")
        print("    3. Test connection in Neo4j Browser first")
        return False


def test_embeddings():
    """Test embedding model"""
    print("\nTesting Embeddings...")

    try:
        embedder = EmbeddingManager()
        print(f"  Model: sentence-transformers/all-MiniLM-L6-v2")
        print(f"  Embedding dimension: {embedder.get_embedding_dimension()}")

        # Test single embedding
        text = "Neo4j is a graph database"
        embedding = embedder.embed_text(text)
        print(f"  ✓ Text embedded successfully ({len(embedding)} dimensions)")

        # Test batch embedding
        texts = ["text1", "text2", "text3"]
        batch_embeddings = embedder.embed_batch(texts)
        print(f"  ✓ Batch embedding successful ({len(batch_embeddings)} vectors)")

        return True
    except Exception as e:
        print(f"  ✗ Embedding test failed: {e}")
        return False


def test_graph_operations():
    """Test basic graph operations"""
    print("\nTesting Graph Operations...")

    try:
        graph = GraphManager()

        # Try creating test nodes
        test_doc_id = "test-doc-123"
        test_chunk_id = "test-chunk-456"

        # Create document
        graph.create_document_node(test_doc_id, "Test Document", "test")
        print("  ✓ Document node created")

        # Create chunk with dummy embedding
        dummy_embedding = [0.1] * 384
        graph.create_chunk_node(
            test_chunk_id,
            "This is a test chunk",
            dummy_embedding,
            {"test": True}
        )
        print("  ✓ Chunk node created")

        # Link chunk to document
        graph.link_chunk_to_document(test_chunk_id, test_doc_id, 0)
        print("  ✓ Chunk linked to document")

        # Retrieve document chunks
        chunks = graph.get_document_chunks(test_doc_id)
        print(f"  ✓ Retrieved {len(chunks)} chunk(s)")

        # Cleanup
        graph.delete_document(test_doc_id)
        print("  ✓ Test data cleaned up")

        graph.close()
        return True
    except Exception as e:
        print(f"  ✗ Graph operation test failed: {e}")
        return False


def main():
    """Run all tests"""
    print("=" * 60)
    print("Neo4j Hybrid RAG - Connection & Setup Test")
    print("=" * 60 + "\n")

    results = []

    # Test connection
    results.append(("Neo4j Connection", test_neo4j_connection()))

    # Test embeddings
    results.append(("Embeddings", test_embeddings()))

    # Test graph operations
    results.append(("Graph Operations", test_graph_operations()))

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    all_passed = True
    for test_name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name}: {status}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n✓ All tests passed! You're ready to run main.py")
        return 0
    else:
        print("\n✗ Some tests failed. Check the output above for details.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
