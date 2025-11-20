# Neo4j Hybrid RAG - Document Ingestion System

A sophisticated document ingestion and knowledge graph construction system that combines vector embeddings, schema-based extraction, and graph-based retrieval for enterprise document processing.

## Overview

This system enables:
- **Document Processing**: Extract text from PDFs and chunking with overlaps
- **Semantic Embeddings**: Generate vector embeddings using Sentence Transformers (all-MiniLM-L6-v2, 384 dimensions)
- **Schema Extraction**: Use OpenAI's gpt-4o-mini to extract entities and relationships from documents
- **Knowledge Graph**: Build Neo4j property graphs with nodes and relationships
- **Hybrid Retrieval**: Combine vector similarity and text search for powerful querying

## Architecture

### Components

1. **GraphManager** (`src/core/graph_manager.py`)
   - Neo4j Aura cloud database management
   - Vector index creation and search
   - Node/relationship CRUD operations

2. **EmbeddingManager** (`src/core/embeddings.py`)
   - Sentence Transformers integration
   - Local embedding generation (no API calls)
   - Batch embedding support

3. **DocumentChunker** (`src/core/chunker.py`)
   - Intelligent text splitting with overlaps
   - Metadata preservation
   - Deduplication handling

4. **SchemaExtractor** (`src/core/schema_extractor.py`)
   - OpenAI gpt-4o-mini integration
   - JSON format normalization
   - Relationship inference from context
   - Markdown code block handling

5. **SchemaGraphBuilder** (`src/core/schema_graph_builder.py`)
   - Graph construction from extracted entities
   - Node and relationship creation
   - ID mapping and validation

6. **HybridRetriever** (`src/rag/retriever.py`)
   - Semantic search (vector similarity)
   - Text search (keyword matching)
   - Weighted score combination

## Technology Stack

- **Database**: Neo4j Aura (cloud instance)
- **Embeddings**: Sentence Transformers (all-MiniLM-L6-v2)
- **LLM**: OpenAI gpt-4o-mini
- **Framework**: Python 3.9+
- **Dependencies**: See `requirements.txt`

## Installation

### 1. Clone and Setup
```bash
cd /Users/niraaj/Projects/school/Capstone/Ingestion
pip install -r requirements.txt
```

### 2. Configure Environment
Create `.env` file with:
```
NEO4J_URI=neo4j+s://your-instance.databases.neo4j.io
NEO4J_USER=neo4j
NEO4J_PASSWORD=your-password
OPENAI_API_KEY=sk-your-key
OPENAI_ORG_ID=org-your-id  # Optional
```

### 3. Verify Setup
```bash
PYTHONPATH=. python3 tests/test_connection.py
```

## Usage

### Quick Start: Document Ingestion

```python
from src.core.pipeline import IngestionPipeline
from src.core.graph_manager import GraphManager

# Initialize pipeline
pipeline = IngestionPipeline()

# Process document
results = pipeline.process_document(
    file_path="document.pdf",
    doc_name="My Document"
)

print(f"Chunks processed: {results['chunks_processed']}")
print(f"Embeddings created: {results['embeddings_created']}")

# Retrieve context for query
graph = GraphManager()
results = graph.vector_search(query_embedding, limit=5)
```

### Schema-Based Extraction

```python
from src.core.schema_extractor import SchemaExtractor
from src.core.schema import Schema, NodeDef, PropertyDef, RelDef

# Define schema
schema = Schema("Legal Documents")
schema.add_node(NodeDef(
    label="Organization",
    properties=[PropertyDef("name", "string", required=True)],
    description="Company or organization"
))

# Extract from document
extractor = SchemaExtractor(schema)
extracted = extractor.extract_from_text(document_text)

# Build graph
from src.core.schema_graph_builder import SchemaGraphBuilder
builder = SchemaGraphBuilder(graph, schema)
result = builder.build_graph(extracted)
```

### Hybrid Retrieval

```python
from src.rag.retriever import HybridRetriever

retriever = HybridRetriever()

# Semantic search
results = retriever.semantic_search("What is the deadline?", limit=5)

# Text search
results = retriever.text_search("deadline", limit=5)

# Hybrid search (combined)
results = retriever.hybrid_search(
    query="What is the deadline?",
    limit=5,
    semantic_weight=0.6  # 60% semantic, 40% text
)

# Get context for RAG
context = retriever.get_context(
    query="What is the deadline?",
    limit=5,
    context_window=2
)
```

## Running Tests

See [tests/README.md](tests/README.md) for detailed test documentation.

### Quick Test Commands

```bash
# Test connectivity
PYTHONPATH=. python3 tests/test_connection.py

# Full system test
PYTHONPATH=. python3 tests/test_system.py

# Schema extraction (requires OpenAI API)
PYTHONPATH=. python3 tests/test_schema_extraction.py
```

## Project Structure

```
/Users/niraaj/Projects/school/Capstone/Ingestion/
├── src/
│   ├── core/              # Core functionality
│   │   ├── graph_manager.py
│   │   ├── embeddings.py
│   │   ├── chunker.py
│   │   ├── schema_extractor.py
│   │   ├── schema_graph_builder.py
│   │   ├── schema.py
│   │   ├── pipeline.py
│   │   ├── pdf_processor.py
│   │   └── pdf_to_graph.py
│   └── rag/               # Retrieval-Augmented Generation
│       └── retriever.py
├── config/                # Configuration
│   ├── settings.py
│   └── schemas.py
├── tests/                 # Test cases
│   ├── test_connection.py
│   ├── test_system.py
│   ├── test_schema_extraction.py
│   ├── examples/
│   │   ├── pdf_example.py
│   │   └── schema_example.py
│   └── README.md
├── main.py                # Primary ingestion script
├── api_service.py         # Flask API endpoints
├── requirements.txt       # Python dependencies
└── README.md              # This file
```

## Key Features

### 1. Intelligent Chunking
- Overlapping chunks prevent context loss
- Configurable chunk size and overlap
- Metadata preservation through chunking

### 2. Vector Embeddings
- Fast local embedding generation
- No API calls needed for embeddings
- 384-dimensional vectors for semantic search

### 3. Schema-Based Extraction
- Custom schema definition support
- Entity and relationship extraction
- Inferred relationship detection
- Flexible JSON format handling

### 4. Knowledge Graph
- Neo4j property graph database
- Vector indexes for semantic search
- Full-text search capabilities
- Relationship types and properties

### 5. Hybrid Search
- Semantic similarity search
- Keyword/text search
- Weighted combination of both
- Configurable weights

## Cost Analysis

### API Usage
| Component | Model | Cost |
|-----------|-------|------|
| Embeddings | Sentence Transformers | Free (local) |
| Schema Extraction | gpt-4o-mini | ~$0.00015 per 1M input tokens |
| | | ~$0.00060 per 1M output tokens |

### Estimated Costs
- Document chunking: Free
- Embedding generation: Free
- Schema extraction: ~$0.0005 per document
- Storage: Neo4j Aura pricing

## Recent Improvements

### Bug Fixes
- **Fixed infinite loop** in DocumentChunker when text < chunk_size
- **Fixed vector index syntax** (similarity_metric → similarity_function)
- **Improved relationship extraction** with explicit prompt examples

### Enhancements
- **Normalization layer** handles LLM format variations
- **Markdown code block handling** for robustness
- **Flexible relationship validation** (from/to or source_id/target_id)
- **Entity-to-ID mapping** for relationship remapping
- **Comprehensive error handling** throughout

### Features Added
- Database cleanup and verification
- API cost tracking and estimation
- Relationship inference from context
- Node label inference for missing labels
- ID generation for nodes without explicit IDs

## Performance Characteristics

| Operation | Time | Notes |
|-----------|------|-------|
| PDF processing | ~1-5s | Depends on PDF size |
| Embedding generation | ~0.5-2s | Batch processing for multiple chunks |
| OpenAI extraction | ~30-60s | API latency, varies by document length |
| Vector search | ~0.1-0.5s | On-database search |
| Text search | ~0.1-0.5s | Full-text index search |

## Troubleshooting

### Connection Issues
```bash
# Test database connectivity
PYTHONPATH=. python3 tests/test_connection.py
```

### Missing Dependencies
```bash
pip install -r requirements.txt --upgrade
```

### Embedding Model Not Loading
- Ensure internet access for HuggingFace model download
- Model: `sentence-transformers/all-MiniLM-L6-v2`

### OpenAI API Errors
- Verify API key in `.env`
- Check API key has gpt-4o-mini access
- Monitor rate limits for API calls

## Contributing

When modifying core components:
1. Update corresponding tests
2. Run full test suite
3. Update this README if adding new features
4. Follow existing code style and patterns

## Future Enhancements

- [ ] Support for more document formats (DOCX, TXT, etc.)
- [ ] Batch processing improvements
- [ ] Custom embedding model support
- [ ] Advanced schema validation
- [ ] GraphQL query interface
- [ ] Web UI for graph visualization
- [ ] Caching layer for embeddings
- [ ] Rate limiting for API calls

## License

Academic use (Capstone Project)

## Support

For issues or questions:
1. Check [tests/README.md](tests/README.md) for test documentation
2. Review test cases in `tests/` directory
3. Run `test_connection.py` to verify setup
4. Check `.env` configuration

## References

- [Neo4j Documentation](https://neo4j.com/docs/)
- [Sentence Transformers](https://www.sbert.net/)
- [OpenAI API](https://platform.openai.com/docs/)
- [RAG Best Practices](https://www.promptingguide.ai/research/rag)
