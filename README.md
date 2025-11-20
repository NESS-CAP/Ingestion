# Neo4j Schema-Based Document Ingestion System

A modular document ingestion system that extracts entities and relationships from PDFs using OpenAI's language models and builds a Neo4j knowledge graph based on a defined schema.

## Quick Overview

**What it does:**
- Reads PDF documents (or text)
- Extracts structured information (entities & relationships) using OpenAI
- Builds a Neo4j knowledge graph for semantic querying
- Supports both basic text and advanced PDF extraction
- Includes full test suite and example implementations

**Two main sections:**
- **test-ingestion**: Testing framework with basic PDF extraction
- **obc-ingestion**: Ontario Building Code specific processing with advanced extraction

---

## Repository Organization

```
ingestion/
├── shared/                          # Core modules used by both sections
│   ├── src/core/
│   │   ├── schema.py               # Schema definitions (including create_elaws_obc_schema)
│   │   ├── schema_extractor.py     # OpenAI-based extraction
│   │   ├── schema_graph_builder.py # Neo4j graph construction
│   │   ├── graph_manager.py        # Neo4j connection & queries
│   │   ├── chunker.py              # Text chunking
│   │   ├── embeddings.py           # Vector embeddings
│   │   └── elaws_extractor.py      # E-Laws OBC specific extraction (hierarchical parsing for section 3.2.2)
│   └── config/
│       ├── settings.py             # Configuration
│       └── schemas.py              # Schema templates
│
├── test-ingestion/
│   ├── tests/                      # Comprehensive test suite
│   └── pdf_read_naive/             # Basic PDF extraction
│
└── obc-ingestion/
    ├── scripts/                    # Main ingestion scripts
    ├── pdf_read_adv/               # Advanced PDF extraction
    ├── pdf_read_with_GPT/          # GPT-powered enrichment
    └── data/                       # Extracted data
```

---

## Getting Started (30 Seconds)

### 1. Setup Environment

```bash
pip install -r requirements.txt

# Create .env file
cat > .env << EOF
NEO4J_URI=neo4j+s://your-host
NEO4J_USER=neo4j
NEO4J_PASSWORD=your-password
OPENAI_API_KEY=sk-your-key
EOF
```

### 2. Run Tests

```bash
cd ingestion/test-ingestion
python -m pytest tests/ -v
```

### 3. Run OBC Ingestion

```bash
cd ingestion/obc-ingestion
python scripts/ingest_schema.py
# or
python pdf_read_with_GPT/main.py
```

---

## How It Works

```
PDF Input
    ↓
[PDF Extraction Layer]
├── pdf_read_naive: Simple text extraction
├── pdf_read_adv: Structured extraction with sections/tables
└── pdf_read_with_GPT: GPT-enriched extraction
    ↓
[Shared Core Processing]
├── DocumentChunker: Split into overlapping chunks
├── SchemaExtractor: Extract entities/relationships (OpenAI)
├── EmbeddingManager: Generate vector embeddings (local)
└── SchemaGraphBuilder: Create Neo4j nodes/relationships
    ↓
[Graph Management]
└── GraphManager: Execute queries, manage indexes
    ↓
Neo4j Knowledge Graph
```

---

## Key Components

### Shared Core (ingestion/shared/src/core/)

| Module | Purpose |
|--------|---------|
| **schema.py** | Define node types, properties, and relationships |
| **schema_extractor.py** | Extract entities/relationships using OpenAI gpt-4o-mini |
| **schema_graph_builder.py** | Convert extracted data to Neo4j nodes/relationships |
| **graph_manager.py** | Neo4j connection, queries, vector search |
| **chunker.py** | Split documents into overlapping chunks |
| **embeddings.py** | Generate 384-dim vectors (Sentence Transformers) |

### Configuration (ingestion/shared/config/)

- **settings.py**: Neo4j connection, chunk size, embedding model, API keys
- **schemas.py**: Pre-built schema templates (legal documents, agreements)

### Test Section (ingestion/test-ingestion/)

- **tests/**: Full test suite with connection, extraction, and system tests
- **pdf_read_naive/**: Basic PDF text extraction using PyPDF2
- **tests/examples/**: Minimal working examples

### OBC Section (ingestion/obc-ingestion/)

- **scripts/ingest_schema.py**: Main ingestion pipeline
- **pdf_read_adv/**: Advanced PDF extraction with hierarchical sections
- **pdf_read_with_GPT/**: Two-stage pipeline with GPT enrichment
- **data/**: Extracted OBC data (JSON format)
- **PDF files**: Source documents (2024 OBC Vol 1, 2025 OBC Vol 2, e-laws)

---

## Usage Examples

### Basic Ingestion

```python
from ingestion.shared.src.core.schema import Schema, NodeDef, PropertyDef, RelDef
from ingestion.shared.src.core.schema_extractor import SchemaExtractor
from ingestion.shared.src.core.schema_graph_builder import SchemaGraphBuilder
from ingestion.shared.src.core.graph_manager import GraphManager

# Define schema
schema = Schema("My Domain")
schema.add_node(NodeDef(
    label="Person",
    properties=[PropertyDef("name", "string", required=True)]
))
schema.add_node(NodeDef(
    label="Company",
    properties=[PropertyDef("name", "string", required=True)]
))
schema.add_relationship(RelDef(
    type="WORKS_AT",
    source_label="Person",
    target_label="Company"
))

# Extract from text
extractor = SchemaExtractor(schema)
extracted = extractor.extract_from_text("John works at Acme Corp")

# Build graph
graph = GraphManager()
builder = SchemaGraphBuilder(graph, schema)
builder.build_graph(extracted)

# Query
results = graph.execute_query(
    "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name"
)
```

### Using Pre-built Schemas

```python
from ingestion.shared.config.schemas import create_legal_document_schema
from ingestion.shared.src.core.schema_extractor import SchemaExtractor

schema = create_legal_document_schema()
extractor = SchemaExtractor(schema)
extraction = extractor.extract_from_text("Your legal document text...")
```

### Document Chunking & Embeddings

```python
from ingestion.shared.src.core.chunker import DocumentChunker
from ingestion.shared.src.core.embeddings import EmbeddingManager

chunker = DocumentChunker(chunk_size=512, overlap=50)
chunks = chunker.chunk_text("Large document text...")

em = EmbeddingManager()
embeddings = em.embed_batch([c["text"] for c in chunks])
```

---

## Running Tests

### Full Test Suite

```bash
cd ingestion/test-ingestion
python -m pytest tests/ -v
```

Tests include:
- Database connectivity validation
- Schema extraction with OpenAI
- Graph construction
- End-to-end pipeline

### Minimal Example

```bash
python -m ingestion.test_ingestion.tests.examples.schema_example
```

---

## OBC Ingestion Pipeline

### Stage 1: PDF Extraction
```bash
cd ingestion/obc-ingestion
python pdf_read_adv/extract.py
```
Extracts structured content with sections, tables, and images.

### Stage 2: GPT Enrichment
```bash
python pdf_read_with_GPT/stage2_enrichment.py
```
Uses OpenAI to enrich extracted data with semantic labels and relationships.

### Stage 3: Neo4j Ingestion
```bash
python pdf_read_with_GPT/stage3_neo4j_ingestion.py
```
Creates nodes and relationships in Neo4j knowledge graph.

Or run all stages at once:
```bash
python pdf_read_with_GPT/main.py
```

---

## Neo4j Test Queries

After ingestion, try these queries:

```cypher
# Count nodes
MATCH (n) RETURN count(n) as total_nodes

# Find section 3.2.2
MATCH (s:Section {section_number: "3.2.2"})
RETURN s

# Find all definitions
MATCH (d:Definition)
RETURN d.term, d.definition
LIMIT 10

# Vector similarity search
CALL db.index.vector.queryNodes('chunk_embeddings', 10, embedding_vector)
YIELD node, score
RETURN node.text, score
```

---

## Configuration

### Environment Variables (.env)

```
# Neo4j
NEO4J_URI=neo4j+s://your-instance.databases.neo4j.io
NEO4J_USER=neo4j
NEO4J_PASSWORD=your-password

# OpenAI
OPENAI_API_KEY=sk-your-api-key

# Optional (defaults shown)
CHUNK_SIZE=512
CHUNK_OVERLAP=50
EMBEDDING_MODEL=all-MiniLM-L6-v2
OPENAI_MODEL=gpt-4o-mini
```

### Configuration Files

- **ingestion/shared/config/settings.py**: Main settings
- **ingestion/shared/config/schemas.py**: Schema templates

---

## Dependencies

```
neo4j>=5.25.0                    # Neo4j driver
python-dotenv>=1.0.0            # Environment variables
openai>=1.10.0                  # OpenAI API
sentence-transformers>=2.2.0    # Vector embeddings
PyPDF2>=3.0.0                   # Basic PDF extraction
pdfplumber>=0.9.0               # Advanced PDF extraction
```

---

## Project Structure (Detailed)

```
Ingestion/
├── README.md                        # Main documentation (this file)
├── QUICKSTART.md                    # Quick reference guide
├── requirements.txt                 # Dependencies
├── .env                            # Environment configuration
├── CLAUDE_CONTEXT.md               # Project context for Claude
│
├── README/                         # Detailed documentation
│   ├── INDEX.md
│   ├── MAIN.md
│   ├── INGESTION.md
│   ├── SHARED.md
│   ├── TEST_INGESTION.md
│   └── OBC_INGESTION.md
│
├── ingestion/                      # Main codebase
│   ├── shared/                     # Shared modules
│   ├── test-ingestion/             # Testing section
│   └── obc-ingestion/              # OBC section
│
└── venv/                           # Virtual environment
```

---

## Documentation

For detailed information, see:

- **README/INDEX.md** - Navigation guide to all documentation
- **README/MAIN.md** - Comprehensive project documentation
- **README/SHARED.md** - Shared core modules reference
- **README/TEST_INGESTION.md** - Testing framework guide
- **README/OBC_INGESTION.md** - OBC-specific documentation
- **IMPLEMENTATION_SUMMARY.md** - Technical implementation details
- **INGESTION_PIPELINE.md** - Detailed pipeline architecture

---

## Troubleshooting

### Neo4j Connection Failed
```bash
# Check environment variables
echo $NEO4J_URI
echo $NEO4J_USER

# Test connection
python -c "from ingestion.shared.src.core.graph_manager import GraphManager; GraphManager().execute_query('RETURN 1')"
```

### OpenAI API Errors
- Verify `OPENAI_API_KEY` is valid
- Check API has gpt-4o-mini access
- Monitor rate limits in OpenAI dashboard

### PDF Extraction Issues
- Ensure PDF files are valid and not corrupted
- Check file permissions
- Review extraction logs for specific errors

### Memory Issues
- Reduce chunk size in settings
- Process PDFs in smaller batches
- Clear Neo4j database if needed

---

## Cost Analysis

- **Schema Extraction**: ~$0.00015 per 1M input tokens (gpt-4o-mini)
- **Embeddings**: Free (local processing, no API calls)
- **Database**: Neo4j Aura pricing (depends on instance)

**Estimated cost**: ~$0.0005 per document

---

## Features

✅ Schema-driven entity and relationship extraction
✅ OpenAI gpt-4o-mini for intelligent extraction
✅ Local vector embeddings (384-dimensional)
✅ Neo4j knowledge graph construction
✅ Vector similarity search
✅ Full-text search
✅ Graph statistics and analysis
✅ Batch document processing
✅ Error handling and logging
✅ Comprehensive test suite

---

## Quick Start Commands

```bash
# Setup
pip install -r requirements.txt
cat > .env << EOF
NEO4J_URI=neo4j+s://your-host
NEO4J_USER=neo4j
NEO4J_PASSWORD=your-password
OPENAI_API_KEY=sk-your-key
EOF

# Run tests
cd ingestion/test-ingestion && python -m pytest tests/ -v

# Run OBC ingestion
cd ingestion/obc-ingestion && python scripts/ingest_schema.py

# Test Neo4j connection
python -c "from ingestion.shared.src.core.graph_manager import GraphManager; GraphManager().execute_query('RETURN 1')"

# Count graph nodes
python -c "from ingestion.shared.src.core.graph_manager import GraphManager; g = GraphManager(); stats = g.get_graph_stats(); print(f'Nodes: {stats[\"total_nodes\"]}, Relationships: {stats[\"total_relationships\"]}')"
```

---

## Status

✅ Production-ready
✅ Fully documented
✅ Comprehensive test coverage
✅ Ready for deployment

**Last Updated**: November 20, 2024
