# Claude Context for Ingestion Project

## Project Goal
Build a hybrid RAG system that:
1. Ingests PDF documents
2. Extracts entities and relationships using OpenAI
3. Creates a knowledge graph in Neo4j
4. Supports vector and graph-based search

## Important Constraints
- **DO NOT** perform git operations (commits, pushes, branches, etc.)
- **DO NOT** create or add documentation files
- Focus solely on ingestion, extraction, and graph creation pipeline

## Current Status
- ✅ Repository reorganized into modular structure (Nov 20, 2024)
- ✅ Split into test-ingestion and obc-ingestion with shared core
- ✅ Complete test suite with examples
- ✅ Production-ready ingestion pipeline
- ✅ Comprehensive documentation

## Repository Structure

```
Ingestion/
├── ingestion/                      # Main codebase
│   ├── shared/                     # Core modules (both sections use)
│   │   ├── src/core/
│   │   │   ├── schema.py
│   │   │   ├── schema_extractor.py (OpenAI extraction)
│   │   │   ├── schema_graph_builder.py
│   │   │   ├── graph_manager.py (Neo4j)
│   │   │   ├── chunker.py
│   │   │   └── embeddings.py (local, 384-dim)
│   │   └── config/
│   │       ├── settings.py
│   │       └── schemas.py
│   │
│   ├── test-ingestion/             # Testing framework
│   │   ├── tests/                  # Full test suite
│   │   └── pdf_read_naive/         # Basic PDF extraction
│   │
│   └── obc-ingestion/              # Ontario Building Code
│       ├── scripts/ingest_schema.py
│       ├── pdf_read_adv/           # Advanced extraction
│       ├── pdf_read_with_GPT/      # GPT enrichment
│       └── data/                   # Results
│
├── README.md                       # Main documentation (START HERE)
├── README/                         # Detailed docs
├── QUICKSTART.md                   # Quick commands
└── requirements.txt
```

## Key Imports
```python
from ingestion.shared.src.core.schema import Schema
from ingestion.shared.src.core.schema_extractor import SchemaExtractor
from ingestion.shared.src.core.graph_manager import GraphManager
from ingestion.shared.src.core.embeddings import EmbeddingManager
from ingestion.shared.config.settings import NEO4J_CONFIG
```

## Quick Commands
```bash
# Setup
pip install -r requirements.txt

# Tests
cd ingestion/test-ingestion && python -m pytest tests/ -v

# OBC Ingestion
cd ingestion/obc-ingestion && python scripts/ingest_schema.py

# Check connection
python -c "from ingestion.shared.src.core.graph_manager import GraphManager; GraphManager().execute_query('RETURN 1')"
```

## Documentation
- **README.md** - Complete project documentation (START HERE)

## Architecture Overview
```
PDF → Extraction → Chunking → Entity Extraction (OpenAI)
                                ↓
                          Embeddings (Local)
                                ↓
                          Graph Construction
                                ↓
                          Neo4j Database
                                ↓
                    Vector & Graph Search
```

## Modules at a Glance

| Module | Purpose | Location |
|--------|---------|----------|
| **schema.py** | Define entities & relationships | shared/src/core/ |
| **schema_extractor.py** | OpenAI extraction | shared/src/core/ |
| **graph_manager.py** | Neo4j operations | shared/src/core/ |
| **chunker.py** | Text splitting | shared/src/core/ |
| **embeddings.py** | Local embeddings | shared/src/core/ |
| **schema_graph_builder.py** | Graph construction | shared/src/core/ |

## Environment Variables
```
NEO4J_URI=neo4j+s://your-host
NEO4J_USER=neo4j
NEO4J_PASSWORD=your-password
OPENAI_API_KEY=sk-your-key
```

## Remind me to reference this file if I start attempting git operations.
