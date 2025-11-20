# HTML E-Laws Ingestion Pipeline with GPT-4o

## Overview

A sophisticated 3-stage pipeline for ingesting Ontario Building Code (O. Reg. 332/12) from HTML format into Neo4j with **fine-grained clause-level nodes**.

### Key Differences from PDF Pipeline

| Aspect | PDF Pipeline | HTML Pipeline |
|--------|-------------|--------------|
| **Input** | PDF file (11MB) | HTML URL/file |
| **Stage 1** | Regex-based section detection | LangChain HTMLHeaderTextSplitter |
| **Stage 2** | OpenAI Vision (PDFs → images) | GPT-4o text extraction (direct) |
| **Stage 3** | 1 node per section (~30 nodes) | **1 node per clause** (~thousands) |
| **Complexity** | 3 stages | 3 stages (simpler) |
| **Cost** | ~$5-25/run (Vision API) | Lower (text API only) |
| **Queryability** | Section-level | **Clause-level** |

## Architecture

```
HTML Source (e-laws.ontario.ca)
          ↓
[Stage 1: HTML Loading & Semantic Chunking]
  ├── Fetch HTML from URL
  ├── Parse with BeautifulSoup
  ├── Split by header hierarchy (h1→h6)
  └── Recursive character splitting for fine chunks
          ↓
    html_chunks.json (~1000s of chunks)
          ↓
[Stage 2: GPT-4o Content Extraction]
  ├── Extract numbered clauses (1), (2), (3)...
  ├── Extract lettered subclauses (a), (b), (c)...
  ├── Extract roman numerals (i), (ii), (iii)...
  ├── Extract definitions
  ├── Extract cross-references
  └── Add semantic tags/keywords
          ↓
    gpt_extracted.json (~1000s of extracted items)
          ↓
[Stage 3: Neo4j Ingestion]
  ├── Create Regulation → Division → Part → Section
  ├── Create individual Clause nodes
  ├── Create SubClause nodes
  ├── Create Item nodes
  ├── Create Definition nodes
  ├── Generate embeddings for semantic search
  └── Create hierarchical relationships
          ↓
Neo4j Knowledge Graph
  ├── 1000s of nodes (vs 30)
  ├── Full clause-level queryability
  ├── Vector embeddings
  └── Ready for LLM RAG
```

## Installation

### Prerequisites

```bash
pip install langchain langchain-community langchain-text-splitters
pip install beautifulsoup4 requests
pip install openai
pip install neo4j
pip install sentence-transformers
```

### Or use requirements file

```bash
pip install -r requirements-html.txt
```

## Usage

### Run Complete Pipeline

```bash
cd ingestion/obc-ingestion/html_read_with_GPT
python main.py
```

### Run Specific Stages

```bash
# Skip Stage 1 (already have chunks)
python main.py --skip-stages 1

# Skip Stages 1 and 2 (already have extraction)
python main.py --skip-stages 1 2

# Custom data directory
python main.py --data-dir ./custom_data
```

### Run Individual Stages

```python
# Stage 1: Load HTML
from stage1_html_loader import HTMLLoader
from ingestion.shared.config.sources import ELAWS_OBC_HTML_URL

loader = HTMLLoader(url=ELAWS_OBC_HTML_URL)
chunks = loader.load_from_url()
loader.save_chunks(chunks, "data/html_chunks.json")

# Stage 2: Extract with GPT
from stage2_gpt_extraction import GPTContentExtractor

extractor = GPTContentExtractor()
extracted = extractor.extract_batch(chunks)
extractor.save_extracted(extracted, "data/gpt_extracted.json")

# Stage 3: Ingest to Neo4j
from stage3_neo4j_html_ingestion import Neo4jHTMLIngester
from ingestion.shared.src.core.graph_manager import GraphManager

graph = GraphManager()
ingester = Neo4jHTMLIngester(graph)
stats = ingester.ingest(extracted)
graph.close()
```

## Configuration

### Environment Variables

```bash
# Neo4j
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USERNAME=neo4j
export NEO4J_PASSWORD=your_password

# OpenAI
export OPENAI_API_KEY=sk-...
```

### Settings (ingestion/shared/config/settings.py)

```python
HTML_PIPELINE_CONFIG = {
    "header_levels": [
        ("h1", "regulation"),      # Top level
        ("h2", "division"),         # A, B, ...
        ("h3", "part"),             # 3, 9, 11, ...
        ("h4", "section"),          # 3.2.2
        ("h5", "subsection"),       # 3.2.2.1
        ("h6", "clause_group"),     # Groups
    ],
    "chunk_size_fine": 200,         # Small chunks for clauses
    "chunk_overlap_fine": 30,
    "gpt_model": "gpt-4o-mini",    # Cost-effective
    "gpt_rate_limit_delay": 1.0,   # 1 second between calls
    "enable_embeddings": True,
}
```

## Output Files

### Stage 1: html_chunks.json

```json
{
  "documents": [
    {
      "source": "...",
      "title": "Ontario Building Code",
      "chunks": [
        {
          "content": "Section 3.2.2. Building Size...",
          "metadata": {
            "regulation": "O. Reg. 332/12",
            "division": "A",
            "part": "3",
            "section": "3.2.2"
          },
          "size": 150
        }
      ],
      "total_chunks": 1200
    }
  ],
  "total_documents": 1,
  "total_chunks": 1200,
  "source_url": "https://www.ontario.ca/laws/regulation/120332"
}
```

### Stage 2: gpt_extracted.json

```json
{
  "source": "...",
  "title": "Ontario Building Code",
  "extracted_chunks": [
    {
      "original_content": "...",
      "extracted": {
        "clauses": [
          {
            "number": "(1)",
            "text": "An owner shall apply for a permit...",
            "type": "clause",
            "keywords": ["owner", "permit"],
            "nested_items": [
              {
                "number": "(a)",
                "text": "the building or part thereof...",
                "type": "subclause"
              }
            ]
          }
        ],
        "definitions": [
          {
            "term": "Accessible Unit",
            "definition": "A dwelling unit that is designed..."
          }
        ],
        "references": [
          {
            "reference": "3.2.2.1",
            "context": "...see section 3.2.2.1 for details..."
          }
        ]
      }
    }
  ]
}
```

### Stage 3: ingestion_stats_html.json

```json
{
  "success": true,
  "nodes_created": 1247,
  "relationships_created": 2104,
  "clauses_created": 892,
  "subclauses_created": 245,
  "items_created": 110,
  "definitions_created": 0,
  "errors": []
}
```

## Neo4j Queries

### Find all clauses in section 3.2.2

```cypher
MATCH (s:Section {section_number: "3.2.2"})-[:HAS_CLAUSE]->(c:Clause)
RETURN c.clause_number, c.text
ORDER BY c.clause_number
```

### Find subclauses under a clause

```cypher
MATCH (c:Clause {clause_number: "(1)"})-[:HAS_SUBCLAUSE]->(sc:SubClause)
RETURN sc.number, sc.text
```

### Vector similarity search

```cypher
WITH $embedding AS embedding
CALL db.index.vector.queryNodes('clause_embeddings', 10, embedding)
YIELD node, score
RETURN node.clause_number, node.text, score
LIMIT 10
```

### Find all definitions used in Part 3

```cypher
MATCH (p:Part {part_number: "3"})-[:HAS_SECTION]->(s:Section)
MATCH (s)-[:HAS_DEFINITION]->(d:Definition)
RETURN DISTINCT d.term, d.definition
```

## Performance Characteristics

### Chunking
- **Stage 1 Runtime**: ~30-60 seconds (HTML loading + splitting)
- **Chunks Created**: ~1,000-2,000 per regulation

### GPT Extraction
- **Stage 2 Runtime**: ~10-20 minutes (rate-limited API calls)
- **Items Extracted**: ~1,000-2,000 clauses + subclauses
- **Cost**: ~$5-15 (using gpt-4o-mini)

### Neo4j Ingestion
- **Stage 3 Runtime**: ~2-5 minutes
- **Nodes Created**: ~1,200-2,500 (vs 30 in PDF pipeline)
- **Relationships**: ~2,000-4,000

## Advanced Usage

### Using Local HTML File

```python
from stage1_html_loader import HTMLLoader

loader = HTMLLoader()
chunks = loader.load_from_file("path/to/elaws.html")
```

### Custom GPT Extraction

```python
from stage2_gpt_extraction import GPTContentExtractor

extractor = GPTContentExtractor(model="gpt-4-turbo")  # Use expensive model

# Or with custom rate limiting
extracted = extractor.extract_batch(chunks, rate_limit_delay=2.0)
```

### Batch Ingestion with Error Recovery

```python
from stage3_neo4j_html_ingestion import Neo4jHTMLIngester
from ingestion.shared.src.core.graph_manager import GraphManager

graph = GraphManager()
ingester = Neo4jHTMLIngester(graph)

# Process in chunks to handle failures
for i in range(0, len(extracted), 100):
    batch = extracted[i:i+100]
    stats = ingester.ingest(batch)
    if not stats["success"]:
        logger.error(f"Batch {i} failed: {stats['errors']}")
```

## Troubleshooting

### Stage 1: HTML loading fails

```python
# Check if URL is accessible
import requests
response = requests.get("https://www.ontario.ca/laws/regulation/120332")
print(response.status_code)  # Should be 200

# Or use local file instead
chunks = loader.load_from_file("local_elaws.html")
```

### Stage 2: GPT extraction timeout

```python
# Increase rate limiting
extractor.extract_batch(chunks, rate_limit_delay=3.0)

# Or use cheaper model
extractor = GPTContentExtractor(model="gpt-3.5-turbo")
```

### Stage 3: Neo4j connection fails

```bash
# Check Neo4j is running
neo4j status

# Check credentials
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USERNAME=neo4j
export NEO4J_PASSWORD=your_password
```

## Comparison: PDF vs HTML Pipeline

### PDF Pipeline (Original)

```
Input: e-laws.pdf (11MB)
     ↓
Stage 1: PyPDF2 + regex (~2 min)
     ↓
30 sections extracted
     ↓
Stage 2: OpenAI Vision (~10 min, $15)
     ↓
Enriched sections
     ↓
Stage 3: Neo4j (~3 min)
     ↓
~30-50 nodes in Neo4j
     ↓
Query at section level only
```

### HTML Pipeline (New)

```
Input: HTML URL
     ↓
Stage 1: BeautifulSoup + LangChain (~1 min)
     ↓
1000s of chunks
     ↓
Stage 2: GPT-4o (~15 min, $5-10)
     ↓
1000s of clauses/subclauses/items
     ↓
Stage 3: Neo4j (~3 min)
     ↓
1000s+ nodes in Neo4j
     ↓
Query at clause/subclause level
```

## Future Enhancements

- [ ] Parallel Stage 2 processing with thread pools
- [ ] Incremental ingestion (only update changed sections)
- [ ] Multi-regulation support (Parts 1, 2, 4, 5, etc.)
- [ ] Custom OCR for embedded images in HTML
- [ ] Semantic deduplication to avoid duplicate clause nodes
- [ ] Automatic cross-reference linking
- [ ] Export to other formats (CSV, RDF, GraphML)

## License

Part of the Capstone Ingestion project.
