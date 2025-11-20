# HTML Pipeline Usage Examples

## Example 1: Run Complete Pipeline

```bash
cd ingestion/obc-ingestion/html_read_with_GPT
python main.py
```

Output:
```
2024-11-20 14:32:15 - root - INFO - ================================================================================
2024-11-20 14:32:15 - root - INFO - STAGE 1: Loading HTML and chunking with semantic awareness
2024-11-20 14:32:15 - root - INFO - ================================================================================
2024-11-20 14:32:15 - stage1_html_loader - INFO - Loading HTML from https://www.ontario.ca/laws/regulation/120332
2024-11-20 14:32:20 - stage1_html_loader - INFO - Loaded 1 documents from HTML
2024-11-20 14:32:22 - stage1_html_loader - INFO - Split document into 1247 header-aware chunks
2024-11-20 14:32:23 - stage1_html_loader - INFO - Processed 1 documents with 1247 total chunks
2024-11-20 14:32:23 - root - INFO - Stage 1 complete: 1247 chunks created

[... 15 minutes of Stage 2 processing ...]

2024-11-20 14:47:45 - root - INFO - Stage 2 complete: 892 clauses, 45 definitions extracted

[... 5 minutes of Stage 3 processing ...]

2024-11-20 14:52:30 - root - INFO - Stage 3 complete: 1247 nodes, 892 clauses created

================================================================================
PIPELINE COMPLETE
================================================================================
PIPELINE SUMMARY
────────────────────────────────────────────────────────────────────────────────
Stage 1 - HTML Loading:
  Chunks created: 1247
  Output: data/html_chunks.json

Stage 2 - GPT Extraction:
  Clauses extracted: 892
  Definitions extracted: 45
  Output: data/gpt_extracted.json

Stage 3 - Neo4j Ingestion:
  Nodes created: 1247
  Clauses created: 892
  Relationships: 2104
  Output: data/ingestion_stats_html.json

────────────────────────────────────────────────────────────────────────────────
Overall Success: True
```

## Example 2: Run with Skip Options

```bash
# Process Stage 1 and 2, skip Stage 3 (test extraction without ingesting)
python main.py --skip-stages 3

# Output:
# Stage 1: completes and saves html_chunks.json
# Stage 2: completes and saves gpt_extracted.json
# Stage 3: skipped
```

## Example 3: Programmatic Usage

### Load and Chunk HTML

```python
from stage1_html_loader import HTMLLoader
from ingestion.shared.config.sources import ELAWS_OBC_HTML_URL

# Create loader
loader = HTMLLoader(url=ELAWS_OBC_HTML_URL, chunk_size=512)

# Load from URL
chunks = loader.load_from_url()
print(f"Loaded {len(chunks)} documents")

# Or from local file
# chunks = loader.load_from_file("path/to/elaws.html")

# Save chunks
loader.save_chunks(chunks, "output/chunks.json")
```

### Extract with GPT

```python
from stage2_gpt_extraction import GPTContentExtractor
import json

# Load chunks
with open("output/chunks.json") as f:
    chunks_data = json.load(f)

chunks = chunks_data["documents"]

# Extract using GPT
extractor = GPTContentExtractor(model="gpt-4o-mini")
extracted = extractor.extract_batch(chunks, rate_limit_delay=1.0)

# Access extracted data
for doc in extracted:
    print(f"Document: {doc['title']}")
    print(f"Total items: {doc['total_items']}")

    for chunk in doc['extracted_chunks']:
        extracted_data = chunk['extracted']
        clauses = extracted_data['clauses']
        print(f"  Clauses in chunk: {len(clauses)}")

        for clause in clauses:
            print(f"    - {clause['number']}: {clause['text'][:50]}...")

# Save results
extractor.save_extracted(extracted, "output/extracted.json")
```

### Ingest to Neo4j

```python
from stage3_neo4j_html_ingestion import Neo4jHTMLIngester
from ingestion.shared.src.core.graph_manager import GraphManager
import json

# Load extracted data
with open("output/extracted.json") as f:
    extracted_docs = json.load(f)

# Connect to Neo4j
graph = GraphManager()

# Create ingester
ingester = Neo4jHTMLIngester(graph)

# Ingest data
stats = ingester.ingest(
    extracted_docs,
    document_id="obc_html_332_12"
)

# Print statistics
print(f"Success: {stats['success']}")
print(f"Nodes created: {stats['nodes_created']}")
print(f"Clauses: {stats['clauses_created']}")
print(f"Relationships: {stats['relationships_created']}")
if stats['errors']:
    print(f"Errors: {stats['errors']}")

graph.close()
```

## Example 4: Extract from Local HTML File

```python
from stage1_html_loader import HTMLLoader

# If you have a local HTML file instead of fetching from URL
loader = HTMLLoader()
chunks = loader.load_from_file("./local_elaws.html")

# Process as normal
print(f"Loaded {len(chunks)} documents")
loader.save_chunks(chunks, "output/chunks.json")
```

## Example 5: Custom Extraction with Different GPT Model

```python
from stage2_gpt_extraction import GPTContentExtractor

# Use more powerful (and expensive) model
extractor = GPTContentExtractor(model="gpt-4-turbo")

# Extract with longer timeout between calls
extracted = extractor.extract_batch(
    chunks,
    rate_limit_delay=2.0  # 2 seconds between API calls
)

extractor.save_extracted(extracted, "output/extracted_turbo.json")
```

## Example 6: Batch Processing with Error Handling

```python
from stage3_neo4j_html_ingestion import Neo4jHTMLIngester
from ingestion.shared.src.core.graph_manager import GraphManager
import json

with open("output/extracted.json") as f:
    extracted_docs = json.load(f)

graph = GraphManager()
ingester = Neo4jHTMLIngester(graph)

# Process in smaller batches for better error handling
batch_size = 100
all_stats = {
    "total_nodes": 0,
    "total_relationships": 0,
    "total_clauses": 0,
    "errors": []
}

for i in range(0, len(extracted_docs), batch_size):
    batch = extracted_docs[i:i+batch_size]
    print(f"Processing batch {i//batch_size + 1} ({len(batch)} documents)")

    try:
        stats = ingester.ingest(batch)

        all_stats["total_nodes"] += stats["nodes_created"]
        all_stats["total_relationships"] += stats["relationships_created"]
        all_stats["total_clauses"] += stats["clauses_created"]

        if not stats["success"]:
            all_stats["errors"].extend(stats["errors"])

    except Exception as e:
        print(f"Error processing batch: {e}")
        all_stats["errors"].append(str(e))

print(f"\nFinal Statistics:")
print(f"Total nodes: {all_stats['total_nodes']}")
print(f"Total relationships: {all_stats['total_relationships']}")
print(f"Total clauses: {all_stats['total_clauses']}")
if all_stats['errors']:
    print(f"Errors: {len(all_stats['errors'])}")

graph.close()
```

## Example 7: Query Neo4j After Ingestion

```python
from ingestion.shared.src.core.graph_manager import GraphManager

graph = GraphManager()

# Count all clauses
result = graph.execute_query(
    "MATCH (c:Clause) RETURN count(c) as total"
)
print(f"Total clauses: {result[0]['total']}")

# Find clauses in section 3.2.2
result = graph.execute_query(
    """
    MATCH (s:Section {section_number: "3.2.2"})-[:HAS_CLAUSE]->(c:Clause)
    RETURN c.clause_number, c.text LIMIT 10
    """
)
for row in result:
    print(f"Clause {row['c.clause_number']}: {row['c.text'][:50]}...")

# Find subclauses under clause (1)
result = graph.execute_query(
    """
    MATCH (c:Clause {clause_number: "(1)"})-[:HAS_SUBCLAUSE]->(sc:SubClause)
    RETURN sc.number, sc.text
    """
)
for row in result:
    print(f"SubClause {row['sc.number']}: {row['sc.text'][:50]}...")

# Vector similarity search (if embeddings enabled)
from ingestion.shared.src.core.embeddings import EmbeddingManager

em = EmbeddingManager()
query_embedding = em.embed_text("fire safety requirements")

# This would require vector index setup in Neo4j
# result = graph.execute_query(
#     """
#     WITH $embedding AS embedding
#     CALL db.index.vector.queryNodes('clause_embeddings', 5, embedding)
#     YIELD node, score
#     RETURN node.clause_number, node.text, score
#     """,
#     {"embedding": query_embedding}
# )

graph.close()
```

## Example 8: Pipeline with Custom Data Directory

```bash
# Run pipeline with custom output directory
python main.py --data-dir ./ingestion_output_20241120

# This creates:
# ingestion_output_20241120/
# ├── html_chunks.json
# ├── gpt_extracted.json
# └── ingestion_stats_html.json
```

## Example 9: Extract Specific Section Only

If you only want to process a specific section:

```python
import json

# Load chunks
with open("data/html_chunks.json") as f:
    chunks_data = json.load(f)

documents = chunks_data["documents"]

# Find chunks for section 3.2.2
section_322_chunks = [
    chunk for doc in documents
    for chunk in doc["chunks"]
    if chunk["metadata"].get("section") == "3.2.2"
]

print(f"Found {len(section_322_chunks)} chunks for section 3.2.2")

# Extract only these chunks
from stage2_gpt_extraction import GPTContentExtractor

extractor = GPTContentExtractor()
docs_to_extract = [{
    "source": documents[0]["source"],
    "title": documents[0]["title"],
    "chunks": section_322_chunks,
    "metadata": documents[0]["metadata"]
}]

extracted = extractor.extract_batch(docs_to_extract)
```

## Example 10: Pipeline with Debugging

```python
import logging
from stage1_html_loader import HTMLLoader
from stage2_gpt_extraction import GPTContentExtractor
from stage3_neo4j_html_ingestion import Neo4jHTMLIngester
from ingestion.shared.src.core.graph_manager import GraphManager

# Enable DEBUG logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

try:
    # Stage 1
    logger.info("Starting Stage 1...")
    loader = HTMLLoader()
    chunks = loader.load_from_url()
    loader.save_chunks(chunks, "data/html_chunks.json")
    logger.info(f"Stage 1 complete: {len(chunks)} documents")

    # Stage 2
    logger.info("Starting Stage 2...")
    with open("data/html_chunks.json") as f:
        chunks_data = json.load(f)
    documents = chunks_data["documents"]

    extractor = GPTContentExtractor()
    extracted = extractor.extract_batch(documents)
    extractor.save_extracted(extracted, "data/gpt_extracted.json")
    logger.info(f"Stage 2 complete: {len(extracted)} documents processed")

    # Stage 3
    logger.info("Starting Stage 3...")
    graph = GraphManager()
    ingester = Neo4jHTMLIngester(graph)
    stats = ingester.ingest(extracted)
    logger.info(f"Stage 3 complete: {stats}")
    graph.close()

except Exception as e:
    logger.error(f"Pipeline failed: {e}", exc_info=True)
```

## Tips & Tricks

### Speed up development with cached chunks
```python
# First run: save chunks
chunks = loader.load_from_url()
loader.save_chunks(chunks, "output/chunks.json")

# Subsequent runs: load from cache (skips network call)
with open("output/chunks.json") as f:
    chunks_data = json.load(f)
documents = chunks_data["documents"]

# Process directly
extractor = GPTContentExtractor()
extracted = extractor.extract_batch(documents)
```

### Monitor API costs during Stage 2
```python
import json
from stage2_gpt_extraction import GPTContentExtractor

extractor = GPTContentExtractor()
chunks = [...]

estimated_calls = sum(len(doc["chunks"]) for doc in chunks)
cost_per_call = 0.005  # gpt-4o-mini text

print(f"Estimated API calls: {estimated_calls}")
print(f"Estimated cost: ${estimated_calls * cost_per_call:.2f}")
```

### Resume from checkpoint
```bash
# If Stage 2 fails, use existing chunks for Stage 3
python main.py --skip-stages 1

# Reuses html_chunks.json, starts from Stage 2
```
