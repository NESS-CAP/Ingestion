# Advanced PDF Reader for Ontario Building Code

Structured PDF extraction that preserves hierarchy, tables, and images.

## Features

- ✓ Hierarchical sections (6.1 → 6.1.1 → 6.1.1.1)
- ✓ Perfect table extraction with headers and rows
- ✓ Image extraction from design diagrams
- ✓ Cross-reference detection (Clause 3.1.5.5, etc.)
- ✓ JSON export format
- ✓ Neo4j graph ready
- ✓ No LLM required (all local)
- ✓ Fast (~25 sec for 500-page PDF)

## Usage

**Use the main script in root directory:**

```bash
python extract.py
```

**Or use directly in code:**

```python
from pdf_read_adv import OBCStructuredReader

reader = OBCStructuredReader("path/to/file.pdf")
data = reader.read()

# Access extracted data
print(f"Sections: {len(reader.sections)}")
print(f"Tables: {len(reader.tables)}")

# Export to JSON
reader.export_json("output.json")

# Extract images
images = reader.extract_images("output_images/")

# Convert to Neo4j format
graph_data = reader.build_graph_data()
```

## Data Classes

### Section
```python
section.number       # "6.1.1"
section.title        # "Exterior Wall Assemblies"
section.content      # Full text content
section.page         # Page number
section.depth        # Hierarchy level (1, 2, 3)
```

### TableData
```python
table.name           # "Table D-6.1.1"
table.title          # Full title
table.page           # Page number
table.headers        # List of column headers
table.rows           # List of dict rows
```

## Performance

- 1 page: ~0.5 seconds
- 10 pages: ~2-3 seconds
- 50 pages: ~5-10 seconds
- 500 pages: ~25-30 seconds

## Installation

```bash
pip install pdfplumber
```

## Troubleshooting

### ModuleNotFoundError: pdfplumber
```bash
pip install pdfplumber
```

### No sections detected
Check that section numbers match pattern: `\d+\.\d+(\.\d+)*`

### Tables not extracted
Some PDFs have image-based tables which cannot be extracted.
