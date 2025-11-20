# PDF Text Extraction Module

Extracts text from PDF files and saves as text files for use in the schema-based ingestion pipeline.

## Directory Structure

```
pdf/
├── data/           # Place your PDF files here
├── pdf_reader.py   # PDF extraction utilities
├── extract_pdfs.py # Main extraction script
└── README.md       # This file
```

## Usage

### 1. Place PDFs in `pdf/data/`

Copy your PDF files to the `pdf/data/` directory:
```bash
cp your_document.pdf pdf/data/
```

### 2. Run the Extraction Script

```bash
PYTHONPATH=. python3 pdf/extract_pdfs.py
```

This will:
- Find all `.pdf` files in `pdf/data/`
- Extract text from each page
- Save as `.txt` files in the main `data/` directory

### 3. Use the Text in Schema Ingestion

The extracted text files are now available in `data/` for the schema-based ingestion pipeline:

```bash
PYTHONPATH=. python3 scripts/ingest_schema.py
```

You can modify `scripts/ingest_schema.py` to read from `data/` instead of using hardcoded text.

## Example

**Before:**
```
project/
├── pdf/
│   ├── data/
│   │   ├── contract1.pdf
│   │   └── contract2.pdf
├── data/           # Empty
└── scripts/
```

**After running `pdf/extract_pdfs.py`:**
```
project/
├── pdf/
│   ├── data/
│   │   ├── contract1.pdf
│   │   └── contract2.pdf
├── data/
│   ├── contract1.txt     # New!
│   └── contract2.txt     # New!
└── scripts/
```

## Programmatic Usage

### Extract Single PDF

```python
from pdf.pdf_reader import extract_text_from_pdf

text = extract_text_from_pdf("pdf/data/document.pdf")
print(text)
```

### Extract All PDFs

```python
from pdf.pdf_reader import extract_all_pdfs, save_extracted_text

# Extract all PDFs
results = extract_all_pdfs()  # defaults to pdf/data/

# Process each one
for pdf_name, text in results.items():
    # Do something with text
    save_extracted_text(text, pdf_name.replace('.pdf', '.txt'))
```

### Custom Output Directory

```python
from pdf.pdf_reader import save_extracted_text

text = "extracted text here..."

# Save to custom location
save_extracted_text(text, "output.txt", output_dir="/custom/path")
```

## Integration with Schema Ingestion

**Modified `scripts/ingest_schema.py`:**

```python
import os
from pathlib import Path
from pdf.pdf_reader import extract_all_pdfs

# Extract PDFs first
pdf_results = extract_all_pdfs()

# Process each extracted text
for pdf_name, text in pdf_results.items():
    result = ingest_document(text, schema, graph, builder)
    print(f"Processed {pdf_name}: {result['nodes_created']} nodes")
```

## Dependencies

The `pdf_reader.py` module requires:
```
PyPDF2>=3.0.0
```

Add to `requirements.txt` if not already present:
```bash
echo "PyPDF2>=3.0.0" >> requirements.txt
pip install -r requirements.txt
```

## Features

- ✅ Extract text from multiple PDFs
- ✅ Preserve page breaks in output
- ✅ Save extracted text as individual files
- ✅ Handle extraction errors gracefully
- ✅ Detailed logging of extraction progress
- ✅ Works with any PDF format

## Troubleshooting

### "No PDF files found"
- Check that PDF files are in `pdf/data/` directory
- Verify filenames end with `.pdf` (lowercase)
- Check file permissions

### PDF extraction fails
- Ensure PDF is readable (not corrupted)
- Try opening the PDF in a PDF reader to verify
- Check console output for specific error

### Text not saving
- Verify `data/` directory exists in project root
- Check write permissions to `data/` directory
- Look for error messages in console output

## Notes

- Pages are separated by `--- Page N ---` headers in the output
- Empty pages will be skipped
- Special characters are preserved in the extraction
- Text encoding defaults to UTF-8
