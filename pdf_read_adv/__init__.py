"""Advanced PDF reading module for structured documents like Ontario Building Code

Features:
- Extract hierarchical sections (6.1 → 6.1.1 → 6.1.1.1)
- Extract tables with perfect structure
- Extract all images
- Find cross-references and citations
- Output Neo4j-ready graph format
"""

from .obc_reader import (
    OBCStructuredReader,
    Section,
    TableData,
    extract_text_from_pdf
)

__all__ = [
    'OBCStructuredReader',
    'Section',
    'TableData',
    'extract_text_from_pdf'
]
