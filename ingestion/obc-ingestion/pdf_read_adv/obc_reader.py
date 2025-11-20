"""
Specialized reader for Ontario Building Code PDFs.

Extracts and preserves hierarchical structure, tables, and cross-references.
Designed to handle:
- Section numbering (6.1, 6.1.1, etc.)
- Complex tables with multiple columns, images, footnotes
- Text content with regulatory language
- Cross-references and citations
"""

import PyPDF2
import pdfplumber
from typing import Dict, List, Any, Optional, Tuple
import re
import logging
from dataclasses import dataclass, asdict
import json
import os
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class Section:
    """Represents a section in the building code"""
    number: str  # e.g., "6.1.1"
    title: str
    content: str
    page: int
    depth: int  # Hierarchy level (6=depth 1, 6.1=depth 2, etc.)
    subsections: List['Section'] = None

    def __post_init__(self):
        if self.subsections is None:
            self.subsections = []


@dataclass
class TableData:
    """Represents an extracted table"""
    name: str  # e.g., "Table D-6.1.1"
    title: str
    page: int
    headers: List[str]
    rows: List[Dict[str, str]]
    footnotes: List[str] = None

    def __post_init__(self):
        if self.footnotes is None:
            self.footnotes = []


class OBCStructuredReader:
    """
    Extracts OBC with structural awareness.

    Uses pdfplumber for tables + custom parsing for hierarchy.
    """

    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.sections: List[Section] = []
        self.tables: List[TableData] = []
        self.references: List[Dict] = []

    def read(self) -> Dict[str, Any]:
        """
        Main extraction method.

        Returns:
            {
                'sections': [...],
                'tables': [...],
                'references': [...],
                'metadata': {...}
            }
        """
        logger.info(f"Reading OBC from: {self.pdf_path}")

        # Extract text with PyPDF2 for structure
        text_pages = self._extract_with_pypdf2()

        # Parse sections and hierarchy
        self._parse_sections(text_pages)

        # Extract tables with pdfplumber
        self._extract_tables_with_pdfplumber()

        # Find cross-references
        self._find_references()

        return {
            'sections': [asdict(s) for s in self.sections],
            'tables': [asdict(t) for t in self.tables],
            'references': self.references,
            'metadata': {
                'total_sections': len(self.sections),
                'total_tables': len(self.tables),
                'total_pages': len(text_pages)
            }
        }

    def _extract_with_pypdf2(self) -> List[Dict[str, str]]:
        """Extract text preserving page boundaries"""
        pages = []

        with open(self.pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)

            for page_num, page in enumerate(reader.pages, 1):
                text = page.extract_text()
                pages.append({
                    'page': page_num,
                    'text': text if text else ""
                })

        logger.info(f"Extracted {len(pages)} pages")
        return pages

    def _parse_sections(self, pages: List[Dict[str, str]]):
        """Parse hierarchical section structure"""

        for page_data in pages:
            text = page_data['text']
            page_num = page_data['page']

            # Split by lines for processing
            lines = text.split('\n')

            i = 0
            while i < len(lines):
                line = lines[i].strip()

                # Detect section header pattern: "6.1.1.    Section Title"
                section_match = re.match(r'^(\d+(?:\.\d+)*)\.\s+(.*)$', line)

                if section_match:
                    section_num = section_match.group(1)
                    section_title = section_match.group(2).strip()

                    # Collect content until next section
                    content_lines = []
                    i += 1

                    while i < len(lines):
                        next_line = lines[i].strip()

                        # Stop if we hit another section
                        if re.match(r'^(\d+(?:\.\d+)*)\.\s+', next_line):
                            break

                        # Stop if we hit a table
                        if next_line.startswith('Table'):
                            break

                        if next_line:
                            content_lines.append(next_line)

                        i += 1

                    content = '\n'.join(content_lines).strip()
                    depth = len(section_num.split('.'))

                    section = Section(
                        number=section_num,
                        title=section_title,
                        content=content,
                        page=page_num,
                        depth=depth
                    )

                    self.sections.append(section)
                else:
                    i += 1

        logger.info(f"Parsed {len(self.sections)} sections")

    def _extract_tables_with_pdfplumber(self):
        """Extract tables preserving structure"""

        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    tables = page.extract_tables()

                    if not tables:
                        continue

                    for table_idx, table in enumerate(tables):
                        if not table:
                            continue

                        # First row is headers
                        headers = [str(cell).strip() if cell else "" for cell in table[0]]

                        # Remaining rows are data
                        rows = []
                        for row in table[1:]:
                            row_dict = {}
                            for col_idx, cell in enumerate(row):
                                header = headers[col_idx] if col_idx < len(headers) else f"Col_{col_idx}"
                                row_dict[header] = str(cell).strip() if cell else ""
                            rows.append(row_dict)

                        # Try to find table name from previous content
                        table_name = self._find_table_name(page_num, table_idx)

                        table = TableData(
                            name=table_name,
                            title=f"Table on page {page_num}",
                            page=page_num,
                            headers=headers,
                            rows=rows
                        )

                        self.tables.append(table)

            logger.info(f"Extracted {len(self.tables)} tables")

        except Exception as e:
            logger.warning(f"Table extraction failed: {e}")

    def _find_table_name(self, page_num: int, table_idx: int) -> str:
        """Find table name/caption"""
        # Look in sections on this page for table references
        for section in self.sections:
            if section.page == page_num:
                match = re.search(r'Table\s+([\w\-\.]+)', section.content)
                if match:
                    return match.group(1)

        return f"Table_{page_num}_{table_idx}"

    def _find_references(self):
        """Find cross-references and citations"""

        # Pattern for references like "Clause 3.1.5.5(1)(b)"
        reference_pattern = r'(Clause|Section|Table)\s+([0-9\.()]+)'

        for section in self.sections:
            matches = re.finditer(reference_pattern, section.content)

            for match in matches:
                ref_type = match.group(1)
                ref_target = match.group(2)

                self.references.append({
                    'source': section.number,
                    'type': ref_type,
                    'target': ref_target,
                    'context': section.content[max(0, match.start()-50):match.end()+50]
                })

        logger.info(f"Found {len(self.references)} cross-references")

    def extract_images(self, output_dir: str = "extracted_images") -> List[Dict[str, Any]]:
        """
        Extract all images from PDF and save to disk.

        Args:
            output_dir: Directory to save images (created if doesn't exist)

        Returns:
            List of image metadata dicts with:
            {
                'page': int,
                'image_num': int,
                'filename': str,
                'path': str,
                'size': (width, height)
            }
        """
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        images_extracted = []

        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    # Get all images on this page
                    page_images = page.images

                    for img_idx, img in enumerate(page_images, 1):
                        try:
                            # Get image data
                            img_obj = page.within_bbox(img['top'], img['x0'], img['bottom'], img['x1']).images

                            if img_obj:
                                # Extract and save using pdfplumber's crop method
                                cropped = page.crop((img['x0'], img['top'], img['x1'], img['bottom']))
                                im = cropped.to_image()

                                # Generate filename
                                filename = f"page_{page_num:03d}_img_{img_idx:02d}.png"
                                filepath = os.path.join(output_dir, filename)

                                # Save image
                                im.save(filepath)

                                # Record metadata
                                images_extracted.append({
                                    'page': page_num,
                                    'image_num': img_idx,
                                    'filename': filename,
                                    'path': os.path.abspath(filepath),
                                    'bbox': {
                                        'x0': img['x0'],
                                        'y0': img['top'],
                                        'x1': img['x1'],
                                        'y1': img['bottom']
                                    }
                                })

                                logger.debug(f"Extracted image: {filename}")

                        except Exception as e:
                            logger.warning(f"Failed to extract image on page {page_num}: {e}")
                            continue

            logger.info(f"Extracted {len(images_extracted)} images to {output_dir}")
            return images_extracted

        except Exception as e:
            logger.error(f"Error extracting images: {e}")
            return []

    def export_json(self, output_path: str):
        """Export structured data to JSON"""
        data = self.read()

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Exported to {output_path}")

    def build_graph_data(self) -> Dict[str, Any]:
        """
        Convert extracted data to format suitable for graph building.

        Returns format expected by SchemaExtractor:
        {
            'nodes': [...],
            'relationships': [...]
        }
        """
        nodes = []
        relationships = []

        section_map = {}  # Track IDs for relationships

        # Create Section nodes
        for i, section in enumerate(self.sections):
            section_id = f"section_{section.number}"
            section_map[section.number] = section_id

            nodes.append({
                'id': section_id,
                'label': 'Section',
                'properties': {
                    'number': section.number,
                    'title': section.title,
                    'page': section.page,
                    'depth': section.depth,
                    'content': section.content[:500] if section.content else ""
                }
            })

        # Create Table nodes
        for i, table in enumerate(self.tables):
            table_id = f"table_{table.name}"

            nodes.append({
                'id': table_id,
                'label': 'Table',
                'properties': {
                    'name': table.name,
                    'title': table.title,
                    'page': table.page,
                    'columns': len(table.headers),
                    'rows': len(table.rows)
                }
            })

            # Link table to sections that reference it
            for ref in self.references:
                if table.name in ref['target']:
                    if ref['source'] in section_map:
                        relationships.append({
                            'source_id': section_map[ref['source']],
                            'target_id': table_id,
                            'type': 'REFERENCES',
                            'properties': {'context': ref['context'][:200]}
                        })

        # Create parent-child relationships for hierarchy
        for i, section in enumerate(self.sections):
            if section.depth > 1:
                # Find parent section
                parent_num = '.'.join(section.number.split('.')[:-1])

                if parent_num in section_map:
                    relationships.append({
                        'source_id': section_map[parent_num],
                        'target_id': section_map[section.number],
                        'type': 'HAS_SUBSECTION',
                        'properties': {}
                    })

        return {
            'nodes': nodes,
            'relationships': relationships
        }


# For compatibility with existing code
def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract text preserving some structure"""
    reader = OBCStructuredReader(pdf_path)
    data = reader.read()

    # Format as readable text
    output = []
    for section in reader.sections:
        output.append(f"\n{'=' * 60}")
        output.append(f"Section {section.number}: {section.title}")
        output.append(f"{'=' * 60}")
        output.append(section.content)

    for table in reader.tables:
        output.append(f"\n{'-' * 60}")
        output.append(f"Table: {table.name}")
        output.append(f"{'-' * 60}")
        # Simple table format
        for row in table.rows:
            output.append(str(row))

    return '\n'.join(output)
