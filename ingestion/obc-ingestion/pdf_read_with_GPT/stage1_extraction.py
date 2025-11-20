"""
Stage 1: Local extraction of OBC PDF structure
Extracts sections, tables, and images with hierarchy preserved
"""

import PyPDF2
import pdfplumber
from typing import Dict, List, Any
import re
import logging
import json
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class Stage1Extractor:
    """Extract PDF structure locally"""

    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.sections: List[Dict] = []
        self.tables: List[Dict] = []
        self.images: List[Dict] = []
        self.references: List[Dict] = []

    def extract(self) -> Dict[str, Any]:
        """Main extraction pipeline"""
        logger.info(f"Stage 1: Extracting from {self.pdf_path}")

        # Extract text with page boundaries
        pages = self._extract_text_pages()

        # Parse hierarchical sections
        self._parse_sections(pages)

        # Extract tables
        self._extract_tables()

        # Extract images
        self._extract_images()

        # Find cross-references
        self._find_references()

        logger.info(f"Extraction complete: {len(self.sections)} sections, {len(self.tables)} tables, {len(self.images)} images")

        return {
            'sections': self.sections,
            'tables': self.tables,
            'images': self.images,
            'references': self.references,
            'metadata': {
                'total_sections': len(self.sections),
                'total_tables': len(self.tables),
                'total_images': len(self.images),
                'total_pages': len(pages)
            }
        }

    def _extract_text_pages(self) -> List[Dict[str, Any]]:
        """Extract text preserving page boundaries"""
        pages = []
        try:
            with open(self.pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                for page_num, page in enumerate(reader.pages, 1):
                    text = page.extract_text()
                    pages.append({
                        'page': page_num,
                        'text': text if text else ""
                    })
            logger.info(f"Extracted {len(pages)} pages")
        except Exception as e:
            logger.error(f"Error extracting text: {e}")
        return pages

    def _parse_sections(self, pages: List[Dict[str, Any]]):
        """Parse hierarchical section structure with regex"""
        for page_data in pages:
            text = page_data['text']
            page_num = page_data['page']
            lines = text.split('\n')

            i = 0
            while i < len(lines):
                line = lines[i].strip()

                # Match section pattern: "6.1.1   Section Title"
                section_match = re.match(r'^(\d+(?:\.\d+)*)\.\s+(.*)$', line)

                if section_match:
                    section_num = section_match.group(1)
                    section_title = section_match.group(2).strip()

                    # Collect content until next section
                    content_lines = []
                    i += 1

                    while i < len(lines):
                        next_line = lines[i].strip()
                        if re.match(r'^(\d+(?:\.\d+)*)\.\s+', next_line):
                            break
                        if next_line.startswith('Table'):
                            break
                        if next_line:
                            content_lines.append(next_line)
                        i += 1

                    content = '\n'.join(content_lines).strip()
                    depth = len(section_num.split('.'))

                    self.sections.append({
                        'number': section_num,
                        'title': section_title,
                        'content': content,
                        'page': page_num,
                        'depth': depth
                    })
                else:
                    i += 1

        logger.info(f"Parsed {len(self.sections)} sections")

    def _extract_tables(self):
        """Extract tables with pdfplumber"""
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    tables = page.extract_tables()

                    if not tables:
                        continue

                    for table_idx, table in enumerate(tables):
                        if not table or len(table) == 0:
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

                        # Find table name from text
                        table_name = self._find_table_name(page_num)

                        self.tables.append({
                            'name': table_name,
                            'page': page_num,
                            'headers': headers,
                            'rows': rows
                        })

            logger.info(f"Extracted {len(self.tables)} tables")
        except Exception as e:
            logger.warning(f"Table extraction error: {e}")

    def _find_table_name(self, page_num: int) -> str:
        """Find table name from nearby sections"""
        for section in self.sections:
            if section['page'] == page_num:
                match = re.search(r'Table\s+([\w\-\.]+)', section['content'])
                if match:
                    return match.group(1)
        return f"Table_p{page_num}"

    def _extract_images(self):
        """Extract images from PDF"""
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    page_images = page.images

                    for img_idx, img in enumerate(page_images, 1):
                        try:
                            # Crop to image bounds using correct coordinates
                            cropped_page = page.crop((img['x0'], img['y0'], img['x1'], img['y1']))
                            page_img = cropped_page.to_image()
                            # Get the actual PIL image
                            pil_image = page_img.original

                            # Store image reference with metadata
                            self.images.append({
                                'page': page_num,
                                'image_num': img_idx,
                                'name': img.get('name', f'img_{page_num}_{img_idx}'),
                                'tag': img.get('tag', 'Image'),
                                'bbox': {
                                    'x0': img['x0'],
                                    'y0': img['y0'],
                                    'x1': img['x1'],
                                    'y1': img['y1']
                                },
                                'size': pil_image.size
                            })
                        except Exception as e:
                            logger.debug(f"Failed to extract image on page {page_num}: {e}")
                            continue

            logger.info(f"Extracted {len(self.images)} images")
        except Exception as e:
            logger.warning(f"Image extraction error: {e}")

    def _find_references(self):
        """Find cross-references between sections and tables"""
        reference_pattern = r'(Clause|Section|Table)\s+([0-9\.()]+)'

        for section in self.sections:
            matches = re.finditer(reference_pattern, section['content'])
            for match in matches:
                ref_type = match.group(1)
                ref_target = match.group(2)

                self.references.append({
                    'source': section['number'],
                    'type': ref_type,
                    'target': ref_target,
                    'context': section['content'][max(0, match.start()-50):match.end()+50]
                })

        logger.info(f"Found {len(self.references)} cross-references")
