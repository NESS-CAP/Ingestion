"""
Improved HTML Extraction - Better Clause Detection

Fixed issues with the original:
1. Regex patterns were too greedy/restrictive
2. Not properly handling nested structures
3. Stopping extraction too early

New approach:
- Split by clause delimiters first (1), (2), (3)...
- Then handle nested subclauses (a), (b), (c)...
- Then handle items (i), (ii), (iii)...
- Capture EVERYTHING within each level
"""

import logging
from typing import Dict, List, Any, Optional
import json
import re
from pathlib import Path
import sys
import subprocess
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parents[4]))

logger = logging.getLogger(__name__)


class HTMLExtractorV2:
    """Improved HTML extraction with better clause detection"""

    def __init__(self):
        pass

    def extract_from_url(self, url: str) -> Dict[str, Any]:
        """Extract from HTML URL"""
        logger.info(f"Fetching HTML from {url}")

        try:
            # Use curl which seems to get the full content better than requests
            logger.debug("Using curl to fetch HTML")
            result = subprocess.run(
                ['curl', '-s', url],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                logger.warning(f"curl failed: {result.stderr}, falling back to requests")
                raise RuntimeError("curl failed")

            html_content = result.stdout

        except (FileNotFoundError, RuntimeError):
            # Fallback to requests if curl not available
            logger.debug("Falling back to requests")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, timeout=30, headers=headers)
            response.raise_for_status()
            html_content = response.text

        return self.extract_from_html(html_content)

    def extract_from_html(self, html_content: str) -> Dict[str, Any]:
        """Extract all structure from HTML using BeautifulSoup with CSS classes"""
        logger.info("Parsing HTML with CSS class-based extraction")
        soup = BeautifulSoup(html_content, 'html.parser')

        # Remove scripts and styles
        for script in soup(["script", "style"]):
            script.decompose()

        # Extract sections using CSS classes: ruleb-e for main sections
        sections = self._extract_sections_by_css(soup)

        logger.info(f"Extracted {len(sections)} sections")

        # Extract clauses from each section
        total_clauses = 0
        for section in sections:
            section_text = section.get("content", "")

            # Extract clauses using smart regex-based approach
            extracted = self._extract_clauses_smart(section_text)

            section["extracted_clauses"] = extracted.get("clauses", [])
            total_clauses += len(section["extracted_clauses"])

        return {
            "source": "HTML extraction v2 (CSS-based)",
            "sections": sections,
            "total_sections": len(sections),
            "total_clauses": total_clauses,
        }

    def _extract_sections_by_css(self, soup) -> List[Dict[str, Any]]:
        """Extract sections using BeautifulSoup CSS class selectors (ruleb-e, section-e, subsection-e)"""
        sections = []

        # Find all section headers with class 'ruleb-e'
        section_headers = soup.find_all('p', class_='ruleb-e')

        logger.info(f"Found {len(section_headers)} section headers")

        for header_idx, header in enumerate(section_headers):
            section_text = header.get_text().strip()

            # Extract section number (e.g., "1.1.1." or "Section 1.1.")
            section_match = re.match(r'^(?:Section\s+)?(\d+\.\d+(?:\.\d+)*)', section_text)
            if not section_match:
                logger.debug(f"Skipping header without number: {section_text[:50]}")
                continue

            section_num = section_match.group(1)
            section_title = section_text

            # Collect all content until the next section header
            content_lines = []

            # Start from the next element after this header
            current = header.next_sibling

            while current:
                # Stop if we hit the next section header
                if isinstance(current, type(header)):
                    if hasattr(current, 'get') and current.get('class'):
                        if 'ruleb-e' in current.get('class', []):
                            break

                # Extract text from various content elements
                if hasattr(current, 'get_text'):
                    # Use separator to add spaces between inline elements (em, strong, etc.)
                    text = current.get_text(separator=' ', strip=True)
                    # Clean up multiple spaces
                    text = re.sub(r'\s+', ' ', text)
                    if text and len(text) > 3:  # Skip very short lines
                        content_lines.append(text)

                current = current.next_sibling

            content_text = '\n'.join(content_lines).strip()

            if content_text and len(content_text) > 10:
                section = {
                    "section_number": section_num,
                    "title": section_title,
                    "content": content_text,
                    "extracted_clauses": []
                }
                sections.append(section)
                logger.debug(f"Extracted section {section_num}: {len(content_text)} chars")

        return sections

    def _extract_all_sections(self, text: str) -> List[Dict[str, Any]]:
        """Extract sections using section number patterns (fallback for plain text)"""
        sections = []

        # Pattern to find section numbers: 3.2.2, 3.2.2.1, etc.
        section_pattern = r'^(\d+\.\d+(?:\.\d+)*)\s*[.:]?\s*(.+?)$'

        lines = text.split('\n')
        current_section = None
        current_content = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check if this line starts a new section
            match = re.match(section_pattern, line)
            if match:
                # Save previous section
                if current_section:
                    content_text = '\n'.join(current_content).strip()
                    if content_text:
                        current_section["content"] = content_text
                        sections.append(current_section)

                # Start new section
                section_num = match.group(1)
                section_title = match.group(2)

                current_section = {
                    "section_number": section_num,
                    "title": section_title,
                    "content": "",
                    "extracted_clauses": []
                }
                current_content = []
            else:
                # Add line to current section
                if current_section:
                    current_content.append(line)

        # Save last section
        if current_section:
            content_text = '\n'.join(current_content).strip()
            if content_text:
                current_section["content"] = content_text
                sections.append(current_section)

        return sections

    def _extract_clauses_smart(self, text: str) -> Dict[str, Any]:
        """Smart clause extraction using multi-pass approach"""
        clauses = []

        # Split by main clause delimiters: (1), (2), (3), ..., (18), (18.1), (xii), (xiii), (xviii.1), etc.
        # Matches: (digits), (digits.digits), (roman numerals), or (roman numerals.digits) formats
        # Roman numerals: i, ii, iii, iv, v, vi, vii, viii, ix, x, xi, xii, xiii, xiv, xv, xvi, xvii, xviii, xix, xx, etc.
        roman_pattern = r'(?:xx|xix|xviii|xvii|xvi|xv|xiv|xiii|xii|xi|x|ix|viii|vii|vi|v|iv|iii|ii|i)'
        clause_delimiter_pattern = f'(?=\\((?:\\d+(?:\\.\\d+)?|{roman_pattern}(?:\\.\\d+)?)\\))'
        clause_splits = re.split(clause_delimiter_pattern, text)

        for clause_idx, clause_text in enumerate(clause_splits):
            clause_text = clause_text.strip()
            if not clause_text:
                continue

            # Extract the clause number - supports (1), (18.1), (xii), (xviii.1) formats
            clause_match = re.match(f'\\((\\d+(?:\\.\\d+)?|{roman_pattern}(?:\\.\\d+)?)\\)(.*)', clause_text, re.DOTALL)
            if not clause_match:
                continue

            clause_num = clause_match.group(1)
            clause_content = clause_match.group(2).strip()

            # Find where the next clause would start (if any)
            # Only look for next clause at line boundaries
            next_clause_match = re.search(f'^\\((?:\\d+(?:\\.\\d+)?|{roman_pattern}(?:\\.\\d+)?)\\)', clause_content, re.MULTILINE)
            if next_clause_match:
                # This means we have content from the next clause, truncate there
                clause_content = clause_content[:next_clause_match.start()].strip()

            if len(clause_content) < 10:
                continue

            # Now extract subclauses from this clause
            nested_items = self._extract_nested_items(clause_content)

            clause_obj = {
                "number": f"({clause_num})",
                "text": clause_content,  # Store FULL text without truncation
                "type": "clause",
                "nested_items": nested_items
            }

            clauses.append(clause_obj)

        return {
            "clauses": clauses,
            "definitions": [],
            "references": []
        }

    def _extract_nested_items(self, text: str) -> List[Dict[str, Any]]:
        """Extract nested items: (a), (b), (c), etc."""
        nested = []

        # Split by subclause delimiters: (a), (b), (c), etc.
        subclause_splits = re.split(r'(?=\([a-z]\))', text)

        for sub_idx, subclause_text in enumerate(subclause_splits):
            subclause_text = subclause_text.strip()
            if not subclause_text:
                continue

            # Extract the letter
            subclause_match = re.match(r'\(([a-z])\)(.*)', subclause_text, re.DOTALL)
            if not subclause_match:
                continue

            sub_letter = subclause_match.group(1)
            sub_content = subclause_match.group(2).strip()

            # Find where next subclause starts (at line start only)
            next_sub_match = re.search(r'^\([a-z]\)', sub_content)
            if next_sub_match:
                sub_content = sub_content[:next_sub_match.start()].strip()

            if len(sub_content) < 5:
                continue

            # Extract roman numerals if they exist
            items = self._extract_roman_items(sub_content)

            nested_obj = {
                "number": f"({sub_letter})",
                "text": sub_content,  # Store FULL text without truncation
                "type": "subclause",
                "nested_items": items
            }

            nested.append(nested_obj)

        return nested

    def _extract_roman_items(self, text: str) -> List[Dict[str, Any]]:
        """Extract roman numeral items: (i), (ii), (iii), (iv), (v), etc."""
        items = []

        # Pattern for roman numerals: (i), (ii), (iii), (iv), (v), (vi), (vii), (viii), (ix), etc.
        # Matches: i, ii, iii, iv, v, vi, vii, viii, ix
        roman_pattern = r'\(((?:viii|vii|vi|iv|ix|v|iii|ii|i))\)'

        roman_splits = re.split(f'(?={roman_pattern})', text)

        for roman_idx, roman_text in enumerate(roman_splits):
            roman_text = roman_text.strip()
            if not roman_text:
                continue

            roman_match = re.match(f'{roman_pattern}(.*)', roman_text, re.DOTALL)
            if not roman_match:
                continue

            roman_num = roman_match.group(1)
            roman_content = roman_match.group(2).strip()

            # Find next roman numeral (at line start only to avoid false positives)
            next_roman_match = re.search(f'^{roman_pattern}', roman_content, re.MULTILINE)
            if next_roman_match:
                roman_content = roman_content[:next_roman_match.start()].strip()

            if len(roman_content) < 5:
                continue

            item_obj = {
                "number": f"({roman_num})",
                "text": roman_content,  # Store FULL text without truncation
                "type": "item"
            }

            items.append(item_obj)

        return items

    def save_extraction(self, extraction: Dict[str, Any], output_path: str):
        """Save extracted data"""
        logger.info(f"Saving extraction to {output_path}")

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(extraction, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved: {extraction['total_clauses']} clauses")


def main():
    """Test extraction"""
    import sys

    logging.basicConfig(level=logging.INFO)

    # Load URL from external file
    possible_paths = [
        Path(__file__).parents[4] / "ingestion" / "data" / "e-laws_url.txt",
        Path(__file__).parents[4] / "data" / "e-laws_url.txt",
    ]

    elaws_url = "https://www.ontario.ca/laws/regulation/120332"  # Default
    for url_file in possible_paths:
        if url_file.exists():
            with open(url_file, 'r') as f:
                elaws_url = f.read().strip()
                break

    extractor = HTMLExtractorV2()

    try:
        logger.info("Starting extraction...")
        extraction = extractor.extract_from_url(elaws_url)
        extractor.save_extraction(extraction, "data/extracted.json")

        logger.info(f"Success: {extraction['total_clauses']} clauses extracted")
        return True

    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
