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
import requests
from bs4 import BeautifulSoup
from openai import OpenAI

sys.path.insert(0, str(Path(__file__).parents[3]))

logger = logging.getLogger(__name__)


class HTMLExtractorV2:
    """Improved HTML extraction with better clause detection"""

    def __init__(self, use_gpt: bool = True, gpt_model: str = "gpt-4o-mini"):
        self.use_gpt = use_gpt
        self.gpt_model = gpt_model
        if use_gpt:
            self.client = OpenAI()

    def extract_from_url(self, url: str) -> Dict[str, Any]:
        """Extract from HTML URL"""
        logger.info(f"Fetching HTML from {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return self.extract_from_html(response.text)

    def extract_from_html(self, html_content: str) -> Dict[str, Any]:
        """Extract all structure from HTML"""
        logger.info("Parsing HTML")
        soup = BeautifulSoup(html_content, 'html.parser')

        # Remove scripts and styles
        for script in soup(["script", "style"]):
            script.decompose()

        # Get all text with minimal processing
        text = soup.get_text(separator='\n', strip=True)

        # Extract sections using the natural structure
        sections = self._extract_all_sections(text)

        logger.info(f"Extracted {len(sections)} sections")

        # Extract clauses from each section
        total_clauses = 0
        for section in sections:
            section_text = section.get("content", "")

            # Use GPT for better extraction if available
            if self.use_gpt and section_text.strip():
                extracted = self._extract_with_gpt(section_text, section.get("section_number", ""))
            else:
                extracted = self._extract_clauses_smart(section_text)

            section["extracted_clauses"] = extracted.get("clauses", [])
            total_clauses += len(section["extracted_clauses"])

        return {
            "source": "HTML extraction v2",
            "sections": sections,
            "total_sections": len(sections),
            "total_clauses": total_clauses,
        }

    def _extract_all_sections(self, text: str) -> List[Dict[str, Any]]:
        """Extract sections using section number patterns"""
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

        # Split by main clause delimiters: (1), (2), (3), etc.
        # Use a positive lookahead to split but keep the delimiter
        clause_splits = re.split(r'(?=\(\d+\))', text)

        for clause_text in clause_splits:
            clause_text = clause_text.strip()
            if not clause_text:
                continue

            # Extract the clause number
            clause_match = re.match(r'\((\d+)\)(.*)', clause_text, re.DOTALL)
            if not clause_match:
                continue

            clause_num = clause_match.group(1)
            clause_content = clause_match.group(2).strip()

            # Find where the next clause would start (if any)
            next_clause_match = re.search(r'\(\d+\)', clause_content)
            if next_clause_match:
                clause_content = clause_content[:next_clause_match.start()].strip()

            if len(clause_content) < 10:
                continue

            # Now extract subclauses from this clause
            nested_items = self._extract_nested_items(clause_content)

            clause_obj = {
                "number": f"({clause_num})",
                "text": clause_content[:1000],  # Store full text
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

        for subclause_text in subclause_splits:
            subclause_text = subclause_text.strip()
            if not subclause_text:
                continue

            # Extract the letter
            subclause_match = re.match(r'\(([a-z])\)(.*)', subclause_text, re.DOTALL)
            if not subclause_match:
                continue

            sub_letter = subclause_match.group(1)
            sub_content = subclause_match.group(2).strip()

            # Find where next subclause starts
            next_sub_match = re.search(r'\([a-z]\)', sub_content)
            if next_sub_match:
                sub_content = sub_content[:next_sub_match.start()].strip()

            if len(sub_content) < 5:
                continue

            # Extract roman numerals if they exist
            items = self._extract_roman_items(sub_content)

            nested_obj = {
                "number": f"({sub_letter})",
                "text": sub_content[:500],
                "type": "subclause",
                "nested_items": items
            }

            nested.append(nested_obj)

        return nested

    def _extract_roman_items(self, text: str) -> List[Dict[str, Any]]:
        """Extract roman numeral items: (i), (ii), (iii), etc."""
        items = []

        # Pattern for roman numerals: (i), (ii), (iii), (iv), etc.
        roman_pattern = r'\(([iv]+)\)'

        roman_splits = re.split(f'(?={roman_pattern})', text)

        for roman_text in roman_splits:
            roman_text = roman_text.strip()
            if not roman_text:
                continue

            roman_match = re.match(f'{roman_pattern}(.*)', roman_text, re.DOTALL)
            if not roman_match:
                continue

            roman_num = roman_match.group(1)
            roman_content = roman_match.group(2).strip()

            # Find next roman numeral
            next_roman_match = re.search(roman_pattern, roman_content)
            if next_roman_match:
                roman_content = roman_content[:next_roman_match.start()].strip()

            if len(roman_content) < 5:
                continue

            item_obj = {
                "number": f"({roman_num})",
                "text": roman_content[:300],
                "type": "item"
            }

            items.append(item_obj)

        return items

    def _extract_with_gpt(self, text: str, section_number: str) -> Dict[str, Any]:
        """Use GPT for extraction as fallback"""
        logger.debug(f"Using GPT for section {section_number}")

        prompt = f"""Extract ALL clauses, subclauses, and items from this text.

Return ONLY this JSON format (no other text):
{{
    "clauses": [
        {{
            "number": "(1)",
            "text": "full text",
            "nested_items": [
                {{"number": "(a)", "text": "text"}},
                {{"number": "(b)", "text": "text"}}
            ]
        }},
        {{
            "number": "(2)",
            "text": "text",
            "nested_items": []
        }}
    ]
}}

TEXT (section {section_number}):
{text[:3000]}
"""

        try:
            response = self.client.messages.create(
                model=self.gpt_model,
                max_tokens=3000,
                messages=[{"role": "user", "content": prompt}]
            )

            response_text = response.content[0].text

            # Parse JSON
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group())
                return {
                    "clauses": parsed.get("clauses", []),
                    "definitions": [],
                    "references": []
                }

        except Exception as e:
            logger.warning(f"GPT extraction failed: {e}, falling back to smart extraction")

        # Fallback to smart extraction
        return self._extract_clauses_smart(text)

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
    from ingestion.shared.config.sources import ELAWS_OBC_HTML_URL

    logging.basicConfig(level=logging.INFO)

    extractor = HTMLExtractorV2(use_gpt=True)

    try:
        logger.info("Starting extraction...")
        extraction = extractor.extract_from_url(ELAWS_OBC_HTML_URL)
        extractor.save_extraction(extraction, "data/extracted.json")

        logger.info(f"Success: {extraction['total_clauses']} clauses extracted")
        return True

    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
