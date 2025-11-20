"""
Simplified HTML Extraction - All-in-One

Combines semantic chunking + intelligent extraction in ONE stage.
Uses BeautifulSoup for parsing + optional GPT for edge cases.

No unnecessary stages. Just pure extraction.
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
import hashlib

sys.path.insert(0, str(Path(__file__).parents[3]))

logger = logging.getLogger(__name__)


class HTMLExtractor:
    """Extract structure and clauses from HTML documents"""

    def __init__(self, use_gpt: bool = True, gpt_model: str = "gpt-4o-mini"):
        """
        Initialize extractor.

        Args:
            use_gpt: Whether to use GPT for intelligent extraction (recommended for complex text)
            gpt_model: Which GPT model to use
        """
        self.use_gpt = use_gpt
        self.gpt_model = gpt_model
        if use_gpt:
            self.client = OpenAI()

    def extract_from_url(self, url: str) -> Dict[str, Any]:
        """
        Extract structured content from HTML URL.

        Args:
            url: URL to fetch

        Returns:
            Structured extraction with sections and clauses
        """
        logger.info(f"Fetching HTML from {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        return self.extract_from_html(response.text)

    def extract_from_html(self, html_content: str) -> Dict[str, Any]:
        """
        Extract structured content from HTML string.

        Args:
            html_content: HTML string to process

        Returns:
            Structured extraction
        """
        logger.info("Parsing HTML with BeautifulSoup")

        soup = BeautifulSoup(html_content, 'html.parser')

        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()

        # Extract sections using header hierarchy
        sections = self._extract_sections_from_html(soup)

        logger.info(f"Extracted {len(sections)} sections")

        # For each section, extract clauses
        for section in sections:
            section_text = section.get("content", "")
            section_number = section.get("section_number", "")

            # Try to extract clauses intelligently
            if self.use_gpt and section_text.strip():
                # Use GPT only for complex regulatory text
                extracted = self._extract_clauses_with_gpt(
                    section_text, section_number
                )
            else:
                # Fall back to structural extraction
                extracted = self._extract_clauses_from_text(section_text)

            section["extracted_clauses"] = extracted.get("clauses", [])
            section["extracted_definitions"] = extracted.get("definitions", [])
            section["extracted_references"] = extracted.get("references", [])

        return {
            "source": "HTML extraction",
            "sections": sections,
            "total_sections": len(sections),
            "total_clauses": sum(
                len(s.get("extracted_clauses", [])) for s in sections
            ),
        }

    def _extract_sections_from_html(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extract sections using HTML header hierarchy.

        Args:
            soup: BeautifulSoup object

        Returns:
            List of sections with content and hierarchy
        """
        sections = []
        current_hierarchy = {}
        current_section_content = []

        for element in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'div']):
            # Handle header elements
            if element.name.startswith('h'):
                # Save previous section if exists
                if current_section_content:
                    section_text = '\n'.join(current_section_content)
                    if section_text.strip():
                        sections.append({
                            "section_number": current_hierarchy.get(4, ""),
                            "division": current_hierarchy.get(2, ""),
                            "part": current_hierarchy.get(3, ""),
                            "title": current_hierarchy.get(4, ""),
                            "content": section_text,
                            "extracted_clauses": [],
                            "extracted_definitions": [],
                            "extracted_references": []
                        })
                    current_section_content = []

                # Update hierarchy
                level = int(element.name[1])
                text = element.get_text().strip()
                current_hierarchy[level] = text

                # Reset deeper levels
                for i in range(level + 1, 7):
                    if i in current_hierarchy:
                        del current_hierarchy[i]

            # Handle content elements
            elif element.name in ['p', 'div']:
                text = element.get_text().strip()
                if text and len(text) > 10:  # Skip tiny content
                    current_section_content.append(text)

        # Save last section
        if current_section_content:
            section_text = '\n'.join(current_section_content)
            if section_text.strip():
                sections.append({
                    "section_number": current_hierarchy.get(4, ""),
                    "division": current_hierarchy.get(2, ""),
                    "part": current_hierarchy.get(3, ""),
                    "title": current_hierarchy.get(4, ""),
                    "content": section_text,
                    "extracted_clauses": [],
                    "extracted_definitions": [],
                    "extracted_references": []
                })

        return sections

    def _extract_clauses_from_text(self, text: str) -> Dict[str, Any]:
        """
        Extract clauses using structural patterns (no GPT).

        Args:
            text: Text to extract from

        Returns:
            Extracted clauses and definitions
        """
        clauses = []
        definitions = []
        references = []

        # Pattern for numbered clauses: (1), (2), (3)
        clause_pattern = r'\((\d+)\)\s*([^(\n]+?)(?=\(\d+\)|$)'
        for match in re.finditer(clause_pattern, text, re.DOTALL):
            clause_num = match.group(1)
            clause_text = match.group(2).strip()

            if len(clause_text) > 10:  # Skip tiny clauses
                clause_obj = {
                    "number": f"({clause_num})",
                    "text": clause_text[:500],  # Limit size
                    "type": "clause",
                    "nested_items": []
                }

                # Try to find subclauses: (a), (b), (c)
                subclause_pattern = r'\(([a-z])\)\s*([^(]+?)(?=\([a-z]\)|$)'
                for sub_match in re.finditer(subclause_pattern, clause_text, re.DOTALL):
                    sub_num = sub_match.group(1)
                    sub_text = sub_match.group(2).strip()

                    if len(sub_text) > 5:
                        clause_obj["nested_items"].append({
                            "number": f"({sub_num})",
                            "text": sub_text[:300],
                            "type": "subclause"
                        })

                clauses.append(clause_obj)

        # Simple definition detection: "term" means / is defined as
        definition_pattern = r'["\']([^"\']+)["\']\s+(?:means|is defined as|refers to)\s+([^.]+\.)'
        for match in re.finditer(definition_pattern, text):
            term = match.group(1)
            definition = match.group(2)
            if len(term) < 100:  # Valid term length
                definitions.append({
                    "term": term,
                    "definition": definition.strip()
                })

        # Reference detection: section, part, etc.
        reference_pattern = r'(?:section|part|subsection|clause|table)\s+(\d+(?:\.\d+)*(?:\.\d+)?)'
        for match in re.finditer(reference_pattern, text, re.IGNORECASE):
            ref = match.group(1)
            references.append({
                "reference": ref,
                "context": text[max(0, match.start()-50):min(len(text), match.end()+50)]
            })

        return {
            "clauses": clauses,
            "definitions": definitions,
            "references": references
        }

    def _extract_clauses_with_gpt(self, text: str, section_number: str) -> Dict[str, Any]:
        """
        Extract clauses using GPT for better understanding.

        Args:
            text: Section text
            section_number: Section number for context

        Returns:
            Extracted clauses with GPT understanding
        """
        # First try structural extraction
        structural = self._extract_clauses_from_text(text)

        # If structural got good results, no need for GPT
        if len(structural["clauses"]) > 0 and len(text) < 2000:
            logger.debug(f"Structural extraction found {len(structural['clauses'])} clauses, using those")
            return structural

        # Only use GPT if structural extraction is insufficient
        logger.debug(f"Using GPT for section {section_number}")

        prompt = f"""Extract all numbered items, lettered subclauses, and roman numerals from this regulatory text.

Section: {section_number}

Return ONLY valid JSON (no other text):
{{
    "clauses": [
        {{
            "number": "(1)",
            "text": "full text",
            "type": "clause",
            "nested_items": [
                {{"number": "(a)", "text": "text", "type": "subclause"}}
            ]
        }}
    ],
    "definitions": [
        {{"term": "term", "definition": "definition"}}
    ]
}}

TEXT:
{text[:3000]}
"""

        try:
            response = self.client.messages.create(
                model=self.gpt_model,
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )

            response_text = response.content[0].text

            # Extract JSON
            import json as json_lib
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                parsed = json_lib.loads(json_match.group())
                return {
                    "clauses": parsed.get("clauses", []),
                    "definitions": parsed.get("definitions", []),
                    "references": []
                }

        except Exception as e:
            logger.warning(f"GPT extraction failed, using structural: {e}")

        return structural

    def save_extraction(self, extraction: Dict[str, Any], output_path: str):
        """Save extracted data to JSON"""
        logger.info(f"Saving extraction to {output_path}")

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(extraction, f, indent=2, ensure_ascii=False)

        logger.info(f"Extraction saved: {extraction['total_clauses']} clauses in {extraction['total_sections']} sections")


def main():
    """Extract HTML document"""
    import sys
    from ingestion.shared.config.sources import ELAWS_OBC_HTML_URL

    logging.basicConfig(level=logging.INFO)

    # Extract with GPT (recommended)
    extractor = HTMLExtractor(use_gpt=True)

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
