"""
Stage 2: GPT-Based Content Extraction

Uses OpenAI GPT-4o to intelligently extract:
- Clauses and numbered items
- Subclauses (lettered items)
- Regulatory language and definitions
- Cross-references and related sections
- Semantic tags for classification

This creates fine-grained nodes that can be individually indexed in Neo4j.
"""

import logging
from typing import Dict, List, Any, Optional
import json
import re
from pathlib import Path
import sys
from openai import OpenAI
import time

sys.path.insert(0, str(Path(__file__).parents[4]))

logger = logging.getLogger(__name__)


class GPTContentExtractor:
    """Extract structured content from HTML chunks using GPT-4o"""

    def __init__(self, model: str = "gpt-4o-mini"):
        """
        Initialize GPT extractor.

        Args:
            model: OpenAI model to use
        """
        self.client = OpenAI()
        self.model = model
        self.extraction_cache = {}

    def extract_clauses(self, content: str, section_context: Dict[str, str]) -> Dict[str, Any]:
        """
        Extract clauses, subclauses, and items from content using GPT.

        Args:
            content: Text content to extract from
            section_context: Metadata about the section (number, title, etc.)

        Returns:
            Dictionary with extracted clauses and metadata
        """
        if not content.strip():
            return {"clauses": [], "items": []}

        logger.debug(f"Extracting clauses from content length: {len(content)}")

        extraction_prompt = self._build_extraction_prompt(content, section_context)

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                messages=[
                    {
                        "role": "user",
                        "content": extraction_prompt
                    }
                ]
            )

            # Parse response
            response_text = response.content[0].text
            extracted = self._parse_extraction_response(response_text, section_context)

            return extracted

        except Exception as e:
            logger.error(f"Error in GPT extraction: {e}")
            return {"clauses": [], "items": [], "error": str(e)}

    def extract_definitions(self, content: str) -> List[Dict[str, str]]:
        """
        Extract definitions from content.

        Args:
            content: Text to extract definitions from

        Returns:
            List of definition dictionaries
        """
        logger.debug(f"Extracting definitions from content length: {len(content)}")

        prompt = f"""Extract any definitions from this text. Return a JSON object with a "definitions" array.
Each definition should have:
- "term": the term being defined
- "definition": the definition text

Text:
{content[:2000]}

Return ONLY valid JSON, no other text."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )

            response_text = response.content[0].text
            parsed = json.loads(response_text)
            return parsed.get("definitions", [])

        except Exception as e:
            logger.warning(f"Error extracting definitions: {e}")
            return []

    def extract_references(self, content: str) -> List[Dict[str, str]]:
        """
        Extract cross-references to other sections.

        Args:
            content: Text to search for references

        Returns:
            List of references
        """
        logger.debug(f"Extracting references from content length: {len(content)}")

        # Use regex for initial detection
        reference_pattern = r'(?:section|part|subsection|clause|table|figure|schedule)\s+(\d+(?:\.\d+)*(?:\.\d+)?)'

        references = []
        for match in re.finditer(reference_pattern, content, re.IGNORECASE):
            ref_number = match.group(1)
            references.append({
                "reference": ref_number,
                "context": content[max(0, match.start()-50):min(len(content), match.end()+50)]
            })

        return references

    def _build_extraction_prompt(self, content: str, section_context: Dict[str, str]) -> str:
        """Build prompt for clause extraction"""
        section_info = f"""
Section: {section_context.get('section', 'Unknown')}
Title: {section_context.get('title', 'Unknown')}
"""

        prompt = f"""Extract all numbered items, lettered subclauses, and roman numeral items from this regulatory text.

{section_info}

For each item/clause/subclause found, extract:
1. The number/letter/roman numeral (e.g., "(1)", "(a)", "(i)")
2. The complete text of the item
3. Any nested items under it
4. Keywords/topics (regulation, safety, occupancy, etc.)

Return a JSON object with this structure:
{{
    "clauses": [
        {{
            "number": "(1)",
            "text": "full text here",
            "type": "clause",
            "nested_items": [
                {{
                    "number": "(a)",
                    "text": "text",
                    "type": "subclause"
                }}
            ],
            "keywords": ["keyword1", "keyword2"]
        }}
    ],
    "summary": "brief summary of the section"
}}

IMPORTANT: Return ONLY valid JSON, no other text. Return ALL items found.

Text to extract from:
{content}
"""
        return prompt

    def _parse_extraction_response(self, response_text: str, context: Dict[str, str]) -> Dict[str, Any]:
        """Parse GPT extraction response"""
        try:
            # Try to extract JSON from response
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if not json_match:
                logger.warning("No JSON found in response")
                return {"clauses": [], "items": []}

            parsed = json.loads(json_match.group())

            # Add context metadata
            clauses = parsed.get("clauses", [])
            for clause in clauses:
                clause["section"] = context.get("section", "")
                clause["section_title"] = context.get("title", "")

            return {
                "clauses": clauses,
                "summary": parsed.get("summary", ""),
                "section_context": context
            }

        except json.JSONDecodeError as e:
            logger.warning(f"Error parsing JSON response: {e}")
            return {"clauses": [], "items": []}

    def extract_batch(self, chunks: List[Dict[str, Any]], rate_limit_delay: float = 1.0) -> List[Dict[str, Any]]:
        """
        Extract from multiple chunks with rate limiting.

        Args:
            chunks: List of chunk dictionaries from Stage 1
            rate_limit_delay: Delay between API calls in seconds

        Returns:
            List of extracted documents
        """
        logger.info(f"Processing {len(chunks)} chunks with GPT extraction")

        extracted_docs = []

        for idx, chunk in enumerate(chunks):
            logger.info(f"Processing chunk {idx+1}/{len(chunks)}")

            try:
                # Extract content and metadata
                content = chunk.get("chunks", [])
                metadata = chunk.get("metadata", {})
                title = chunk.get("title", "")

                extracted_chunks = []

                for chunk_item in content:
                    chunk_content = chunk_item.get("content", "")
                    chunk_metadata = chunk_item.get("metadata", {})

                    # Extract clauses
                    section_context = {
                        "section": chunk_metadata.get("section", ""),
                        "title": chunk_metadata.get("title", title),
                        "division": chunk_metadata.get("division", ""),
                        "part": chunk_metadata.get("part", ""),
                    }

                    extracted = self.extract_clauses(chunk_content, section_context)

                    # Extract definitions and references
                    definitions = self.extract_definitions(chunk_content)
                    references = self.extract_references(chunk_content)

                    extracted_chunks.append({
                        "original_content": chunk_content,
                        "content_metadata": chunk_metadata,
                        "extracted": {
                            "clauses": extracted.get("clauses", []),
                            "definitions": definitions,
                            "references": references,
                            "summary": extracted.get("summary", "")
                        }
                    })

                    # Rate limiting
                    time.sleep(rate_limit_delay)

                extracted_docs.append({
                    "source": chunk.get("source", ""),
                    "title": title,
                    "extracted_chunks": extracted_chunks,
                    "total_items": sum(
                        1 + len(c.get("extracted", {}).get("clauses", []))
                        for c in extracted_chunks
                    ),
                    "metadata": metadata
                })

            except Exception as e:
                logger.error(f"Error processing chunk {idx}: {e}")
                continue

        logger.info(f"Extracted {len(extracted_docs)} documents")

        return extracted_docs

    def save_extracted(self, extracted: List[Dict[str, Any]], output_path: str):
        """Save extracted content to JSON"""
        logger.info(f"Saving extracted content to {output_path}")

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(extracted, f, indent=2, ensure_ascii=False)

        logger.info(f"Extraction saved to {output_path}")


def main():
    """Run GPT-based extraction"""
    logging.basicConfig(level=logging.INFO)

    # Load chunks from Stage 1
    chunks_path = "data/html_chunks.json"

    if not Path(chunks_path).exists():
        logger.error(f"Chunks file not found: {chunks_path}")
        return

    with open(chunks_path, "r", encoding="utf-8") as f:
        chunks_data = json.load(f)

    chunks = chunks_data.get("documents", [])

    # Extract using GPT
    extractor = GPTContentExtractor()
    extracted = extractor.extract_batch(chunks)

    # Save results
    extractor.save_extracted(extracted, "data/gpt_extracted.json")

    logger.info("GPT extraction complete")


if __name__ == "__main__":
    main()
