"""
Stage 2: Semantic enrichment with OpenAI Vision API
Takes extracted structure + PDF pages, enriches with semantic understanding
"""

import base64
import logging
import json
from typing import Dict, List, Any
from pathlib import Path
import os
import pdf2image
from PIL import Image
import io
import time

logger = logging.getLogger(__name__)


class Stage2Enrichment:
    """Enrich extracted data with OpenAI Vision API"""

    def __init__(self, api_key: str, model: str = "gpt-4o"):
        self.api_key = api_key
        self.model = model
        self.client = self._init_client()

    def _init_client(self):
        """Initialize OpenAI client"""
        try:
            from openai import OpenAI
            return OpenAI(api_key=self.api_key)
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {e}")
            return None

    def enrich(self, pdf_path: str, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich extracted data with semantic understanding from Vision API

        Args:
            pdf_path: Path to original PDF
            extracted_data: Output from Stage1Extractor.extract()

        Returns:
            Enriched data with semantic annotations
        """
        logger.info("Stage 2: Starting semantic enrichment with OpenAI Vision")

        if not self.client:
            logger.error("OpenAI client not initialized")
            return extracted_data

        try:
            # Convert PDF to images
            pdf_images = self._pdf_to_images(pdf_path)
            logger.info(f"Converted PDF to {len(pdf_images)} page images")

            # Enrich sections with visual context
            enriched_sections = self._enrich_sections(
                extracted_data['sections'],
                pdf_images,
                extracted_data.get('references', [])
            )

            # Enrich tables with semantic meaning
            enriched_tables = self._enrich_tables(
                extracted_data['tables'],
                pdf_images
            )

            # Generate image descriptions
            image_descriptions = self._describe_images(
                extracted_data['images'],
                pdf_images
            )

            enriched_data = {
                'sections': enriched_sections,
                'tables': enriched_tables,
                'images': image_descriptions,
                'references': extracted_data.get('references', []),
                'metadata': {
                    **extracted_data.get('metadata', {}),
                    'enrichment_model': self.model,
                    'enriched': True
                }
            }

            logger.info("Enrichment complete")
            return enriched_data

        except Exception as e:
            logger.error(f"Enrichment failed: {e}")
            return extracted_data

    def _pdf_to_images(self, pdf_path: str) -> List[Image.Image]:
        """Convert PDF pages to images"""
        try:
            images = pdf2image.convert_from_path(pdf_path, dpi=150)
            return images
        except Exception as e:
            logger.error(f"Failed to convert PDF to images: {e}")
            return []

    def _image_to_base64(self, image: Image.Image) -> str:
        """Convert PIL Image to base64 for API"""
        try:
            buffered = io.BytesIO()
            image.save(buffered, format="PNG")
            return base64.b64encode(buffered.getvalue()).decode()
        except Exception as e:
            logger.error(f"Failed to encode image: {e}")
            return ""

    def _enrich_sections(
        self,
        sections: List[Dict],
        pdf_images: List[Image.Image],
        references: List[Dict]
    ) -> List[Dict]:
        """Enrich sections with semantic tags and context"""
        enriched = []

        for section in sections:
            page_num = section['page']

            if page_num > len(pdf_images):
                enriched.append({**section, 'semantic_tags': [], 'relationships': []})
                continue

            # Get the page image for context
            page_image = pdf_images[page_num - 1]
            image_b64 = self._image_to_base64(page_image)

            try:
                # Ask Claude to understand this section's semantic meaning
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:image/png;base64,{image_b64}"
                                    }
                                },
                                {
                                    "type": "text",
                                    "text": f"""Analyze this section from the Ontario Building Code:

Section {section['number']}: {section['title']}

Content: {section['content'][:500]}

Based on the visual context and content, identify:
1. semantic_type: Is this a requirement, definition, guideline, note, or reference?
2. related_sections: What other sections does this relate to?
3. key_concepts: What are the main building code concepts?
4. compliance_focus: What does the builder/designer need to comply with?

Return as JSON:
{{
    "semantic_type": "requirement|definition|guideline|note|reference",
    "related_sections": ["6.1.1", "6.2"],
    "key_concepts": ["structural", "fire safety"],
    "compliance_focus": "description"
}}"""
                                }
                            ]
                        }
                    ],
                    max_tokens=500
                )

                # Parse response
                response_text = response.choices[0].message.content
                try:
                    semantic_info = json.loads(response_text)
                except:
                    # Try to extract JSON from response
                    import re
                    json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                    semantic_info = json.loads(json_match.group(0)) if json_match else {}

                enriched.append({
                    **section,
                    'semantic_type': semantic_info.get('semantic_type', 'unknown'),
                    'related_sections': semantic_info.get('related_sections', []),
                    'key_concepts': semantic_info.get('key_concepts', []),
                    'compliance_focus': semantic_info.get('compliance_focus', '')
                })

                # Rate limit to avoid hitting API limits
                time.sleep(0.5)

            except Exception as e:
                logger.warning(f"Failed to enrich section {section['number']}: {e}")
                enriched.append({**section, 'semantic_tags': [], 'relationships': []})

        logger.info(f"Enriched {len(enriched)} sections")
        return enriched

    def _enrich_tables(
        self,
        tables: List[Dict],
        pdf_images: List[Image.Image]
    ) -> List[Dict]:
        """Enrich tables with semantic understanding"""
        enriched = []

        for table in tables:
            page_num = table['page']

            if page_num > len(pdf_images):
                enriched.append({**table, 'semantic_meaning': '', 'row_interpretations': []})
                continue

            page_image = pdf_images[page_num - 1]
            image_b64 = self._image_to_base64(page_image)

            try:
                # Ask Claude about table meaning
                table_summary = str(table['rows'][:5])  # First 5 rows for context

                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:image/png;base64,{image_b64}"
                                    }
                                },
                                {
                                    "type": "text",
                                    "text": f"""Analyze this table from the Ontario Building Code (page {page_num}):

Table: {table['name']}
Headers: {', '.join(table['headers'])}
Sample rows: {table_summary}

Explain:
1. What does this table specify?
2. How would a builder/designer use this table?
3. What are the key decisions or classifications?

Keep response concise (2-3 sentences)."""
                                }
                            ]
                        }
                    ],
                    max_tokens=300
                )

                meaning = response.choices[0].message.content

                enriched.append({
                    **table,
                    'semantic_meaning': meaning
                })

                time.sleep(0.5)

            except Exception as e:
                logger.warning(f"Failed to enrich table {table['name']}: {e}")
                enriched.append({**table, 'semantic_meaning': ''})

        logger.info(f"Enriched {len(enriched)} tables")
        return enriched

    def _describe_images(
        self,
        images: List[Dict],
        pdf_images: List[Image.Image]
    ) -> List[Dict]:
        """Generate descriptions for extracted images"""
        descriptions = []

        for img_ref in images:
            page_num = img_ref['page']

            if page_num > len(pdf_images):
                descriptions.append({**img_ref, 'description': 'Image unavailable'})
                continue

            page_image = pdf_images[page_num - 1]

            # Crop to image bbox if available
            if 'bbox' in img_ref:
                bbox = img_ref['bbox']
                width, height = page_image.size
                # Normalize bbox coordinates
                left = int(bbox['x0'] * width / 72) if bbox['x0'] < 100 else int(bbox['x0'])
                top = int(bbox['y0'] * height / 72) if bbox['y0'] < 100 else int(bbox['y0'])
                right = int(bbox['x1'] * width / 72) if bbox['x1'] < 100 else int(bbox['x1'])
                bottom = int(bbox['y1'] * height / 72) if bbox['y1'] < 100 else int(bbox['y1'])

                try:
                    cropped = page_image.crop((left, top, right, bottom))
                except:
                    cropped = page_image

            else:
                cropped = page_image

            image_b64 = self._image_to_base64(cropped)

            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:image/png;base64,{image_b64}"
                                    }
                                },
                                {
                                    "type": "text",
                                    "text": "Describe this diagram from the Ontario Building Code. What building code concept does it illustrate? Keep description concise (1-2 sentences)."
                                }
                            ]
                        }
                    ],
                    max_tokens=200
                )

                description = response.choices[0].message.content

                descriptions.append({
                    **img_ref,
                    'description': description
                })

                time.sleep(0.5)

            except Exception as e:
                logger.warning(f"Failed to describe image on page {page_num}: {e}")
                descriptions.append({**img_ref, 'description': f'Image on page {page_num}'})

        logger.info(f"Generated {len(descriptions)} image descriptions")
        return descriptions
