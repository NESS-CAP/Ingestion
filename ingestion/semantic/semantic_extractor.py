"""
LLM-based semantic extraction from OBC nodes.

Analyzes Sentence and Article nodes to extract:
- Topics (FireSafety, Stairs, Egress, etc.)
- Building Types (Residential, Industrial, etc.)
- Occupancy Classes (A, B, C, D, E, F)
- Construction Types (Combustible, NonCombustible, etc.)
- Space Types (Basement, Bathroom, Stairwell, etc.)
"""

import json
import time
import requests
from typing import Dict, List, Any, Optional

from .semantic_schema import (
    get_all_topic_names,
    get_all_building_type_names,
    get_all_occupancy_codes,
    get_all_construction_type_names,
    get_all_space_type_names,
)


class SemanticExtractor:
    """Extracts semantic concepts from OBC text using LLM."""

    def __init__(self, provider: str = "groq", api_key: str = "", model: str = None):
        """
        Initialize the extractor.

        Args:
            provider: LLM provider ("groq", "openai", "anthropic")
            api_key: API key for the provider
            model: Optional model override
        """
        self.provider = provider.lower()
        self.api_key = api_key
        self.model = model or self._get_default_model()

        # Rate limiting — Groq free tier: 30 req/min = 1 req/2s
        self.last_request_time = 0
        self.min_request_interval = 0.1  # seconds between requests
        self.max_retries = 3
        self.retry_delay = 10.0  # seconds to wait after a 429

    def _get_default_model(self) -> str:
        """Get default model for the provider."""
        defaults = {
            "groq": "llama-3.1-8b-instant",
            "openai": "gpt-4o-mini",
            "anthropic": "claude-3-haiku-20240307",
        }
        return defaults.get(self.provider, "llama-3.1-8b-instant")

    def _build_extraction_prompt(self, text: str, ref: str) -> str:
        """Build the prompt for semantic extraction."""
        topics = ", ".join(get_all_topic_names())
        building_types = ", ".join(get_all_building_type_names())
        occupancy_codes = ", ".join(get_all_occupancy_codes())
        construction_types = ", ".join(get_all_construction_type_names())
        space_types = ", ".join(get_all_space_type_names())

        return f"""Analyze this Ontario Building Code text and extract relevant semantic categories.

TEXT (Reference: {ref}):
"{text}"

CATEGORIES TO EXTRACT:

1. Topics - Regulatory subjects this text relates to.
   Valid options: {topics}
   Only include topics that are DIRECTLY discussed in the text.

2. Building Types - What type of buildings does this apply to?
   Valid options: {building_types}
   If the text is from Part 9, it likely applies to Residential.
   If general/universal, include all applicable types.

3. Occupancy Classes - OBC occupancy classifications mentioned or implied.
   Valid options: {occupancy_codes}
   Only include if explicitly mentioned or clearly implied.

4. Construction Types - Construction methods this applies to.
   Valid options: {construction_types}
   Only include if the text specifically discusses construction type.

5. Space Types - Specific spaces or rooms this applies to.
   Valid options: {space_types}
   Only include spaces DIRECTLY mentioned or clearly implied.

RULES:
- Only extract categories that are CLEARLY relevant to the text
- Use EXACT names from the valid options lists above
- It's okay to return empty arrays if a category doesn't apply
- Be conservative - only include what's explicitly stated or strongly implied

Return ONLY a valid JSON object with this exact structure:
{{
  "topics": [],
  "building_types": [],
  "occupancy_classes": [],
  "construction_types": [],
  "space_types": []
}}"""

    def _rate_limit(self):
        """Apply rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()

    def _call_groq(self, prompt: str) -> Optional[str]:
        """Call Groq API with retry on 429."""
        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0,
            "max_tokens": 350,
        }
        for attempt in range(self.max_retries):
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=30)
                if response.status_code == 429:
                    wait = self.retry_delay * (attempt + 1)
                    print(f"\n[RATE LIMIT] 429 received, waiting {wait}s...", flush=True)
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response.json()["choices"][0]["message"]["content"]
            except Exception as e:
                if attempt == self.max_retries - 1:
                    print(f"[ERROR] Groq API call failed: {e}", flush=True)
                return None
        return None

    def _call_openai(self, prompt: str) -> Optional[str]:
        """Call OpenAI API with retry on 429."""
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0,
            "max_tokens": 350,
        }
        for attempt in range(self.max_retries):
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=30)
                if response.status_code == 429:
                    wait = self.retry_delay * (attempt + 1)
                    print(f"\n[RATE LIMIT] 429 received, waiting {wait}s...", flush=True)
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response.json()["choices"][0]["message"]["content"]
            except Exception as e:
                if attempt == self.max_retries - 1:
                    print(f"[ERROR] OpenAI API call failed: {e}", flush=True)
                return None
        return None

    def _call_anthropic(self, prompt: str) -> Optional[str]:
        """Call Anthropic API."""
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
        }
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0,
            "max_tokens": 500,
        }
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            return response.json()["content"][0]["text"]
        except Exception as e:
            print(f"[ERROR] Anthropic API call failed: {e}", flush=True)
            return None

    def _call_llm(self, prompt: str) -> Optional[str]:
        """Call the configured LLM provider."""
        self._rate_limit()

        if self.provider == "groq":
            return self._call_groq(prompt)
        elif self.provider == "openai":
            return self._call_openai(prompt)
        elif self.provider == "anthropic":
            return self._call_anthropic(prompt)
        else:
            print(f"[ERROR] Unknown provider: {self.provider}", flush=True)
            return None

    def _parse_llm_response(self, response: str) -> Optional[Dict[str, List[str]]]:
        """Parse and validate the LLM response."""
        if not response:
            return None

        # Try to extract JSON from the response
        try:
            # Handle case where LLM wraps JSON in markdown code blocks
            if "```json" in response:
                start = response.find("```json") + 7
                end = response.find("```", start)
                response = response[start:end].strip()
            elif "```" in response:
                start = response.find("```") + 3
                end = response.find("```", start)
                response = response[start:end].strip()
            else:
                # Find the first { and last } to extract bare JSON from mixed text
                start = response.find("{")
                end = response.rfind("}") + 1
                if start != -1 and end > start:
                    response = response[start:end].strip()

            data = json.loads(response)

            # Validate structure
            result = {
                "topics": [],
                "building_types": [],
                "occupancy_classes": [],
                "construction_types": [],
                "space_types": [],
            }

            # Validate and filter each category
            valid_topics = set(get_all_topic_names())
            valid_building_types = set(get_all_building_type_names())
            valid_occupancy_codes = set(get_all_occupancy_codes())
            valid_construction_types = set(get_all_construction_type_names())
            valid_space_types = set(get_all_space_type_names())

            if "topics" in data and isinstance(data["topics"], list):
                result["topics"] = [t for t in data["topics"] if t in valid_topics]

            if "building_types" in data and isinstance(data["building_types"], list):
                result["building_types"] = [bt for bt in data["building_types"] if bt in valid_building_types]

            if "occupancy_classes" in data and isinstance(data["occupancy_classes"], list):
                result["occupancy_classes"] = [oc for oc in data["occupancy_classes"] if oc in valid_occupancy_codes]

            if "construction_types" in data and isinstance(data["construction_types"], list):
                result["construction_types"] = [ct for ct in data["construction_types"] if ct in valid_construction_types]

            if "space_types" in data and isinstance(data["space_types"], list):
                result["space_types"] = [st for st in data["space_types"] if st in valid_space_types]

            return result

        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to parse JSON response: {e}", flush=True)
            print(f"[DEBUG] Raw response: {response[:500]}", flush=True)
            return None

    def extract(self, text: str, ref: str) -> Optional[Dict[str, List[str]]]:
        """
        Extract semantic concepts from OBC text.

        Args:
            text: The OBC text content to analyze
            ref: The reference ID (e.g., "9.8.2.1.(1)")

        Returns:
            Dict with extracted concepts, or None on error
        """
        if not text or not text.strip():
            return None

        prompt = self._build_extraction_prompt(text, ref)
        response = self._call_llm(prompt)
        return self._parse_llm_response(response)

    def extract_batch(
        self,
        nodes: List[Dict[str, str]],
        progress_callback=None,
    ) -> List[Dict[str, Any]]:
        """
        Extract concepts from a batch of nodes.

        Args:
            nodes: List of dicts with "text" and "ref" keys
            progress_callback: Optional callback(current, total) for progress

        Returns:
            List of dicts with "ref" and "concepts" keys
        """
        results = []
        total = len(nodes)

        for i, node in enumerate(nodes):
            text = node.get("text", "")
            ref = node.get("ref", "")

            if progress_callback:
                progress_callback(i + 1, total)

            concepts = self.extract(text, ref)

            results.append({
                "ref": ref,
                "concepts": concepts,
            })

        return results
