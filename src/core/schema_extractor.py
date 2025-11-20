import json
from typing import List, Dict, Any, Optional
from openai import OpenAI
from src.core.schema import Schema, NodeDef, RelDef
import logging

logger = logging.getLogger(__name__)


class SchemaExtractor:
    """
    Extracts entities and relationships from text based on a defined schema.
    Uses LLM to understand documents and map them to schema.
    """

    def __init__(self, schema: Schema, openai_api_key: Optional[str] = None):
        self.schema = schema
        self.client = OpenAI(api_key=openai_api_key)
        self.model = "gpt-4o-mini"  # Fast and cost-effective

    def extract_from_text(self, text: str) -> Dict[str, Any]:
        """
        Extract entities and relationships from text based on schema.

        Returns:
            {
                "nodes": [
                    {"label": "Organization", "properties": {"name": "Acme", "role": "Vendor"}},
                    ...
                ],
                "relationships": [
                    {"type": "PARTY_TO", "source_id": "org_1", "target_id": "agreement_1", "properties": {}},
                    ...
                ]
            }
        """
        # Build schema description for LLM
        schema_prompt = self._build_schema_prompt()

        # Create extraction prompt
        extraction_prompt = f"""
You are extracting structured data from a legal document.

Use this schema to identify entities and relationships:

{schema_prompt}

Document text:
{text}

TASK: Extract ALL entities and relationships from the text that match the schema.

For NODES: Identify all instances of each entity type mentioned.
For RELATIONSHIPS: Identify connections between entities based on:
  - Explicit mentions (e.g., "Organization X is party to Agreement Y")
  - Implicit relationships from context (e.g., an Organization mentioned in a section with an Agreement)
  - Hierarchical structure (e.g., a Clause belongs to an Agreement, Obligations are part of Clauses)

Respond with valid JSON in this format (example):
{{
    "nodes": [
        {{"id": "org_1", "label": "Organization", "properties": {{"name": "Acme Corp", "type": "Client"}}}},
        {{"id": "agreement_1", "label": "Agreement", "properties": {{"title": "Service Agreement"}}}}
    ],
    "relationships": [
        {{"source_id": "org_1", "target_id": "agreement_1", "type": "PARTY_TO", "properties": {{}}}}
    ]
}}

CRITICAL INSTRUCTIONS:
- Extract ALL entities found in the text (no filtering)
- For every pair of entities where a relationship type exists in the schema, check if they are related
- Do NOT skip relationships just because they're not explicitly stated - infer from context
- Use schema labels and types EXACTLY as defined
- Generate unique, consistent IDs (e.g., "org_1", "org_2", "agreement_1", "clause_1")
- For nodes: Always include an "id" field (e.g., "org_1", "org_2")
- For relationships: Use EXACTLY "source_id" and "target_id" (not "from"/"to"), pointing to node IDs
- Include all relevant properties for each entity
- Return ONLY valid JSON, no markdown, no code blocks, no other text
"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": extraction_prompt
                    }
                ],
                temperature=0.1,  # Low temperature for consistency
            )

            result_text = response.choices[0].message.content

            # Clean up markdown code blocks if present
            result_text = result_text.strip()
            if result_text.startswith("```"):
                # Remove markdown code block
                result_text = result_text.split("```")[1]
                if result_text.startswith("json"):
                    result_text = result_text[4:]
                result_text = result_text.strip()

            # Parse JSON response
            extracted = json.loads(result_text)

            # Normalize the extraction format
            normalized = self._normalize_extraction(extracted)

            # Validate extracted data against schema
            validated = self._validate_extraction(normalized)

            return validated

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            return {"nodes": [], "relationships": []}
        except Exception as e:
            logger.error(f"Error during extraction: {e}")
            return {"nodes": [], "relationships": []}

    def extract_from_chunks(self, chunks: List[str]) -> Dict[str, Any]:
        """
        Extract from multiple text chunks and merge results.
        Deduplicates nodes by label and primary identifier across chunks.

        Args:
            chunks: List of text chunks to process

        Returns:
            Merged extraction with deduplicated nodes and relationships
        """
        all_nodes = []
        all_relationships = []
        node_key_to_id = {}  # Maps node_key (label:name) to canonical ID
        chunk_id_map = {}    # Maps extraction_id to canonical ID within each chunk

        for chunk_idx, chunk_text in enumerate(chunks):
            result = self.extract_from_text(chunk_text)
            chunk_id_map.clear()  # Reset per chunk

            # Process nodes: deduplicate by label + primary identifier
            for node in result.get("nodes", []):
                label = node["label"]
                props = node["properties"]
                extraction_id = node.get("id")

                # Create dedup key based on label and primary identifier
                node_key = self._create_node_key(label, props)

                if node_key not in node_key_to_id:
                    # New node - assign canonical ID and add to results
                    canonical_id = f"{label.lower()}_{len([n for n in all_nodes if n['label'] == label])}"
                    node_key_to_id[node_key] = canonical_id
                    node["id"] = canonical_id
                    all_nodes.append(node)
                    logger.debug(f"New node in chunk {chunk_idx}: {canonical_id}")
                else:
                    # Duplicate node - use canonical ID
                    canonical_id = node_key_to_id[node_key]
                    logger.debug(f"Deduplicated node in chunk {chunk_idx}: {extraction_id} -> {canonical_id}")

                # Track mapping from extraction ID to canonical ID
                chunk_id_map[extraction_id] = canonical_id

            # Process relationships: remap IDs to canonical IDs
            for rel in result.get("relationships", []):
                source_extraction_id = rel.get("source_id")
                target_extraction_id = rel.get("target_id")

                # Remap to canonical IDs
                if source_extraction_id in chunk_id_map:
                    rel["source_id"] = chunk_id_map[source_extraction_id]
                if target_extraction_id in chunk_id_map:
                    rel["target_id"] = chunk_id_map[target_extraction_id]

                all_relationships.append(rel)

        logger.info(f"Extracted {len(all_nodes)} unique nodes and {len(all_relationships)} relationships from {len(chunks)} chunks")
        return {
            "nodes": all_nodes,
            "relationships": all_relationships
        }

    def _build_schema_prompt(self) -> str:
        """Generate schema description for LLM prompt"""
        lines = []

        lines.append("NODE TYPES:")
        for label, node_def in self.schema.nodes.items():
            lines.append(f"  {label}")
            for prop in node_def.properties:
                req = " (required)" if prop.required else ""
                lines.append(f"    - {prop.name}: {prop.type}{req}")

        lines.append("\nRELATIONSHIP TYPES:")
        for rel_def in self.schema.relationships:
            lines.append(f"  {rel_def.source_label} -[{rel_def.type}]-> {rel_def.target_label}")

        return "\n".join(lines)

    def _normalize_extraction(self, extracted: Dict) -> Dict:
        """
        Normalize extraction to standard format.
        Handles variations in how LLM might format nodes and relationships.
        """
        normalized_nodes = []
        normalized_rels = []
        node_counter = {}  # Track node count per type for ID generation
        entity_to_id = {}  # Map entities to generated IDs

        # Normalize nodes
        for idx, node in enumerate(extracted.get("nodes", [])):
            # Get label - might be direct or inferred from node structure
            label = node.get("label")
            properties = node.get("properties", {})

            if not label:
                # Try to infer label from node structure
                # Check which schema node type best matches this entity
                for schema_label, node_def in self.schema.nodes.items():
                    # Simple heuristic: check if any required property exists
                    if any(prop.name in node for prop in node_def.properties):
                        label = schema_label
                        break

            if not label:
                continue

            # Get or generate ID
            node_id = node.get("id")
            if not node_id:
                # Generate ID based on label
                if label not in node_counter:
                    node_counter[label] = 0
                node_counter[label] += 1
                node_id = f"{label.lower()}_{node_counter[label]}"

            # Extract properties - if node uses flat structure, normalize it
            if not properties:
                # Extract properties from node itself (excluding 'label', 'id', 'type')
                properties = {
                    k: v for k, v in node.items()
                    if k not in ['label', 'id', 'type'] and v is not None
                }

            # Generate entity key for relationship remapping
            entity_key = None
            for prop_def in self.schema.nodes.get(label, NodeDef("", [])).properties:
                if prop_def.name in properties:
                    entity_key = properties[prop_def.name]
                    break

            if entity_key:
                entity_to_id[f"{label}:{entity_key}"] = node_id

            normalized_nodes.append({
                "id": node_id,
                "label": label,
                "properties": properties
            })

        # Normalize relationships
        for rel in extracted.get("relationships", []):
            rel_type = rel.get("type")
            source_id = rel.get("source_id") or rel.get("from")
            target_id = rel.get("target_id") or rel.get("to")
            properties = rel.get("properties", {})

            # Try to remap named entities to IDs if needed
            if isinstance(source_id, str) and ":" not in source_id:
                # source_id might be an entity name, try to find matching ID
                for entity_key, eid in entity_to_id.items():
                    if source_id.lower() in entity_key.lower():
                        source_id = eid
                        break

            if isinstance(target_id, str) and ":" not in target_id:
                # target_id might be an entity name, try to find matching ID
                for entity_key, eid in entity_to_id.items():
                    if target_id.lower() in entity_key.lower():
                        target_id = eid
                        break

            if source_id and target_id:
                normalized_rels.append({
                    "type": rel_type,
                    "source_id": source_id,
                    "target_id": target_id,
                    "properties": properties
                })

        return {
            "nodes": normalized_nodes,
            "relationships": normalized_rels
        }

    def _validate_extraction(self, extracted: Dict) -> Dict:
        """Validate extracted data against schema"""
        validated_nodes = []
        validated_rels = []
        node_ids = set()
        node_id_mapping = {}  # Maps alternative IDs to canonical IDs

        # Validate nodes
        for node in extracted.get("nodes", []):
            label = node.get("label")
            properties = node.get("properties", {})
            node_id = node.get("id")

            # Check if node type exists in schema
            if label not in self.schema.nodes:
                continue

            node_ids.add(node_id)
            validated_nodes.append({
                "id": node_id,
                "label": label,
                "properties": properties
            })

        # Validate relationships with format flexibility
        for rel in extracted.get("relationships", []):
            rel_type = rel.get("type")
            # Handle both "source_id"/"target_id" and "from"/"to" formats
            source_id = rel.get("source_id") or rel.get("from")
            target_id = rel.get("target_id") or rel.get("to")

            # Check if relationship type exists in schema
            rel_exists = any(
                r.type == rel_type for r in self.schema.relationships
            )

            if not rel_exists:
                continue

            # Check if both nodes exist (by ID)
            if source_id not in node_ids or target_id not in node_ids:
                continue

            validated_rels.append({
                "type": rel_type,
                "source_id": source_id,
                "target_id": target_id,
                "properties": rel.get("properties", {})
            })

        return {
            "nodes": validated_nodes,
            "relationships": validated_rels
        }

    def _create_node_key(self, label: str, properties: Dict) -> str:
        """Create unique key for deduplication based on node identity properties"""
        # For most entities, use label + primary identifier
        # This is a simple heuristic - customize for your domain
        primary_prop = properties.get("name") or properties.get("title") or str(properties)
        return f"{label}:{primary_prop}"
