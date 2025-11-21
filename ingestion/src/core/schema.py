from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class PropertyDef:
    """Property definition for a node or relationship"""
    name: str
    type: str  # "string", "int", "float", "bool", "list", "date"
    required: bool = False


@dataclass
class NodeDef:
    """Node definition in schema"""
    label: str
    properties: List[PropertyDef]
    description: str = ""

    def validate(self, node_data: Dict) -> Tuple[bool, List[str]]:
        """Validate node against schema"""
        errors = []
        for prop in self.properties:
            if prop.required and prop.name not in node_data:
                errors.append(f"Missing required property: {prop.name}")
        return len(errors) == 0, errors


@dataclass
class RelDef:
    """Relationship definition in schema"""
    type: str
    source_label: str
    target_label: str
    properties: List[PropertyDef] = None
    description: str = ""

    def __post_init__(self):
        if self.properties is None:
            self.properties = []


class Schema:
    """Graph schema - defines node types and relationships"""

    def __init__(self, name: str):
        self.name = name
        self.nodes: Dict[str, NodeDef] = {}
        self.relationships: List[RelDef] = []

    def add_node(self, node_def: NodeDef) -> None:
        """Add a node type to schema"""
        self.nodes[node_def.label] = node_def

    def add_relationship(self, rel_def: RelDef) -> None:
        """Add a relationship type to schema"""
        self.relationships.append(rel_def)

    def get_node(self, label: str) -> Optional[NodeDef]:
        """Get node definition by label"""
        return self.nodes.get(label)

    def to_dict(self) -> Dict:
        """Export schema as dictionary"""
        return {
            "name": self.name,
            "nodes": {
                label: {
                    "properties": [asdict(p) for p in node.properties],
                    "description": node.description
                }
                for label, node in self.nodes.items()
            },
            "relationships": [
                {
                    "type": r.type,
                    "source": r.source_label,
                    "target": r.target_label,
                    "properties": [asdict(p) for p in r.properties]
                }
                for r in self.relationships
            ]
        }

    def print_schema(self) -> None:
        """Log schema summary"""
        logger.info(f"Schema: {self.name}")
        logger.info(f"NODE TYPES ({len(self.nodes)}):")
        for label, node in self.nodes.items():
            logger.info(f"  {label}")
            for prop in node.properties:
                req = " [REQUIRED]" if prop.required else ""
                logger.info(f"    - {prop.name}: {prop.type}{req}")

        logger.info(f"RELATIONSHIPS ({len(self.relationships)}):")
        for rel in self.relationships:
            logger.info(f"  {rel.source_label} -[{rel.type}]-> {rel.target_label}")


# Example: Simple legal document schema
def create_legal_schema() -> Schema:
    """Simple legal document schema"""
    schema = Schema("Legal Document Schema")

    # Node types
    schema.add_node(NodeDef(
        label="Organization",
        properties=[
            PropertyDef("name", "string", required=True),
            PropertyDef("role", "string"),
        ],
        description="Legal entity"
    ))

    schema.add_node(NodeDef(
        label="Agreement",
        properties=[
            PropertyDef("type", "string", required=True),
            PropertyDef("title", "string", required=True),
            PropertyDef("effective_date", "date"),
        ],
        description="Contract or agreement"
    ))

    schema.add_node(NodeDef(
        label="Clause",
        properties=[
            PropertyDef("number", "string", required=True),
            PropertyDef("title", "string"),
            PropertyDef("text", "string", required=True),
        ],
        description="Clause in agreement"
    ))

    schema.add_node(NodeDef(
        label="Obligation",
        properties=[
            PropertyDef("description", "string", required=True),
            PropertyDef("obligated_party", "string"),
            PropertyDef("due_date", "date"),
        ],
        description="Obligation or requirement"
    ))

    schema.add_node(NodeDef(
        label="Payment",
        properties=[
            PropertyDef("amount", "float", required=True),
            PropertyDef("currency", "string"),
            PropertyDef("due_date", "date"),
        ],
        description="Payment term"
    ))

    # Relationship types
    schema.add_relationship(RelDef(
        type="PARTY_TO",
        source_label="Organization",
        target_label="Agreement"
    ))

    schema.add_relationship(RelDef(
        type="HAS_CLAUSE",
        source_label="Agreement",
        target_label="Clause",
        properties=[PropertyDef("sequence", "int")]
    ))

    schema.add_relationship(RelDef(
        type="SPECIFIES",
        source_label="Clause",
        target_label="Obligation"
    ))

    schema.add_relationship(RelDef(
        type="REQUIRES_PAYMENT",
        source_label="Clause",
        target_label="Payment"
    ))

    schema.add_relationship(RelDef(
        type="OBLIGATED_TO",
        source_label="Organization",
        target_label="Obligation"
    ))

    return schema


def create_elaws_obc_schema() -> Schema:
    """
    Comprehensive schema for E-Laws O. Reg. 332/12 (Ontario Building Code).

    This schema covers:
    - Full hierarchical structure: Regulation → Division → Part → Section → Clause → SubClause → Item
    - Definitions section
    - Tables (flattened from E-Laws format)
    - Cross-references and citations
    - Vector embeddings for RAG
    - Semantic tagging
    """
    schema = Schema("E-Laws OBC Schema")

    # ========== NODE TYPES ==========

    # A. Root-level
    schema.add_node(NodeDef(
        label="Regulation",
        properties=[
            PropertyDef("regulation_id", "string", required=True),
            PropertyDef("title", "string", required=True),
            PropertyDef("abbreviation", "string"),
            PropertyDef("last_amended", "date"),
            PropertyDef("source_url", "string"),
        ],
        description="Root regulation node: O. Reg. 332/12 (Building Code)"
    ))

    # B. Document Structure Hierarchy
    schema.add_node(NodeDef(
        label="Division",
        properties=[
            PropertyDef("division_id", "string", required=True),
            PropertyDef("title", "string", required=True),
            PropertyDef("section_range", "string"),
        ],
        description="Division level (e.g., Division A)"
    ))

    schema.add_node(NodeDef(
        label="Part",
        properties=[
            PropertyDef("part_number", "string", required=True),
            PropertyDef("title", "string", required=True),
            PropertyDef("sequence", "int"),
        ],
        description="Part level (e.g., Part 3 - Fire Protection)"
    ))

    schema.add_node(NodeDef(
        label="Section",
        properties=[
            PropertyDef("section_number", "string", required=True),
            PropertyDef("title", "string"),
            PropertyDef("sequence", "int"),
        ],
        description="Section level (e.g., 3.2.2)"
    ))

    schema.add_node(NodeDef(
        label="Clause",
        properties=[
            PropertyDef("clause_number", "string", required=True),
            PropertyDef("text", "string", required=True),
            PropertyDef("sequence", "int"),
        ],
        description="Clause (sentence) level - the actual enforceable rule (e.g., 3.2.2.45.(1))"
    ))

    schema.add_node(NodeDef(
        label="SubClause",
        properties=[
            PropertyDef("subclause_id", "string", required=True),
            PropertyDef("text", "string", required=True),
            PropertyDef("sequence", "int"),
        ],
        description="Lettered sub-items within a clause (e.g., (a), (b), (c))"
    ))

    schema.add_node(NodeDef(
        label="Item",
        properties=[
            PropertyDef("item_id", "string", required=True),
            PropertyDef("text", "string", required=True),
            PropertyDef("sequence", "int"),
        ],
        description="Roman numeral items within sub-clauses (e.g., (i), (ii), (iii))"
    ))

    # C. Definitions
    schema.add_node(NodeDef(
        label="Definition",
        properties=[
            PropertyDef("term", "string", required=True),
            PropertyDef("definition", "string", required=True),
            PropertyDef("source_section", "string"),
            PropertyDef("alternative_terms", "list"),
        ],
        description="Definition node for terms used in the code"
    ))

    # D. Tables
    schema.add_node(NodeDef(
        label="Table",
        properties=[
            PropertyDef("table_number", "string", required=True),
            PropertyDef("title", "string"),
            PropertyDef("raw_text", "string", required=True),
            PropertyDef("section_reference", "string"),
        ],
        description="Table explicitly tagged in E-Laws content"
    ))

    # E. Semantic Nodes
    schema.add_node(NodeDef(
        label="Reference",
        properties=[
            PropertyDef("target", "string", required=True),
            PropertyDef("text", "string", required=True),
            PropertyDef("type", "string"),  # "internal" or "external"
        ],
        description="Cross-reference or hyperlink from E-Laws"
    ))

    schema.add_node(NodeDef(
        label="Embedding",
        properties=[
            PropertyDef("vector", "list", required=True),
            PropertyDef("source_type", "string", required=True),  # e.g., "Clause", "Section"
            PropertyDef("source_id", "string", required=True),
            PropertyDef("model", "string"),
        ],
        description="Vector embedding for RAG semantic search"
    ))

    schema.add_node(NodeDef(
        label="Topic",
        properties=[
            PropertyDef("name", "string", required=True),
            PropertyDef("description", "string"),
        ],
        description="Semantic tag for organizing content (e.g., 'Fire Safety', 'Energy Efficiency')"
    ))

    # ========== RELATIONSHIP TYPES ==========

    # A. Structural Hierarchy
    schema.add_relationship(RelDef(
        type="HAS_DIVISION",
        source_label="Regulation",
        target_label="Division",
        properties=[PropertyDef("sequence", "int")],
        description="Regulation contains Divisions"
    ))

    schema.add_relationship(RelDef(
        type="HAS_PART",
        source_label="Division",
        target_label="Part",
        properties=[PropertyDef("sequence", "int")],
        description="Division contains Parts"
    ))

    schema.add_relationship(RelDef(
        type="HAS_SECTION",
        source_label="Part",
        target_label="Section",
        properties=[PropertyDef("sequence", "int")],
        description="Part contains Sections"
    ))

    schema.add_relationship(RelDef(
        type="HAS_CLAUSE",
        source_label="Section",
        target_label="Clause",
        properties=[PropertyDef("sequence", "int")],
        description="Section contains Clauses"
    ))

    schema.add_relationship(RelDef(
        type="HAS_SUBCLAUSE",
        source_label="Clause",
        target_label="SubClause",
        properties=[PropertyDef("sequence", "int")],
        description="Clause contains SubClauses"
    ))

    schema.add_relationship(RelDef(
        type="HAS_ITEM",
        source_label="SubClause",
        target_label="Item",
        properties=[PropertyDef("sequence", "int")],
        description="SubClause contains Items"
    ))

    # B. Definition Relationships
    schema.add_relationship(RelDef(
        type="DEFINED_IN",
        source_label="Definition",
        target_label="Section",
        description="Definition is defined in a Section"
    ))

    schema.add_relationship(RelDef(
        type="USES_TERM",
        source_label="Clause",
        target_label="Definition",
        description="Clause uses a defined term"
    ))

    # C. Table Relationships
    schema.add_relationship(RelDef(
        type="HAS_TABLE",
        source_label="Section",
        target_label="Table",
        properties=[PropertyDef("sequence", "int")],
        description="Section contains a Table"
    ))

    schema.add_relationship(RelDef(
        type="REFERENCED_BY_CLAUSE",
        source_label="Table",
        target_label="Clause",
        description="Table is referenced by a Clause"
    ))

    # D. Cross-References (Hyperlinks)
    schema.add_relationship(RelDef(
        type="CITES",
        source_label="Clause",
        target_label="Clause",
        description="Clause cites another Clause"
    ))

    schema.add_relationship(RelDef(
        type="CITES",
        source_label="SubClause",
        target_label="Clause",
        description="SubClause cites a Clause"
    ))

    schema.add_relationship(RelDef(
        type="CONTAINS_REFERENCE",
        source_label="Clause",
        target_label="Reference",
        description="Clause contains a cross-reference"
    ))

    # E. Embedding Relationships
    schema.add_relationship(RelDef(
        type="HAS_EMBEDDING",
        source_label="Clause",
        target_label="Embedding",
        description="Clause has a vector embedding"
    ))

    schema.add_relationship(RelDef(
        type="HAS_EMBEDDING",
        source_label="Section",
        target_label="Embedding",
        description="Section has a vector embedding"
    ))

    schema.add_relationship(RelDef(
        type="HAS_EMBEDDING",
        source_label="Definition",
        target_label="Embedding",
        description="Definition has a vector embedding"
    ))

    # F. Semantic Tagging
    schema.add_relationship(RelDef(
        type="BELONGS_TO_TOPIC",
        source_label="Clause",
        target_label="Topic",
        description="Clause belongs to a semantic topic"
    ))

    schema.add_relationship(RelDef(
        type="BELONGS_TO_TOPIC",
        source_label="Section",
        target_label="Topic",
        description="Section belongs to a semantic topic"
    ))

    return schema
