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
