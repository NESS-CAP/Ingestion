"""
Centralized schema definitions for all legal document processing.

Contains reusable schema templates for document ingestion and graph building.
"""

from ingestion.src.core.schema import Schema, NodeDef, PropertyDef, RelDef


def create_obc_schema() -> Schema:
    """
    Create schema for Ontario Building Code (OBC) regulations.

    Hierarchy based on HTML class names:
    - Regulation (regnumber-e) - Top level document
    - Division (division classes) - Major divisions
    - Part (partnum-e) - Major sections
    - Section (Ssection-e) - Numbered sections
    - Subsection (Ssubsection-e) - Subsections
    - Clause (Sclause-e) - Numbered items
    - Subclause (clause with letters) - Lettered items
    - Item (roman numerals) - List items

    Returns:
        Schema: Configured OBC schema with hierarchical relationships
    """
    schema = Schema("Ontario Building Code Schema")

    # Node definitions for the hierarchy
    schema.add_node(NodeDef(
        label="Regulation",
        properties=[
            PropertyDef("name", "string", required=True),
            PropertyDef("number", "string", required=True),
            PropertyDef("description", "string"),
        ]
    ))

    schema.add_node(NodeDef(
        label="Division",
        properties=[
            PropertyDef("name", "string", required=True),
            PropertyDef("number", "string"),
            PropertyDef("description", "string"),
        ]
    ))

    schema.add_node(NodeDef(
        label="Part",
        properties=[
            PropertyDef("name", "string", required=True),
            PropertyDef("number", "string", required=True),
            PropertyDef("description", "string"),
        ]
    ))

    schema.add_node(NodeDef(
        label="Section",
        properties=[
            PropertyDef("number", "string", required=True),
            PropertyDef("title", "string"),
            PropertyDef("text", "string"),
        ]
    ))

    schema.add_node(NodeDef(
        label="Subsection",
        properties=[
            PropertyDef("number", "string", required=True),
            PropertyDef("text", "string", required=True),
        ]
    ))

    schema.add_node(NodeDef(
        label="Clause",
        properties=[
            PropertyDef("number", "string", required=True),
            PropertyDef("text", "string", required=True),
        ]
    ))

    schema.add_node(NodeDef(
        label="Subclause",
        properties=[
            PropertyDef("number", "string", required=True),
            PropertyDef("text", "string", required=True),
        ]
    ))

    schema.add_node(NodeDef(
        label="Item",
        properties=[
            PropertyDef("number", "string", required=True),
            PropertyDef("text", "string", required=True),
        ]
    ))

    schema.add_node(NodeDef(
        label="Paragraph",
        properties=[
            PropertyDef("html_class", "string"),
            PropertyDef("text", "string", required=True),
            PropertyDef("index", "int"),
        ]
    ))

    # Hierarchical relationships (Pass 1 - by HTML class name)
    schema.add_relationship(RelDef(
        type="CONTAINS_DIVISION",
        source_label="Regulation",
        target_label="Division"
    ))

    schema.add_relationship(RelDef(
        type="CONTAINS_PART",
        source_label="Division",
        target_label="Part"
    ))

    schema.add_relationship(RelDef(
        type="CONTAINS_SECTION",
        source_label="Part",
        target_label="Section"
    ))

    schema.add_relationship(RelDef(
        type="CONTAINS_PARAGRAPH",
        source_label="Section",
        target_label="Paragraph"
    ))

    schema.add_relationship(RelDef(
        type="CONTAINS_SUBSECTION",
        source_label="Section",
        target_label="Subsection"
    ))

    schema.add_relationship(RelDef(
        type="CONTAINS_CLAUSE",
        source_label="Subsection",
        target_label="Clause"
    ))

    schema.add_relationship(RelDef(
        type="CONTAINS_SUBCLAUSE",
        source_label="Clause",
        target_label="Subclause"
    ))

    schema.add_relationship(RelDef(
        type="CONTAINS_ITEM",
        source_label="Subclause",
        target_label="Item"
    ))

    # Cross-reference relationships (Pass 2 - by reading node content)
    schema.add_relationship(RelDef(
        type="REFERENCES",
        source_label="Clause",
        target_label="Section"
    ))

    schema.add_relationship(RelDef(
        type="REFERENCES",
        source_label="Section",
        target_label="Section"
    ))

    schema.add_relationship(RelDef(
        type="RELATED_TO",
        source_label="Clause",
        target_label="Clause"
    ))

    return schema


def create_legal_document_schema() -> Schema:
    """
    Create schema for legal documents.

    Entities:
    - Organization: Companies/parties involved
    - Clause: Numbered sections of agreements
    - Obligation: Duties and requirements
    - Payment: Financial terms and amounts

    Relationships:
    - PARTY_TO: Organization involved in clause
    - SPECIFIES: Clause defines obligation
    - REQUIRES_PAYMENT: Clause requires payment
    - OBLIGATED_TO: Organization responsible for obligation

    Returns:
        Schema: Configured legal document schema
    """
    schema = Schema("Legal Document Schema")

    # Node definitions
    schema.add_node(NodeDef(
        label="Organization",
        properties=[
            PropertyDef("name", "string", required=True),
            PropertyDef("role", "string"),
            PropertyDef("address", "string"),
        ]
    ))

    schema.add_node(NodeDef(
        label="Clause",
        properties=[
            PropertyDef("number", "string", required=True),
            PropertyDef("title", "string"),
            PropertyDef("text", "string", required=True),
        ]
    ))

    schema.add_node(NodeDef(
        label="Obligation",
        properties=[
            PropertyDef("description", "string", required=True),
            PropertyDef("obligated_party", "string"),
            PropertyDef("due_date", "date"),
            PropertyDef("type", "string"),
        ]
    ))

    schema.add_node(NodeDef(
        label="Payment",
        properties=[
            PropertyDef("amount", "float"),
            PropertyDef("currency", "string"),
            PropertyDef("due_date", "date"),
            PropertyDef("description", "string"),
        ]
    ))

    # Relationship definitions
    schema.add_relationship(RelDef(
        type="PARTY_TO",
        source_label="Organization",
        target_label="Clause"
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


def create_agreement_schema() -> Schema:
    """
    Create comprehensive schema for service agreements.

    Adds Agreement node type for more detailed agreement tracking.
    """
    schema = create_legal_document_schema()

    # Add Agreement node type
    schema.add_node(NodeDef(
        label="Agreement",
        properties=[
            PropertyDef("type", "string", required=True),
            PropertyDef("title", "string", required=True),
            PropertyDef("effective_date", "date"),
            PropertyDef("expiration_date", "date"),
        ]
    ))

    # Add relationships with Agreement
    schema.add_relationship(RelDef(
        type="PARTY_TO_AGREEMENT",
        source_label="Organization",
        target_label="Agreement"
    ))

    schema.add_relationship(RelDef(
        type="HAS_CLAUSE",
        source_label="Agreement",
        target_label="Clause",
        properties=[PropertyDef("sequence", "int")]
    ))

    return schema
