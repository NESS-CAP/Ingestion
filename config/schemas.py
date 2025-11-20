"""
Centralized schema definitions for all legal document processing.

Contains reusable schema templates for document ingestion and graph building.
"""

from src.core.schema import Schema, NodeDef, PropertyDef, RelDef


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
