"""
Stage 3: Neo4j Ingestion Pipeline

Takes enriched OBC data from Stage 2 and ingests it into Neo4j using the E-Laws OBC schema.

Converts:
- Regulation → Division → Part → Section → Clause → SubClause → Item hierarchy
- Creates embeddings
- Links documents and sections
"""

import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import json
from dataclasses import dataclass, asdict

# Add root ingestion directory to path
sys.path.insert(0, str(Path(__file__).parents[3]))

# Import shared modules
from ingestion.src.core.graph_manager import GraphManager
from ingestion.src.core.schema import create_elaws_obc_schema
from ingestion.src.core.embeddings import EmbeddingManager

logger = logging.getLogger(__name__)


@dataclass
class OBCNodeData:
    """Data for a single node to be created in Neo4j"""
    node_id: str
    label: str
    properties: Dict[str, Any]


@dataclass
class OBCRelationshipData:
    """Data for a single relationship to be created in Neo4j"""
    rel_type: str
    source_id: str
    target_id: str
    properties: Dict[str, Any] = None

    def __post_init__(self):
        if self.properties is None:
            self.properties = {}


class Neo4jOBCIngester:
    """
    Ingests enriched OBC data into Neo4j using the E-Laws OBC schema.
    """

    def __init__(self, graph: GraphManager):
        self.graph = graph
        self.schema = create_elaws_obc_schema()
        self.embedding_manager = EmbeddingManager()
        self.created_nodes = {}  # Maps node_id -> neo4j_internal_id

    def ingest(self, enriched_data: Dict[str, Any], document_id: str = "obc_elaws_332_12") -> Dict[str, Any]:
        """
        Main ingestion method.

        Args:
            enriched_data: Output from Stage 2 enrichment
            document_id: ID for the document node

        Returns:
            {
                "success": bool,
                "nodes_created": int,
                "relationships_created": int,
                "errors": [str]
            }
        """
        stats = {
            "success": True,
            "nodes_created": 0,
            "relationships_created": 0,
            "errors": []
        }

        try:
            # Step 1: Create document node
            doc_node_id = self._create_document_node(document_id)
            stats["nodes_created"] += 1

            # Step 2: Create regulation and hierarchy
            reg_node_id = self._create_regulation_node()
            stats["nodes_created"] += 1

            # Step 3: Create divisions and parts
            div_nodes = self._create_divisions()
            stats["nodes_created"] += len(div_nodes)

            part_nodes = self._create_parts()
            stats["nodes_created"] += len(part_nodes)

            # Step 4: Process sections and clauses
            sections = enriched_data.get("sections", [])
            logger.info(f"Processing {len(sections)} sections")

            for section in sections:
                try:
                    section_nodes, section_rels = self._process_section(
                        section, part_nodes[0]  # Assume part 3
                    )
                    stats["nodes_created"] += len(section_nodes)
                    stats["relationships_created"] += len(section_rels)
                except Exception as e:
                    error_msg = f"Error processing section {section.get('number')}: {e}"
                    logger.error(error_msg)
                    stats["errors"].append(error_msg)

            logger.info(f"Ingestion complete: {stats['nodes_created']} nodes, {stats['relationships_created']} relationships")

        except Exception as e:
            stats["success"] = False
            stats["errors"].append(str(e))
            logger.error(f"Ingestion failed: {e}", exc_info=True)

        return stats

    def _create_document_node(self, document_id: str) -> str:
        """Create a document node"""
        query = """
        CREATE (d:Document {
            id: $id,
            title: 'Ontario Building Code - E-Laws O. Reg. 332/12',
            source: 'e-laws.pdf',
            ingested_at: datetime()
        })
        RETURN id(d) as neo4j_id
        """
        result = self.graph.execute_query(query, {"id": document_id})
        node_id = result[0]["neo4j_id"] if result else None
        self.created_nodes["document"] = node_id
        return node_id

    def _create_regulation_node(self) -> str:
        """Create the root Regulation node"""
        query = """
        CREATE (r:Regulation {
            regulation_id: '332/12',
            title: 'Building Code',
            abbreviation: 'O. Reg. 332/12',
            source_url: 'https://www.ontario.ca/laws/regulation/120332'
        })
        RETURN id(r) as neo4j_id
        """
        result = self.graph.execute_query(query, {})
        node_id = result[0]["neo4j_id"] if result else None
        self.created_nodes["regulation"] = node_id
        return node_id

    def _create_divisions(self) -> list:
        """Create Division nodes"""
        divisions = [
            ("A", "Compliance and Objectives"),
            ("B", "Building Occupancy")
        ]
        created = []

        for div_id, div_title in divisions:
            query = """
            CREATE (d:Division {
                division_id: $div_id,
                title: $title
            })
            RETURN id(d) as neo4j_id
            """
            result = self.graph.execute_query(query, {
                "div_id": div_id,
                "title": div_title
            })
            node_id = result[0]["neo4j_id"] if result else None

            # Link to regulation
            if node_id and self.created_nodes.get("regulation"):
                rel_query = """
                MATCH (r) WHERE id(r) = $reg_id
                MATCH (d) WHERE id(d) = $div_id
                CREATE (r)-[:HAS_DIVISION {sequence: $seq}]->(d)
                """
                self.graph.execute_query(rel_query, {
                    "reg_id": self.created_nodes["regulation"],
                    "div_id": node_id,
                    "seq": ord(div_id) - ord('A')
                })

            self.created_nodes[f"division_{div_id}"] = node_id
            created.append(node_id)

        return created

    def _create_parts(self) -> list:
        """Create Part nodes (e.g., Part 3)"""
        parts = [
            ("3", "Fire Protection, Occupant Safety and Accessibility"),
            ("9", "Housing and Recreational Construction"),
            ("11", "Renovation")
        ]
        created = []

        for part_num, part_title in parts:
            query = """
            CREATE (p:Part {
                part_number: $part_num,
                title: $title,
                sequence: $seq
            })
            RETURN id(p) as neo4j_id
            """
            result = self.graph.execute_query(query, {
                "part_num": part_num,
                "title": part_title,
                "seq": int(part_num)
            })
            node_id = result[0]["neo4j_id"] if result else None

            # Link to division A
            if node_id and self.created_nodes.get("division_A"):
                rel_query = """
                MATCH (d) WHERE id(d) = $div_id
                MATCH (p) WHERE id(p) = $part_id
                CREATE (d)-[:HAS_PART {sequence: $seq}]->(p)
                """
                self.graph.execute_query(rel_query, {
                    "div_id": self.created_nodes["division_A"],
                    "part_id": node_id,
                    "seq": int(part_num)
                })

            self.created_nodes[f"part_{part_num}"] = node_id
            created.append(node_id)

        return created

    def _process_section(self, section: Dict[str, Any], parent_part_id: str) -> tuple:
        """Process a section and its clauses"""
        nodes_created = []
        rels_created = []

        section_number = section.get("number", "")
        section_title = section.get("title", "")

        # Create section node
        query = """
        CREATE (s:Section {
            section_number: $number,
            title: $title,
            sequence: $seq
        })
        RETURN id(s) as neo4j_id
        """
        result = self.graph.execute_query(query, {
            "number": section_number,
            "title": section_title,
            "seq": self._calculate_sequence(section_number)
        })
        section_node_id = result[0]["neo4j_id"] if result else None
        nodes_created.append(section_node_id)

        # Link section to part
        if section_node_id and parent_part_id:
            rel_query = """
            MATCH (p) WHERE id(p) = $part_id
            MATCH (s) WHERE id(s) = $section_id
            CREATE (p)-[:HAS_SECTION {sequence: $seq}]->(s)
            """
            self.graph.execute_query(rel_query, {
                "part_id": parent_part_id,
                "section_id": section_node_id,
                "seq": self._calculate_sequence(section_number)
            })
            rels_created.append(1)

        # Process subsections
        subsections = section.get("subsections", [])
        for subsection in subsections:
            sub_nodes, sub_rels = self._process_subsection(
                subsection, section_node_id
            )
            nodes_created.extend(sub_nodes)
            rels_created.extend(sub_rels)

        return nodes_created, rels_created

    def _process_subsection(self, subsection: Dict[str, Any], parent_section_id: str) -> tuple:
        """Process a subsection"""
        nodes_created = []
        rels_created = []

        subsection_number = subsection.get("number", "")
        subsection_title = subsection.get("title", "")

        # Create subsection node
        query = """
        CREATE (ss:Section {
            section_number: $number,
            title: $title,
            sequence: $seq
        })
        RETURN id(ss) as neo4j_id
        """
        result = self.graph.execute_query(query, {
            "number": subsection_number,
            "title": subsection_title,
            "seq": self._calculate_sequence(subsection_number)
        })
        subsection_node_id = result[0]["neo4j_id"] if result else None
        nodes_created.append(subsection_node_id)

        # Link subsection to parent section
        if subsection_node_id and parent_section_id:
            rel_query = """
            MATCH (parent) WHERE id(parent) = $parent_id
            MATCH (sub) WHERE id(sub) = $sub_id
            CREATE (parent)-[:HAS_SECTION {sequence: $seq}]->(sub)
            """
            self.graph.execute_query(rel_query, {
                "parent_id": parent_section_id,
                "sub_id": subsection_node_id,
                "seq": self._calculate_sequence(subsection_number)
            })
            rels_created.append(1)

        # Process clauses
        clauses = subsection.get("clauses", [])
        for clause_idx, clause in enumerate(clauses):
            clause_nodes, clause_rels = self._process_clause(
                clause, subsection_node_id, clause_idx
            )
            nodes_created.extend(clause_nodes)
            rels_created.extend(clause_rels)

        return nodes_created, rels_created

    def _process_clause(self, clause: Dict[str, Any], parent_section_id: str, sequence: int) -> tuple:
        """Process a clause"""
        nodes_created = []
        rels_created = []

        clause_number = clause.get("number", f"({sequence})")
        clause_text = clause.get("text", "")

        # Create clause node
        query = """
        CREATE (c:Clause {
            clause_number: $number,
            text: $text,
            sequence: $seq
        })
        RETURN id(c) as neo4j_id
        """
        result = self.graph.execute_query(query, {
            "number": clause_number,
            "text": clause_text[:500],  # Limit text
            "seq": sequence
        })
        clause_node_id = result[0]["neo4j_id"] if result else None
        nodes_created.append(clause_node_id)

        # Link clause to section
        if clause_node_id and parent_section_id:
            rel_query = """
            MATCH (s) WHERE id(s) = $section_id
            MATCH (c) WHERE id(c) = $clause_id
            CREATE (s)-[:HAS_CLAUSE {sequence: $seq}]->(c)
            """
            self.graph.execute_query(rel_query, {
                "section_id": parent_section_id,
                "clause_id": clause_node_id,
                "seq": sequence
            })
            rels_created.append(1)

        return nodes_created, rels_created

    def _calculate_sequence(self, number_string: str) -> int:
        """Convert section number to sortable sequence"""
        try:
            parts = number_string.split(".")
            return sum(int(p) * (10000 ** (4 - i)) for i, p in enumerate(parts[:4]))
        except:
            return 0


def main():
    """Run Stage 3 ingestion"""
    import os
    from dotenv import load_dotenv

    load_dotenv()

    # Configuration
    input_file = "../data/obc_enriched.json"
    output_file = "../data/ingestion_stats.json"

    # Load enriched data
    logger.info(f"Loading enriched data from {input_file}")
    with open(input_file, 'r') as f:
        enriched_data = json.load(f)

    try:
        # Connect to Neo4j
        graph = GraphManager()

        # Run ingestion
        logger.info("Starting Stage 3 ingestion")
        ingester = Neo4jOBCIngester(graph)
        stats = ingester.ingest(enriched_data)

        # Save stats
        with open(output_file, 'w') as f:
            json.dump(stats, f, indent=2)

        logger.info(f"Ingestion stats saved to {output_file}")
        logger.info(f"Result: {stats}")

        graph.close()
        return stats["success"]

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
