"""
Stage 3: Neo4j Ingestion for HTML-Extracted Data

Creates fine-grained nodes for every:
- Clause (numbered items)
- SubClause (lettered items)
- Item (roman numerals)
- Definition
- Reference

Instead of one node per section (30 nodes for 30 pages), this creates
thousands of granular nodes for better queryability and search.
"""

import logging
from typing import Dict, List, Any, Optional
import json
from pathlib import Path
import sys
import hashlib

sys.path.insert(0, str(Path(__file__).parents[3]))

from ingestion.src.core.graph_manager import GraphManager
from ingestion.src.core.embeddings import EmbeddingManager
from ingestion.src.core.schema import create_elaws_obc_schema

logger = logging.getLogger(__name__)


class Neo4jHTMLIngester:
    """Ingest fine-grained HTML-extracted data into Neo4j"""

    def __init__(self, graph: GraphManager):
        """
        Initialize ingester.

        Args:
            graph: GraphManager instance for Neo4j connection
        """
        self.graph = graph
        self.schema = create_elaws_obc_schema()
        self.embedding_manager = EmbeddingManager()
        self.created_nodes = {}
        self.node_count = 0
        self.relationship_count = 0

    def ingest(self, extracted_data: List[Dict[str, Any]], document_id: str = "obc_html_332_12") -> Dict[str, Any]:
        """
        Main ingestion method.

        Args:
            extracted_data: Output from Stage 2 extraction
            document_id: ID for the document node

        Returns:
            Statistics about ingestion
        """
        stats = {
            "success": True,
            "nodes_created": 0,
            "relationships_created": 0,
            "clauses_created": 0,
            "subclauses_created": 0,
            "items_created": 0,
            "definitions_created": 0,
            "errors": []
        }

        try:
            # Step 1: Create document and regulation hierarchy
            doc_node_id = self._create_document_node(document_id)
            reg_node_id = self._create_regulation_node()

            stats["nodes_created"] = 2

            # Step 2: Create divisions and parts
            div_nodes = self._create_divisions()
            part_nodes = self._create_parts()
            stats["nodes_created"] += len(div_nodes) + len(part_nodes)

            # Step 3: Process extracted data
            for doc_idx, document in enumerate(extracted_data):
                logger.info(f"Processing document {doc_idx + 1}/{len(extracted_data)}")

                try:
                    doc_stats = self._process_document(
                        document, reg_node_id, part_nodes[0] if part_nodes else None
                    )
                    stats["nodes_created"] += doc_stats["nodes_created"]
                    stats["relationships_created"] += doc_stats["relationships_created"]
                    stats["clauses_created"] += doc_stats["clauses_created"]
                    stats["subclauses_created"] += doc_stats["subclauses_created"]
                    stats["items_created"] += doc_stats["items_created"]
                    stats["definitions_created"] += doc_stats["definitions_created"]

                except Exception as e:
                    error_msg = f"Error processing document {doc_idx}: {e}"
                    logger.error(error_msg)
                    stats["errors"].append(error_msg)

            logger.info(
                f"Ingestion complete: {stats['nodes_created']} nodes, "
                f"{stats['relationships_created']} relationships, "
                f"{stats['clauses_created']} clauses"
            )

        except Exception as e:
            stats["success"] = False
            stats["errors"].append(str(e))
            logger.error(f"Ingestion failed: {e}", exc_info=True)

        return stats

    def _create_document_node(self, document_id: str) -> str:
        """Create document node"""
        query = """
        CREATE (d:Document {
            id: $id,
            title: 'Ontario Building Code - E-Laws O. Reg. 332/12 (HTML)',
            source: 'https://www.ontario.ca/laws/regulation/120332',
            ingested_at: datetime(),
            format: 'html'
        })
        RETURN id(d) as neo4j_id
        """
        result = self.graph.execute_query(query, {"id": document_id})
        node_id = result[0]["neo4j_id"] if result else None
        self.created_nodes["document"] = node_id
        return node_id

    def _create_regulation_node(self) -> str:
        """Create regulation node"""
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
        """Create division nodes"""
        divisions = [
            ("A", "Compliance and Objectives"),
            ("B", "Building Occupancy")
        ]
        created = []

        for div_id, title in divisions:
            query = """
            CREATE (d:Division {
                division_id: $div_id,
                title: $title
            })
            RETURN id(d) as neo4j_id
            """
            result = self.graph.execute_query(query, {
                "div_id": div_id,
                "title": title
            })
            node_id = result[0]["neo4j_id"] if result else None

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
        """Create part nodes"""
        parts = [
            ("3", "Fire Protection, Occupant Safety and Accessibility"),
        ]
        created = []

        for part_num, title in parts:
            query = """
            CREATE (p:Part {
                part_number: $part_num,
                title: $title
            })
            RETURN id(p) as neo4j_id
            """
            result = self.graph.execute_query(query, {
                "part_num": part_num,
                "title": title
            })
            node_id = result[0]["neo4j_id"] if result else None

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

    def _process_document(self, document: Dict[str, Any], reg_id: str, part_id: str) -> Dict[str, int]:
        """Process a single document"""
        stats = {
            "nodes_created": 0,
            "relationships_created": 0,
            "clauses_created": 0,
            "subclauses_created": 0,
            "items_created": 0,
            "definitions_created": 0,
        }

        extracted_chunks = document.get("extracted_chunks", [])

        for chunk_idx, chunk in enumerate(extracted_chunks):
            try:
                chunk_stats = self._process_chunk(chunk, part_id)
                for key in stats:
                    stats[key] += chunk_stats.get(key, 0)

            except Exception as e:
                logger.error(f"Error processing chunk {chunk_idx}: {e}")
                continue

        return stats

    def _process_chunk(self, chunk: Dict[str, Any], part_id: str) -> Dict[str, int]:
        """Process a single chunk and create nodes for clauses"""
        stats = {
            "nodes_created": 0,
            "relationships_created": 0,
            "clauses_created": 0,
            "subclauses_created": 0,
            "items_created": 0,
            "definitions_created": 0,
        }

        extracted = chunk.get("extracted", {})
        metadata = chunk.get("content_metadata", {})

        # Get or create section
        section_number = metadata.get("section", "")
        section_node_id = self._get_or_create_section(section_number, part_id)

        # Process clauses
        clauses = extracted.get("clauses", [])
        for clause in clauses:
            clause_stats = self._process_clause(clause, section_node_id)
            stats["nodes_created"] += clause_stats["nodes_created"]
            stats["relationships_created"] += clause_stats["relationships_created"]
            stats["clauses_created"] += clause_stats["clauses_created"]
            stats["subclauses_created"] += clause_stats["subclauses_created"]
            stats["items_created"] += clause_stats["items_created"]

        # Process definitions
        definitions = extracted.get("definitions", [])
        for definition in definitions:
            def_node_id = self._create_definition_node(definition, section_node_id)
            if def_node_id:
                stats["definitions_created"] += 1
                stats["nodes_created"] += 1

        return stats

    def _get_or_create_section(self, section_number: str, part_id: str) -> Optional[str]:
        """Get or create section node"""
        if not section_number:
            return part_id  # Use part as parent if no section

        # Check if section exists
        check_query = """
        MATCH (s:Section {section_number: $section_num})
        RETURN id(s) as neo4j_id
        """
        result = self.graph.execute_query(check_query, {"section_num": section_number})

        if result:
            return result[0]["neo4j_id"]

        # Create new section
        create_query = """
        CREATE (s:Section {
            section_number: $section_num,
            title: $section_num
        })
        RETURN id(s) as neo4j_id
        """
        result = self.graph.execute_query(create_query, {"section_num": section_number})
        section_id = result[0]["neo4j_id"] if result else None

        # Link to part
        if section_id and part_id:
            rel_query = """
            MATCH (p) WHERE id(p) = $part_id
            MATCH (s) WHERE id(s) = $section_id
            CREATE (p)-[:HAS_SECTION]->(s)
            """
            self.graph.execute_query(rel_query, {
                "part_id": part_id,
                "section_id": section_id
            })

        return section_id

    def _process_clause(self, clause: Dict[str, Any], parent_section_id: str) -> Dict[str, int]:
        """Process a clause and create nodes for it and nested items"""
        stats = {
            "nodes_created": 0,
            "relationships_created": 0,
            "clauses_created": 0,
            "subclauses_created": 0,
            "items_created": 0,
        }

        clause_number = clause.get("number", "")
        clause_text = clause.get("text", "")
        clause_type = clause.get("type", "clause")

        # Create clause node
        clause_node_id = self._create_clause_node(
            clause_number, clause_text, parent_section_id
        )

        if clause_node_id:
            stats["nodes_created"] += 1
            stats["clauses_created"] += 1

            # Process nested items
            nested_items = clause.get("nested_items", [])
            for nested_item in nested_items:
                nested_stats = self._process_nested_item(
                    nested_item, clause_node_id
                )
                stats["nodes_created"] += nested_stats["nodes_created"]
                stats["relationships_created"] += nested_stats["relationships_created"]
                stats["subclauses_created"] += nested_stats["subclauses_created"]
                stats["items_created"] += nested_stats["items_created"]

        return stats

    def _process_nested_item(self, item: Dict[str, Any], parent_id: str, depth: int = 1) -> Dict[str, int]:
        """Process nested items (subclauses, items)"""
        stats = {
            "nodes_created": 0,
            "relationships_created": 0,
            "subclauses_created": 0,
            "items_created": 0,
        }

        item_number = item.get("number", "")
        item_text = item.get("text", "")
        item_type = item.get("type", "item")

        # Create node
        item_node_id = self._create_item_node(item_number, item_text, item_type)

        if item_node_id:
            stats["nodes_created"] += 1

            # Link to parent
            self.graph.execute_query(
                f"MATCH (p) WHERE id(p) = $parent_id MATCH (i) WHERE id(i) = $item_id CREATE (p)-[:HAS_{item_type.upper()}]->(i)",
                {"parent_id": parent_id, "item_id": item_node_id}
            )
            stats["relationships_created"] += 1

            # Count by type
            if "subclause" in item_type.lower():
                stats["subclauses_created"] += 1
            elif "item" in item_type.lower():
                stats["items_created"] += 1

            # Process deeply nested items
            nested = item.get("nested_items", [])
            if nested and depth < 3:  # Limit recursion depth
                for nested_item in nested:
                    nested_stats = self._process_nested_item(
                        nested_item, item_node_id, depth + 1
                    )
                    for key in nested_stats:
                        stats[key] += nested_stats[key]

        return stats

    def _create_clause_node(self, number: str, text: str, section_id: str) -> Optional[str]:
        """Create a clause node with embedding"""
        try:
            # Generate embedding
            embedding = self.embedding_manager.embed_text(text)

            query = """
            CREATE (c:Clause {
                clause_number: $number,
                text: $text,
                embedding: $embedding,
                hash: $hash
            })
            RETURN id(c) as neo4j_id
            """

            result = self.graph.execute_query(query, {
                "number": number,
                "text": text[:1000],  # Limit to 1000 chars
                "embedding": embedding,
                "hash": hashlib.md5(text.encode()).hexdigest()
            })

            clause_id = result[0]["neo4j_id"] if result else None

            # Link to section
            if clause_id and section_id:
                self.graph.execute_query(
                    "MATCH (s) WHERE id(s) = $section_id MATCH (c) WHERE id(c) = $clause_id CREATE (s)-[:HAS_CLAUSE]->(c)",
                    {"section_id": section_id, "clause_id": clause_id}
                )

            return clause_id

        except Exception as e:
            logger.error(f"Error creating clause node: {e}")
            return None

    def _create_item_node(self, number: str, text: str, item_type: str = "item") -> Optional[str]:
        """Create an item (subclause or item) node"""
        try:
            # Generate embedding
            embedding = self.embedding_manager.embed_text(text)

            node_label = "SubClause" if "subclause" in item_type.lower() else "Item"

            query = f"""
            CREATE (i:{node_label} {{
                number: $number,
                text: $text,
                embedding: $embedding,
                type: $type
            }})
            RETURN id(i) as neo4j_id
            """

            result = self.graph.execute_query(query, {
                "number": number,
                "text": text[:1000],
                "embedding": embedding,
                "type": item_type
            })

            return result[0]["neo4j_id"] if result else None

        except Exception as e:
            logger.error(f"Error creating item node: {e}")
            return None

    def _create_definition_node(self, definition: Dict[str, str], section_id: str) -> Optional[str]:
        """Create a definition node"""
        try:
            term = definition.get("term", "")
            definition_text = definition.get("definition", "")

            embedding = self.embedding_manager.embed_text(definition_text)

            query = """
            CREATE (d:Definition {
                term: $term,
                definition: $definition,
                embedding: $embedding
            })
            RETURN id(d) as neo4j_id
            """

            result = self.graph.execute_query(query, {
                "term": term,
                "definition": definition_text,
                "embedding": embedding
            })

            def_id = result[0]["neo4j_id"] if result else None

            # Link to section if provided
            if def_id and section_id:
                self.graph.execute_query(
                    "MATCH (s) WHERE id(s) = $section_id MATCH (d) WHERE id(d) = $def_id CREATE (s)-[:HAS_DEFINITION]->(d)",
                    {"section_id": section_id, "def_id": def_id}
                )

            return def_id

        except Exception as e:
            logger.error(f"Error creating definition node: {e}")
            return None


def main():
    """Run Neo4j ingestion"""
    logging.basicConfig(level=logging.INFO)

    extracted_path = "data/gpt_extracted.json"

    if not Path(extracted_path).exists():
        logger.error(f"Extracted file not found: {extracted_path}")
        return

    with open(extracted_path, "r", encoding="utf-8") as f:
        extracted_data = json.load(f)

    documents = extracted_data if isinstance(extracted_data, list) else [extracted_data]

    # Ingest to Neo4j
    graph = GraphManager()
    ingester = Neo4jHTMLIngester(graph)
    stats = ingester.ingest(documents)

    # Save stats
    with open("data/ingestion_stats_html.json", "w") as f:
        json.dump(stats, f, indent=2)

    logger.info(f"Ingestion complete: {json.dumps(stats, indent=2)}")

    graph.close()


if __name__ == "__main__":
    main()
