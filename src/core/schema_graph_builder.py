from typing import Dict, List, Any, Optional
from src.core.graph_manager import GraphManager
from src.core.schema import Schema
import logging

logger = logging.getLogger(__name__)


class SchemaGraphBuilder:
    """
    Builds a Neo4j graph from extracted structured data based on schema.
    """

    def __init__(self, graph: GraphManager, schema: Schema):
        self.graph = graph
        self.schema = schema
        self.node_id_to_neo4j_id = {}  # Map extraction IDs to Neo4j IDs

    def build_graph(self, extracted_data: Dict[str, Any], document_id: str = None) -> Dict[str, Any]:
        """
        Build Neo4j graph from extracted entities and relationships.

        Args:
            extracted_data: Output from SchemaExtractor.extract_from_text()
            document_id: Optional document ID to link all nodes to

        Returns:
            {
                "nodes_created": count,
                "relationships_created": count,
                "node_mapping": {extraction_id -> neo4j_id}
            }
        """
        stats = {
            "nodes_created": 0,
            "relationships_created": 0,
            "node_mapping": {}
        }

        # Step 1: Create all nodes
        for node in extracted_data.get("nodes", []):
            neo4j_id = self._create_node(node, document_id)
            self.node_id_to_neo4j_id[node["id"]] = neo4j_id
            stats["node_mapping"][node["id"]] = neo4j_id
            stats["nodes_created"] += 1
            logger.debug(f"Created {node['label']} node: {node['id']}")

        # Step 2: Create all relationships
        for rel in extracted_data.get("relationships", []):
            source_neo4j_id = self.node_id_to_neo4j_id.get(rel["source_id"])
            target_neo4j_id = self.node_id_to_neo4j_id.get(rel["target_id"])

            if source_neo4j_id and target_neo4j_id:
                self._create_relationship(
                    source_neo4j_id,
                    rel["type"],
                    target_neo4j_id,
                    rel.get("properties", {})
                )
                stats["relationships_created"] += 1
                logger.debug(f"Created relationship: {rel['type']}")

        # Step 3: Link all nodes to document if provided
        if document_id:
            document_cypher = """
            MATCH (d:Document {id: $doc_id})
            MATCH (n {neo4j_id: $node_id})
            CREATE (d)-[:CONTAINS_ENTITY]->(n)
            """
            for extraction_id, neo4j_id in self.node_id_to_neo4j_id.items():
                try:
                    self.graph.execute_query(document_cypher, {
                        "doc_id": document_id,
                        "node_id": neo4j_id
                    })
                except KeyError as e:
                    logger.debug(f"Node missing expected property: {e}")
                except Exception as e:
                    logger.warning(f"Could not link node {neo4j_id} to document: {e}")

        return stats

    def _create_node(self, node_data: Dict, document_id: str = None) -> str:
        """
        Create a node in Neo4j from extracted data.

        Returns:
            Neo4j internal node ID
        """
        label = node_data["label"]
        properties = node_data["properties"]
        extraction_id = node_data["id"]

        # Add extraction ID to properties for reference
        properties["_extraction_id"] = extraction_id

        # Build Cypher query
        props_str = ", ".join([
            f"{k}: ${k}" for k in properties.keys()
        ])

        cypher = f"""
        CREATE (n:{label} {{{props_str}}})
        RETURN id(n) as neo4j_id
        """

        result = self.graph.execute_query(cypher, properties)

        if result:
            return result[0].get("neo4j_id")
        return extraction_id

    def _create_relationship(
        self,
        source_node_id: str,
        rel_type: str,
        target_node_id: str,
        properties: Dict = None
    ) -> bool:
        """
        Create a relationship between two nodes.
        """
        if properties is None:
            properties = {}

        cypher = """
        MATCH (source) WHERE id(source) = $source_id
        MATCH (target) WHERE id(target) = $target_id
        CREATE (source)-[r:""" + rel_type + """]->(target)
        SET r += $properties
        RETURN r
        """

        try:
            result = self.graph.execute_query(cypher, {
                "source_id": source_node_id,
                "target_id": target_node_id,
                "properties": properties
            })
            return len(result) > 0
        except Exception as e:
            logger.error(f"Error creating relationship: {e}")
            return False

    def print_graph_stats(self) -> None:
        """Log statistics about created graph"""
        stats = self.graph.get_graph_stats()
        logger.info(f"Graph Statistics: {stats['total_nodes']} nodes, {stats['total_relationships']} relationships")
        for node_type in stats.get('node_types', []):
            logger.info(f"  {node_type.get('label', 'Unknown')}: {node_type.get('count', 0)}")
