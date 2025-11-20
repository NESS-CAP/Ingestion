from neo4j import GraphDatabase
from ingestion.shared.config.settings import NEO4J_CONFIG
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class GraphManager:
    def __init__(self):
        logger.info(f"Connecting to Neo4j at {NEO4J_CONFIG['uri']}")
        try:
            self.driver = GraphDatabase.driver(
                NEO4J_CONFIG["uri"],
                auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"]),
                connection_timeout=10
            )
            logger.info("Successfully connected to Neo4j")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise

    def close(self):
        self.driver.close()

    def execute_query(self, query: str, parameters: Dict = None) -> List[Dict]:
        """Execute a Cypher query and return results"""
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]

    def create_chunk_node(self, chunk_id: str, text: str, embedding: List[float], metadata: Dict) -> List[Dict]:
        """Create a Chunk node with embedding and metadata"""
        query = """
        CREATE (c:Chunk {
            id: $chunk_id,
            text: $text,
            embedding: $embedding,
            created_at: datetime()
        })
        SET c += $metadata
        RETURN c
        """
        return self.execute_query(query, {
            "chunk_id": chunk_id,
            "text": text,
            "embedding": embedding,
            "metadata": metadata
        })

    def create_document_node(self, doc_id: str, name: str, source: str) -> List[Dict]:
        """Create a Document node"""
        query = """
        CREATE (d:Document {
            id: $doc_id,
            name: $name,
            source: $source,
            ingested_at: datetime()
        })
        RETURN d
        """
        return self.execute_query(query, {
            "doc_id": doc_id,
            "name": name,
            "source": source
        })

    def link_chunk_to_document(self, chunk_id: str, doc_id: str, sequence: int) -> List[Dict]:
        """Link a Chunk to a Document with sequence order"""
        query = """
        MATCH (d:Document {id: $doc_id})
        MATCH (c:Chunk {id: $chunk_id})
        CREATE (d)-[:CONTAINS {sequence: $sequence}]->(c)
        RETURN d, c
        """
        return self.execute_query(query, {
            "doc_id": doc_id,
            "chunk_id": chunk_id,
            "sequence": sequence
        })

    def vector_search(self, embedding: List[float], limit: int = 5) -> List[Dict]:
        """Perform vector similarity search on chunk embeddings"""
        query = """
        CALL db.index.vector.queryNodes('chunk_embeddings', $limit, $embedding)
        YIELD node, score
        RETURN node.id as id, node.text as text, score
        LIMIT $limit
        """
        return self.execute_query(query, {
            "embedding": embedding,
            "limit": limit
        })

    def text_search(self, query_str: str, limit: int = 5) -> List[Dict]:
        """Search for chunks containing query text"""
        query = """
        MATCH (doc:Document)-[:CONTAINS]->(chunk:Chunk)
        WHERE chunk.text CONTAINS $query
        RETURN doc.name as document, chunk.id as id, chunk.text as text
        LIMIT $limit
        """
        return self.execute_query(query, {
            "query": query_str,
            "limit": limit
        })

    def get_document_chunks(self, doc_id: str) -> List[Dict]:
        """Get all chunks for a specific document ordered by sequence"""
        query = """
        MATCH (d:Document {id: $doc_id})-[rel:CONTAINS]->(c:Chunk)
        RETURN c.id as id, c.text as text, rel.sequence as sequence
        ORDER BY rel.sequence ASC
        """
        return self.execute_query(query, {"doc_id": doc_id})

    def get_all_documents(self) -> List[Dict]:
        """Get all documents in the database"""
        query = """
        MATCH (d:Document)
        RETURN d.id as id, d.name as name, d.source as source, d.ingested_at as ingested_at
        """
        return self.execute_query(query)

    def delete_document(self, doc_id: str) -> bool:
        """Delete a document and all its chunks"""
        query = """
        MATCH (d:Document {id: $doc_id})-[:CONTAINS]->(c:Chunk)
        DETACH DELETE d, c
        """
        try:
            self.execute_query(query, {"doc_id": doc_id})
            logger.info(f"Successfully deleted document: {doc_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting document {doc_id}: {e}")
            return False

    def create_vector_index(self) -> bool:
        """Create vector index for similarity search"""
        query = """
        CREATE VECTOR INDEX chunk_embeddings IF NOT EXISTS
        FOR (c:Chunk) ON (c.embedding)
        OPTIONS {indexConfig: {`vector.dimensions`: 384, `vector.similarity_function`: 'cosine'}}
        """
        try:
            self.execute_query(query)
            logger.info("Vector index created or already exists")
            return True
        except Exception as e:
            logger.warning(f"Vector index creation note: {e}")
            return False

    def delete_all(self, confirm: bool = False) -> bool:
        """
        Delete all nodes and relationships from graph.

        Args:
            confirm: Must be True to prevent accidental deletion

        Returns:
            True if successful, False otherwise
        """
        if not confirm:
            raise ValueError(
                "Deleting entire graph is irreversible. "
                "Set confirm=True to proceed."
            )

        try:
            self.execute_query("MATCH (n) DETACH DELETE n")
            logger.warning("Graph cleared: All nodes and relationships deleted")
            return True
        except Exception as e:
            logger.error(f"Error clearing graph: {e}")
            return False

    def get_graph_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the graph.

        Returns:
            Dictionary with node count, relationship count, and node type breakdown
        """
        try:
            # Total nodes
            nodes_result = self.execute_query("MATCH (n) RETURN count(n) as count")
            total_nodes = nodes_result[0]['count'] if nodes_result else 0

            # Total relationships
            rels_result = self.execute_query("MATCH ()-[r]->() RETURN count(r) as count")
            total_rels = rels_result[0]['count'] if rels_result else 0

            # Nodes by type
            types_result = self.execute_query("""
                MATCH (n)
                RETURN labels(n)[0] as label, count(n) as count
                ORDER BY count DESC
            """)

            logger.debug(f"Graph stats: {total_nodes} nodes, {total_rels} relationships")
            return {
                "total_nodes": total_nodes,
                "total_relationships": total_rels,
                "node_types": types_result
            }
        except Exception as e:
            logger.error(f"Error getting graph stats: {e}")
            return {"total_nodes": 0, "total_relationships": 0, "node_types": []}
