"""
Neo4j linker for semantic nodes.

Creates semantic nodes and APPLIES_TO relationships to OBC content.
All operations use MERGE for idempotency.
"""

from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase

from .semantic_schema import (
    SEMANTIC_CONSTRAINTS,
    get_all_seed_data,
)


class SemanticLinker:
    """Creates and links semantic nodes in Neo4j."""

    def __init__(self, uri: str, user: str, password: str, database: str = "neo4j"):
        """
        Initialize the linker with Neo4j connection details.

        Args:
            uri: Neo4j connection URI
            user: Neo4j username
            password: Neo4j password
            database: Neo4j database name
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
        self.session_kwargs = {"database": database} if database else {}

    def close(self):
        """Close the Neo4j driver."""
        self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def init_constraints(self):
        """Create uniqueness constraints for semantic nodes."""
        with self.driver.session(**self.session_kwargs) as session:
            for constraint in SEMANTIC_CONSTRAINTS:
                try:
                    session.run(constraint)
                    print(f"[INFO] Created constraint: {constraint[:60]}...", flush=True)
                except Exception as e:
                    # Constraint may already exist
                    if "already exists" not in str(e).lower():
                        print(f"[WARNING] Constraint creation: {e}", flush=True)

    def seed_semantic_nodes(self):
        """Create seed data for all semantic node types."""
        seed_data = get_all_seed_data()

        with self.driver.session(**self.session_kwargs) as session:
            # Topics
            for topic in seed_data["Topic"]:
                session.execute_write(self._merge_topic, topic)

            # BuildingTypes
            for bt in seed_data["BuildingType"]:
                session.execute_write(self._merge_building_type, bt)

            # OccupancyClasses
            for oc in seed_data["OccupancyClass"]:
                session.execute_write(self._merge_occupancy_class, oc)

            # ConstructionTypes
            for ct in seed_data["ConstructionType"]:
                session.execute_write(self._merge_construction_type, ct)

            # SpaceTypes
            for st in seed_data["SpaceType"]:
                session.execute_write(self._merge_space_type, st)

        print("[INFO] Seeded all semantic nodes", flush=True)

    @staticmethod
    def _merge_topic(tx, data: Dict[str, Any]):
        tx.run("""
        MERGE (t:Topic {name: $name})
        SET t.description = $description,
            t.keywords = $keywords
        """, name=data["name"], description=data["description"],
             keywords=data.get("keywords", []))

    @staticmethod
    def _merge_building_type(tx, data: Dict[str, Any]):
        tx.run("""
        MERGE (bt:BuildingType {name: $name})
        SET bt.description = $description,
            bt.obc_parts = $obc_parts,
            bt.keywords = $keywords
        """, name=data["name"], description=data["description"],
             obc_parts=data.get("obc_parts", []),
             keywords=data.get("keywords", []))

    @staticmethod
    def _merge_occupancy_class(tx, data: Dict[str, Any]):
        tx.run("""
        MERGE (oc:OccupancyClass {code: $code})
        SET oc.name = $name,
            oc.description = $description,
            oc.divisions = $divisions
        """, code=data["code"], name=data["name"],
             description=data["description"],
             divisions=data.get("divisions", []))

    @staticmethod
    def _merge_construction_type(tx, data: Dict[str, Any]):
        tx.run("""
        MERGE (ct:ConstructionType {name: $name})
        SET ct.description = $description,
            ct.keywords = $keywords
        """, name=data["name"], description=data["description"],
             keywords=data.get("keywords", []))

    @staticmethod
    def _merge_space_type(tx, data: Dict[str, Any]):
        tx.run("""
        MERGE (st:SpaceType {name: $name})
        SET st.description = $description,
            st.keywords = $keywords
        """, name=data["name"], description=data["description"],
             keywords=data.get("keywords", []))

    def link_concepts_to_node(self, node_ref: str, concepts: Dict[str, List[str]]):
        """
        Create APPLIES_TO relationships from semantic nodes to an OBC node.

        Args:
            node_ref: The reference ID of the OBC node (e.g., "9.8.2.1.(1)")
            concepts: Dict with topics, building_types, etc.
        """
        if not concepts:
            return

        with self.driver.session(**self.session_kwargs) as session:
            # Link Topics
            for topic_name in concepts.get("topics", []):
                session.execute_write(
                    self._link_topic_to_sentence,
                    topic_name,
                    node_ref,
                )

            # Link BuildingTypes
            for bt_name in concepts.get("building_types", []):
                session.execute_write(
                    self._link_building_type_to_sentence,
                    bt_name,
                    node_ref,
                )

            # Link OccupancyClasses
            for oc_code in concepts.get("occupancy_classes", []):
                session.execute_write(
                    self._link_occupancy_class_to_sentence,
                    oc_code,
                    node_ref,
                )

            # Link ConstructionTypes
            for ct_name in concepts.get("construction_types", []):
                session.execute_write(
                    self._link_construction_type_to_sentence,
                    ct_name,
                    node_ref,
                )

            # Link SpaceTypes
            for st_name in concepts.get("space_types", []):
                session.execute_write(
                    self._link_space_type_to_sentence,
                    st_name,
                    node_ref,
                )

    @staticmethod
    def _link_topic_to_sentence(tx, topic_name: str, sentence_ref: str):
        tx.run("""
        MATCH (t:Topic {name: $topic_name})
        MATCH (s:Sentence {ref: $sentence_ref})
        MERGE (t)-[:APPLIES_TO]->(s)
        """, topic_name=topic_name, sentence_ref=sentence_ref)

    @staticmethod
    def _link_building_type_to_sentence(tx, bt_name: str, sentence_ref: str):
        tx.run("""
        MATCH (bt:BuildingType {name: $bt_name})
        MATCH (s:Sentence {ref: $sentence_ref})
        MERGE (bt)-[:APPLIES_TO]->(s)
        """, bt_name=bt_name, sentence_ref=sentence_ref)

    @staticmethod
    def _link_occupancy_class_to_sentence(tx, oc_code: str, sentence_ref: str):
        tx.run("""
        MATCH (oc:OccupancyClass {code: $oc_code})
        MATCH (s:Sentence {ref: $sentence_ref})
        MERGE (oc)-[:APPLIES_TO]->(s)
        """, oc_code=oc_code, sentence_ref=sentence_ref)

    @staticmethod
    def _link_construction_type_to_sentence(tx, ct_name: str, sentence_ref: str):
        tx.run("""
        MATCH (ct:ConstructionType {name: $ct_name})
        MATCH (s:Sentence {ref: $sentence_ref})
        MERGE (ct)-[:APPLIES_TO]->(s)
        """, ct_name=ct_name, sentence_ref=sentence_ref)

    @staticmethod
    def _link_space_type_to_sentence(tx, st_name: str, sentence_ref: str):
        tx.run("""
        MATCH (st:SpaceType {name: $st_name})
        MATCH (s:Sentence {ref: $sentence_ref})
        MERGE (st)-[:APPLIES_TO]->(s)
        """, st_name=st_name, sentence_ref=sentence_ref)

    def get_sentences_for_extraction(
        self,
        limit: Optional[int] = None,
        skip_already_linked: bool = True,
    ) -> List[Dict[str, str]]:
        """
        Fetch Sentence nodes from Neo4j for extraction.

        Args:
            limit: Optional limit on number of sentences
            skip_already_linked: Skip sentences that already have semantic links

        Returns:
            List of dicts with "ref" and "text" keys
        """
        with self.driver.session(**self.session_kwargs) as session:
            if skip_already_linked:
                query = """
                MATCH (s:Sentence)
                WHERE s.text IS NOT NULL
                AND NOT EXISTS {
                    MATCH (semantic)-[:APPLIES_TO]->(s)
                    WHERE semantic:Topic OR semantic:BuildingType OR
                          semantic:OccupancyClass OR semantic:ConstructionType OR
                          semantic:SpaceType
                }
                RETURN s.ref AS ref, s.text AS text
                ORDER BY s.ref
                """
            else:
                query = """
                MATCH (s:Sentence)
                WHERE s.text IS NOT NULL
                RETURN s.ref AS ref, s.text AS text
                ORDER BY s.ref
                """

            if limit:
                query += f" LIMIT {limit}"

            result = session.run(query)
            return [{"ref": r["ref"], "text": r["text"]} for r in result]

    def get_extraction_stats(self) -> Dict[str, int]:
        """Get statistics about semantic extraction progress."""
        with self.driver.session(**self.session_kwargs) as session:
            stats = {}

            # Total sentences
            result = session.run("""
            MATCH (s:Sentence)
            WHERE s.text IS NOT NULL
            RETURN count(s) AS count
            """)
            stats["total_sentences"] = result.single()["count"]

            # Linked sentences
            result = session.run("""
            MATCH (semantic)-[:APPLIES_TO]->(s:Sentence)
            RETURN count(DISTINCT s) AS count
            """)
            stats["linked_sentences"] = result.single()["count"]

            # Semantic node counts
            for label in ["Topic", "BuildingType", "OccupancyClass", "ConstructionType", "SpaceType"]:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) AS count")
                stats[f"{label.lower()}_count"] = result.single()["count"]

            # Total APPLIES_TO relationships
            result = session.run("""
            MATCH ()-[r:APPLIES_TO]->()
            RETURN count(r) AS count
            """)
            stats["applies_to_count"] = result.single()["count"]

            return stats
