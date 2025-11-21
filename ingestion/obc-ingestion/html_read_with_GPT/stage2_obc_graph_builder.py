"""
Stage 2: OBC Graph Builder

Takes extracted HTML data (sections, clauses, etc.) and builds proper Neo4j graph
using the comprehensive OBC schema with:
- Hierarchical structure (Division → Part → Section → Article → Sentence → Clause)
- Building classifications
- Requirements and components
- Compliance checking support
"""

import logging
from typing import Dict, List, Any, Optional
import json
from pathlib import Path
import sys
from datetime import datetime
import re

sys.path.insert(0, str(Path(__file__).parents[3]))

from ingestion.shared.src.core.graph_manager import GraphManager
from ingestion.shared.src.core.obc_schema import create_obc_schema
from ingestion.shared.src.core.embeddings import EmbeddingManager

logger = logging.getLogger(__name__)


class OBCGraphBuilder:
    """Build OBC graph in Neo4j from extracted data"""

    def __init__(self, graph: GraphManager):
        self.graph = graph
        self.schema = create_obc_schema()
        self.embedding_manager = EmbeddingManager()
        self.created_nodes = {}
        self.section_sentences = {}  # Track sentences per section for summaries
        self.article_sentences = {}  # Track sentences per article for summaries

    def build(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build complete OBC graph from extracted data.

        Args:
            extracted_data: Output from HTML extraction with sections and clauses

        Returns:
            Statistics about graph building
        """
        stats = {
            "success": True,
            "nodes_created": {
                "divisions": 0,
                "parts": 0,
                "sections": 0,
                "articles": 0,
                "sentences": 0,
                "clauses": 0,
                "requirements": 0,
                "components": 0,
                "occupancy_groups": 0,
                "total": 0
            },
            "relationships_created": 0,
            "errors": []
        }

        try:
            # Phase 1: Create base structure
            logger.info("Phase 1: Creating base structure (Division → Part → Section)")
            self._create_base_structure()
            stats["nodes_created"]["divisions"] = 1
            stats["nodes_created"]["parts"] = 1
            stats["relationships_created"] += 1

            # Phase 2: Create occupancy classifications
            logger.info("Phase 2: Creating occupancy classifications")
            self._create_occupancy_classifications()
            stats["nodes_created"]["occupancy_groups"] = 1

            # Phase 3: Process extracted sections and clauses
            logger.info("Phase 3: Processing extracted content")
            sections = extracted_data.get("sections", [])

            for section_idx, section in enumerate(sections):
                try:
                    section_stats = self._process_section(section)

                    stats["nodes_created"]["sections"] += section_stats.get("sections", 0)
                    stats["nodes_created"]["articles"] += section_stats.get("articles", 0)
                    stats["nodes_created"]["sentences"] += section_stats.get("sentences", 0)
                    stats["nodes_created"]["clauses"] += section_stats.get("clauses", 0)
                    stats["nodes_created"]["requirements"] += section_stats.get("requirements", 0)
                    stats["nodes_created"]["components"] += section_stats.get("components", 0)
                    stats["relationships_created"] += section_stats.get("relationships", 0)

                except Exception as e:
                    error_msg = f"Error processing section {section_idx}: {e}"
                    logger.error(error_msg)
                    stats["errors"].append(error_msg)
                    continue

            stats["nodes_created"]["total"] = sum(
                v for k, v in stats["nodes_created"].items() if k != "total"
            )

            logger.info(f"Graph building complete: {stats['nodes_created']['total']} nodes created")

        except Exception as e:
            stats["success"] = False
            stats["errors"].append(str(e))
            logger.error(f"Graph building failed: {e}", exc_info=True)

        return stats

    def _create_base_structure(self):
        """Create Division B and Part 3"""
        # Create Division B
        div_query = """
        CREATE (div:Division {
            id: "DIV_B",
            name: "Division B",
            description: "Acceptable Solutions"
        })
        RETURN id(div) as id
        """
        result = self.graph.execute_query(div_query, {})
        div_id = result[0]["id"] if result else None
        self.created_nodes["division_b"] = div_id

        # Create Part 3
        part_query = """
        CREATE (p:Part {
            id: "PART_3",
            number: 3,
            name: "Fire Protection, Occupancy and Building Height and Area",
            division: "B",
            description: "Health and fire safety requirements"
        })
        RETURN id(p) as id
        """
        result = self.graph.execute_query(part_query, {})
        part_id = result[0]["id"] if result else None
        self.created_nodes["part_3"] = part_id

        # Link Division to Part
        if div_id and part_id:
            rel_query = """
            MATCH (div) WHERE id(div) = $div_id
            MATCH (p) WHERE id(p) = $part_id
            CREATE (div)-[:CONTAINS]->(p)
            """
            self.graph.execute_query(rel_query, {"div_id": div_id, "part_id": part_id})

    def _create_occupancy_classifications(self):
        """Create occupancy groups and divisions"""
        # Create Group A
        group_query = """
        CREATE (og:OccupancyGroup {
            id: "GROUP_A",
            group: "A",
            name: "Assembly Occupancies",
            description: "Buildings for gathering of persons for civic, political, travel..."
        })
        RETURN id(og) as id
        """
        result = self.graph.execute_query(group_query, {})
        group_id = result[0]["id"] if result else None
        self.created_nodes["group_a"] = group_id

        # Create Group A Division 1
        div_query = """
        CREATE (od:OccupancyDivision {
            id: "GROUP_A_DIV_1",
            group: "A",
            division: 1,
            name: "Theatres and Concert Halls",
            description: "Assembly for entertainment and viewing performances"
        })
        RETURN id(od) as id
        """
        result = self.graph.execute_query(div_query, {})
        div_id = result[0]["id"] if result else None
        self.created_nodes["group_a_div_1"] = div_id

        # Link Group to Division
        if group_id and div_id:
            rel_query = """
            MATCH (og) WHERE id(og) = $group_id
            MATCH (od) WHERE id(od) = $div_id
            CREATE (og)-[:HAS_DIVISION]->(od)
            """
            self.graph.execute_query(rel_query, {"group_id": group_id, "div_id": div_id})

    def _process_section(self, section: Dict[str, Any]) -> Dict[str, int]:
        """Process a section and its extracted clauses"""
        stats = {
            "sections": 0,
            "articles": 0,
            "sentences": 0,
            "clauses": 0,
            "requirements": 0,
            "components": 0,
            "relationships": 0
        }

        section_number = section.get("section_number", "")
        section_content = section.get("content", "")
        extracted_clauses = section.get("extracted_clauses", [])

        if not section_number:
            return stats

        # Initialize sentence tracking for this section
        self.section_sentences[section_number] = []

        # Generate embedding for section content
        section_embedding = self.embedding_manager.embed_text(section_content)

        # Create Section node with text and embedding
        section_query = """
        CREATE (s:Section {
            id: $id,
            number: $number,
            part: 3,
            name: $number,
            text: $text,
            embedding: $embedding
        })
        RETURN id(s) as id
        """
        result = self.graph.execute_query(section_query, {
            "id": f"SEC_{section_number}",
            "number": section_number,
            "text": section_content,
            "embedding": section_embedding
        })
        section_id = result[0]["id"] if result else None

        if section_id:
            stats["sections"] += 1

            # Link Part to Section
            if "part_3" in self.created_nodes:
                rel_query = """
                MATCH (p) WHERE id(p) = $part_id
                MATCH (s) WHERE id(s) = $section_id
                CREATE (p)-[:CONTAINS]->(s)
                """
                self.graph.execute_query(rel_query, {
                    "part_id": self.created_nodes["part_3"],
                    "section_id": section_id
                })
                stats["relationships"] += 1

            # Create Article for this section
            article_stats = self._create_article_from_section(
                section_number, section_content, section_id, extracted_clauses, self.section_sentences
            )

            stats["articles"] += article_stats.get("articles", 0)
            stats["sentences"] += article_stats.get("sentences", 0)
            stats["clauses"] += article_stats.get("clauses", 0)
            stats["requirements"] += article_stats.get("requirements", 0)
            stats["components"] += article_stats.get("components", 0)
            stats["relationships"] += article_stats.get("relationships", 0)

            # Generate and add summary to Section node
            if section_number in self.section_sentences:
                summary = self._generate_summary(self.section_sentences[section_number])
                if summary:
                    update_query = """
                    MATCH (s:Section {id: $id})
                    SET s.summary = $summary
                    """
                    self.graph.execute_query(update_query, {
                        "id": f"SEC_{section_number}",
                        "summary": summary
                    })

        return stats

    def _create_article_from_section(
        self, section_number: str, section_content: str, section_id: str,
        extracted_clauses: List[Dict[str, Any]], section_sentences_map: Dict
    ) -> Dict[str, int]:
        """Create Article node from section and process its clauses"""
        stats = {
            "articles": 0,
            "sentences": 0,
            "clauses": 0,
            "requirements": 0,
            "components": 0,
            "relationships": 0
        }

        # Generate embedding for article content
        article_embedding = self.embedding_manager.embed_text(section_content)

        # Create Article node with full text and embedding
        article_query = """
        CREATE (a:Article {
            id: $id,
            number: $number,
            subsection: $subsection,
            title: $title,
            full_reference: $number,
            effective_date: date("2024-01-01"),
            text: $text,
            embedding: $embedding
        })
        RETURN id(a) as id
        """
        result = self.graph.execute_query(article_query, {
            "id": f"ART_{section_number}",
            "number": section_number,
            "subsection": section_number.rsplit(".", 1)[0] if "." in section_number else section_number,
            "title": section_number,
            "text": section_content,  # Store full text, no truncation
            "embedding": article_embedding
        })
        article_id = result[0]["id"] if result else None

        if article_id:
            stats["articles"] += 1
            self.article_sentences[section_number] = []

            # Link Section to Article
            rel_query = """
            MATCH (s) WHERE id(s) = $section_id
            MATCH (a) WHERE id(a) = $article_id
            CREATE (s)-[:CONTAINS]->(a)
            """
            self.graph.execute_query(rel_query, {
                "section_id": section_id,
                "article_id": article_id
            })
            stats["relationships"] += 1

            # Create Sentence nodes and process clauses
            for clause_idx, clause in enumerate(extracted_clauses):
                clause_stats = self._process_clause(clause, article_id, clause_idx,
                                                    section_number, section_sentences_map, self.article_sentences)

                stats["sentences"] += clause_stats.get("sentences", 0)
                stats["clauses"] += clause_stats.get("clauses", 0)
                stats["requirements"] += clause_stats.get("requirements", 0)
                stats["components"] += clause_stats.get("components", 0)
                stats["relationships"] += clause_stats.get("relationships", 0)

            # Generate and add summary to Article node
            if section_number in self.article_sentences:
                summary = self._generate_summary(self.article_sentences[section_number])
                if summary:
                    update_query = """
                    MATCH (a:Article {id: $id})
                    SET a.summary = $summary
                    """
                    self.graph.execute_query(update_query, {
                        "id": f"ART_{section_number}",
                        "summary": summary
                    })

        return stats

    def _process_clause(
        self, clause: Dict[str, Any], article_id: str, clause_idx: int,
        section_number: str = None, section_sentences_map: Dict = None,
        article_sentences_map: Dict = None
    ) -> Dict[str, int]:
        """Process a clause and create Sentence, Clause, and Requirement nodes"""
        stats = {
            "sentences": 0,
            "clauses": 0,
            "requirements": 0,
            "components": 0,
            "relationships": 0
        }

        clause_number = clause.get("number", f"({clause_idx + 1})")
        clause_text = clause.get("text", "")

        if not clause_text:
            return stats

        # Generate embedding for sentence text
        sentence_embedding = self.embedding_manager.embed_text(clause_text)

        # Create Sentence node with embedding
        sentence_query = """
        CREATE (sent:Sentence {
            id: $id,
            article: $article,
            number: $number,
            text: $text,
            embedding: $embedding,
            is_requirement: true,
            is_exception: false
        })
        RETURN id(sent) as id
        """
        result = self.graph.execute_query(sentence_query, {
            "id": f"SENT_{article_id}_{clause_idx}",
            "article": article_id,
            "number": clause_idx,
            "text": clause_text,
            "embedding": sentence_embedding
        })
        sentence_id = result[0]["id"] if result else None

        if sentence_id:
            stats["sentences"] += 1

            # Track sentence for summary generation
            if section_sentences_map and section_number:
                section_sentences_map[section_number].append(clause_text)
            if article_sentences_map and section_number:
                article_sentences_map[section_number].append(clause_text)

            # Link Article to Sentence
            rel_query = """
            MATCH (a) WHERE id(a) = $article_id
            MATCH (sent) WHERE id(sent) = $sentence_id
            CREATE (a)-[:CONTAINS]->(sent)
            """
            self.graph.execute_query(rel_query, {
                "article_id": article_id,
                "sentence_id": sentence_id
            })
            stats["relationships"] += 1

            # Create Clause nodes for main and nested items
            clause_stats = self._create_clause_nodes(
                clause, sentence_id, article_id
            )

            stats["clauses"] += clause_stats.get("clauses", 0)
            stats["requirements"] += clause_stats.get("requirements", 0)
            stats["components"] += clause_stats.get("components", 0)
            stats["relationships"] += clause_stats.get("relationships", 0)

        return stats

    def _create_clause_nodes(
        self, clause: Dict[str, Any], sentence_id: str, article_id: str
    ) -> Dict[str, int]:
        """Create Clause nodes and extract requirements"""
        stats = {
            "clauses": 0,
            "requirements": 0,
            "components": 0,
            "relationships": 0
        }

        clause_number = clause.get("number", "")
        clause_text = clause.get("text", "")

        if not clause_text:
            return stats

        # Generate embedding for clause text
        clause_embedding = self.embedding_manager.embed_text(clause_text)

        # Create main clause node with embedding
        clause_query = """
        CREATE (c:Clause {
            id: $id,
            sentence_id: $sentence_id,
            letter: $letter,
            text: $text,
            embedding: $embedding
        })
        RETURN id(c) as id
        """
        result = self.graph.execute_query(clause_query, {
            "id": f"CLAUSE_{sentence_id}_{clause_number}",
            "sentence_id": sentence_id,
            "letter": clause_number,
            "text": clause_text,
            "embedding": clause_embedding
        })
        clause_id = result[0]["id"] if result else None

        if clause_id:
            stats["clauses"] += 1

            # Link Sentence to Clause
            rel_query = """
            MATCH (sent) WHERE id(sent) = $sentence_id
            MATCH (c) WHERE id(c) = $clause_id
            CREATE (sent)-[:CONTAINS]->(c)
            """
            self.graph.execute_query(rel_query, {
                "sentence_id": sentence_id,
                "clause_id": clause_id
            })
            stats["relationships"] += 1

            # Extract and create requirements from clause text
            req_stats = self._extract_requirements(clause_text, clause_id)
            stats["requirements"] += req_stats.get("requirements", 0)
            stats["components"] += req_stats.get("components", 0)
            stats["relationships"] += req_stats.get("relationships", 0)

            # Process nested items (subclauses)
            nested_items = clause.get("nested_items", [])
            for nested_item in nested_items:
                nested_stats = self._create_nested_clause(nested_item, clause_id)
                stats["clauses"] += nested_stats.get("clauses", 0)
                stats["relationships"] += nested_stats.get("relationships", 0)

        return stats

    def _create_nested_clause(
        self, nested_item: Dict[str, Any], parent_clause_id: str
    ) -> Dict[str, int]:
        """Create nested clause nodes (subclauses, items)"""
        stats = {
            "clauses": 0,
            "relationships": 0
        }

        item_number = nested_item.get("number", "")
        item_text = nested_item.get("text", "")

        if not item_text:
            return stats

        # Generate embedding for nested clause text
        nested_embedding = self.embedding_manager.embed_text(item_text)

        # Create nested clause node with embedding
        clause_query = """
        CREATE (c:Clause {
            id: $id,
            sentence_id: $parent_id,
            letter: $letter,
            text: $text,
            embedding: $embedding
        })
        RETURN id(c) as id
        """
        result = self.graph.execute_query(clause_query, {
            "id": f"CLAUSE_{parent_clause_id}_{item_number}",
            "parent_id": parent_clause_id,
            "letter": item_number,
            "text": item_text,
            "embedding": nested_embedding
        })
        nested_id = result[0]["id"] if result else None

        if nested_id:
            stats["clauses"] += 1

            # Link parent clause to nested clause
            rel_query = """
            MATCH (parent) WHERE id(parent) = $parent_id
            MATCH (c) WHERE id(c) = $nested_id
            CREATE (parent)-[:CONTAINS]->(c)
            """
            self.graph.execute_query(rel_query, {
                "parent_id": parent_clause_id,
                "nested_id": nested_id
            })
            stats["relationships"] += 1

        return stats

    def _extract_requirements(self, text: str, clause_id: str) -> Dict[str, int]:
        """Extract and create requirement nodes from clause text"""
        stats = {
            "requirements": 0,
            "components": 0,
            "relationships": 0
        }

        # Simple pattern matching for common requirements
        import re

        # Look for fire rating requirements
        fire_rating_match = re.search(r'(\d+)\s*min(?:ute)?s?\s+(?:fire.?)?(?:rating|resistance)', text, re.IGNORECASE)
        if fire_rating_match:
            rating_value = fire_rating_match.group(1)

            # Create Requirement node
            req_query = """
            CREATE (r:Requirement {
                id: $id,
                type: "fire_resistance_rating",
                value: $value,
                unit: "minutes",
                description: $description,
                is_minimum: true,
                is_maximum: false
            })
            RETURN id(r) as id
            """
            result = self.graph.execute_query(req_query, {
                "id": f"REQ_FIRE_{rating_value}",
                "value": rating_value,
                "description": f"Fire-resistance rating not less than {rating_value} min"
            })
            req_id = result[0]["id"] if result else None

            if req_id:
                stats["requirements"] += 1

                # Link clause to requirement
                rel_query = """
                MATCH (c) WHERE id(c) = $clause_id
                MATCH (r) WHERE id(r) = $req_id
                CREATE (c)-[:SPECIFIES_REQUIREMENT]->(r)
                """
                self.graph.execute_query(rel_query, {
                    "clause_id": clause_id,
                    "req_id": req_id
                })
                stats["relationships"] += 1

        return stats

    def _generate_summary(self, sentences: List[str]) -> Optional[str]:
        """
        Generate a summary of sentences by extracting key information.

        Summarization strategy:
        1. Extract key concepts (numbers, technical terms, actions)
        2. Remove redundancy
        3. Create concise description
        """
        if not sentences:
            return None

        # Combine all sentences
        full_text = " ".join(sentences)

        # Extract key information
        key_terms = set()

        # Find numbers (requirements, dimensions)
        numbers = re.findall(r'\d+(?:\.\d+)?', full_text)
        if numbers:
            key_terms.add(f"values: {', '.join(set(numbers[:5]))}")  # First 5 unique numbers

        # Find key technical terms (uppercase words, common code terms)
        technical_terms = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b', full_text)
        if technical_terms:
            key_terms.add(f"applies to: {', '.join(set(technical_terms[:3]))}")  # First 3 unique terms

        # Find action verbs (shall, must, may, should)
        if "shall" in full_text.lower():
            key_terms.add("mandatory")
        elif "may" in full_text.lower():
            key_terms.add("optional")

        # Build summary
        summary_parts = []

        # Add sentence count
        summary_parts.append(f"Contains {len(sentences)} requirement(s)")

        # Add extracted key terms
        if key_terms:
            summary_parts.extend(sorted(key_terms))

        # Add first sentence as context (truncated)
        if sentences[0]:
            first_sentence = sentences[0][:100].strip()
            if len(sentences[0]) > 100:
                first_sentence += "..."
            summary_parts.append(f"Overview: {first_sentence}")

        return "; ".join(summary_parts)


def main():
    """Build OBC graph from extracted data"""
    import sys
    import logging

    logging.basicConfig(level=logging.INFO)

    # Load extracted data
    extracted_file = Path("data/extracted.json")
    if not extracted_file.exists():
        logger.error(f"Extracted file not found: {extracted_file}")
        return False

    with open(extracted_file, "r", encoding="utf-8") as f:
        extracted_data = json.load(f)

    # Build graph
    try:
        graph = GraphManager()
        builder = OBCGraphBuilder(graph)

        logger.info("Building OBC graph...")
        stats = builder.build(extracted_data)

        # Save stats
        output_file = Path("data/graph_build_stats.json")
        with open(output_file, "w") as f:
            json.dump(stats, f, indent=2)

        logger.info(f"Graph building complete: {json.dumps(stats, indent=2)}")
        graph.close()

        return stats["success"]

    except Exception as e:
        logger.error(f"Failed to build graph: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
