"""
OBC HTML Ingestion - Two Pass Strategy

Pass 1: Build graph hierarchy using ONLY HTML class names
  - Parse HTML and extract by CSS class selectors
  - Create nodes: Regulation → Division → Part → Section → Subsection → Clause → Subclause → Item
  - Create CONTAINS relationships based on HTML structure
  - NO text analysis, NO cross-reference resolution

Pass 2: Create relationships by reading node content
  - Read each node's text content
  - Find references to other sections/clauses
  - Create REFERENCES and RELATED_TO relationships
"""

import logging
from typing import Dict, List, Any, Optional, Set, Tuple
import json
from pathlib import Path
import re
import sys
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parents[4]))

from ingestion.src.core.graph_manager import GraphManager
from ingestion.config.schemas import create_obc_schema

logger = logging.getLogger(__name__)


class OBCHTMLParser:
    """Parse OBC HTML using class-based hierarchy"""

    def __init__(self, html_content: str):
        self.soup = BeautifulSoup(html_content, 'html.parser')
        self.regulation_name = self._extract_regulation_name()

    def _extract_regulation_name(self) -> str:
        """Extract regulation title (e.g., 'O. Reg. 332/12')"""
        # Try to find regulation number in the page
        reg_elem = self.soup.find(class_='regnumber-e')
        if reg_elem:
            return reg_elem.get_text(strip=True)
        return "Ontario Building Code"

    def extract_hierarchy(self) -> Dict[str, Any]:
        """
        Extract entire document hierarchy using HTML class names.

        Follows reference script pattern:
        - Iterate top-level children
        - Track parent context (section, subsection, etc.)
        - Each <p> becomes a node; continuation paragraphs append to current parent

        Returns hierarchy structure with all nodes and their relationships.
        """
        from bs4 import NavigableString

        logger.info("Pass 1: Extracting hierarchy from HTML class names")

        # Find main content container
        content = self.soup.find(class_='act-content')
        if not content:
            logger.error("Could not find act-content container")
            return {
                "regulation": {
                    "name": self.regulation_name,
                    "number": "O. Reg. 332/12",
                    "description": "Ontario Building Code"
                },
                "nodes": [],
                "relationships": []
            }

        # Find the actual document content - look for WordSection1 or fall back to act-content
        word_section = self.soup.find(class_='WordSection1')
        if word_section:
            content = word_section
            logger.info(f"Found WordSection1 with {len(list(content.children))} children")
        else:
            # Fallback to original approach
            doc_content = content.find(class_='laws-document__act-content')
            if doc_content:
                content = doc_content
                logger.info(f"Found laws-document__act-content with {len(list(content.children))} children")

        # Current parent tracking
        current_section = None
        current_subsection = None
        parent_stack = []  # Stack to track parent nodes

        nodes = []  # All nodes to create
        relationships = []  # All relationships
        node_counter = 0  # Counter for unique node IDs

        # Iterate top-level children
        for elem in content.children:
            if isinstance(elem, NavigableString):
                continue

            # Skip non-paragraph elements
            if elem.name != 'p':
                continue

            classes = elem.get('class', [])
            text = elem.get_text(' ', strip=True)
            if not text:
                continue

            # Determine node type based on classes
            node_type = "Paragraph"
            if 'partnum-e' in classes:
                node_type = "Part"
            elif 'ruleb-e' in classes:
                node_type = "Section"
            elif 'section' in classes:
                node_type = "Subsection"
            elif 'subsection-e' in classes:
                node_type = "Clause"
            elif 'clause-e' in classes:
                node_type = "Clause"
            elif 'subclause-e' in classes:
                node_type = "Subclause"

            # Create unique reference for this node
            node_id = f"node_{node_counter}"
            node_counter += 1

            node = {
                "type": node_type,
                "ref": node_id,
                "text": text,
                "html_class": " ".join(classes)
            }
            nodes.append(node)

            # Track parent relationships based on node type hierarchy
            if node_type in ("Part", "Section"):
                # Part and Section are top-level, clear the stack
                parent_stack = [node_id]
                current_section = node_id
            elif node_type in ("Subsection", "Clause"):
                # These go under sections
                if current_section:
                    if current_section not in [rel["target"] for rel in relationships if rel["type"] == "CONTAINS_SUBSECTION"]:
                        relationships.append({
                            "source": current_section,
                            "target": node_id,
                            "type": "CONTAINS_SUBSECTION" if node_type == "Subsection" else "CONTAINS_CLAUSE"
                        })
                    else:
                        relationships.append({
                            "source": current_section,
                            "target": node_id,
                            "type": "CONTAINS_SUBSECTION" if node_type == "Subsection" else "CONTAINS_CLAUSE"
                        })
            elif node_type == "Paragraph":
                # Paragraphs go under their immediate parent
                if current_section:
                    relationships.append({
                        "source": current_section,
                        "target": node_id,
                        "type": "CONTAINS_PARAGRAPH"
                    })

        logger.info(f"Extracted {len(nodes)} nodes from HTML")

        return {
            "regulation": {
                "name": self.regulation_name,
                "number": "O. Reg. 332/12",
                "description": "Ontario Building Code"
            },
            "nodes": nodes,
            "relationships": relationships
        }

    def _extract_section(self, section_elem) -> Optional[Dict[str, Any]]:
        """Extract section with all its subsections, clauses, etc."""
        # Get section number and text
        section_text = section_elem.get_text(strip=True)

        # Parse section number (e.g., "3.2.2")
        section_match = re.match(r'^([\d.]+)\s*[.:-]?\s*(.*)$', section_text[:100])
        if not section_match:
            return None

        section_num = section_match.group(1)
        section_title = section_match.group(2) if len(section_match.group(2)) > 0 else ""

        # Extract subsections
        subsections = self._extract_subsections(section_elem)

        return {
            "number": section_num,
            "title": section_title,
            "text": section_text,
            "subsections": subsections,
            "total_subsections": len(subsections)
        }

    def _extract_subsections(self, parent_elem) -> List[Dict[str, Any]]:
        """Extract subsections from parent element"""
        subsections = []

        # Look for subsection markers in parent element
        for elem in parent_elem.find_all(class_='Ssubsection-e'):
            subsection_text = elem.get_text(strip=True)

            # Parse subsection number
            subsection_match = re.match(r'^([\d.]+)\s*[.:-]?\s*(.*)$', subsection_text[:100])
            if subsection_match:
                subsection_num = subsection_match.group(1)

                # Extract clauses from this subsection
                clauses = self._extract_clauses(elem)

                subsections.append({
                    "number": subsection_num,
                    "text": subsection_text,
                    "clauses": clauses,
                    "total_clauses": len(clauses)
                })

        return subsections

    def _extract_clauses(self, parent_elem) -> List[Dict[str, Any]]:
        """Extract clauses (numbered items) from parent element"""
        clauses = []

        # Split by clause delimiters: (1), (2), (3), etc.
        roman_pattern = r'(?:xx|xix|xviii|xvii|xvi|xv|xiv|xiii|xii|xi|x|ix|viii|vii|vi|v|iv|iii|ii|i)'
        clause_pattern = f'(?=\\((?:\\d+(?:\\.\\d+)?|{roman_pattern}(?:\\.\\d+)?)\\))'

        text = parent_elem.get_text(strip=True)
        clause_splits = re.split(clause_pattern, text)

        for clause_text in clause_splits:
            clause_text = clause_text.strip()
            if not clause_text:
                continue

            # Extract clause number
            clause_match = re.match(
                f'\\((\\d+(?:\\.\\d+)?|{roman_pattern}(?:\\.\\d+)?)\\)(.*)',
                clause_text,
                re.DOTALL
            )

            if clause_match:
                clause_num = clause_match.group(1)
                clause_content = clause_match.group(2).strip()

                clauses.append({
                    "number": f"({clause_num})",
                    "text": clause_content[:500],  # Store first 500 chars
                    "full_text": clause_content
                })

        return clauses


class OBCNeo4jIngester:
    """Ingest OBC hierarchy into Neo4j using two-pass strategy"""

    def __init__(self, graph: GraphManager):
        self.graph = graph
        self.schema = create_obc_schema()

    def ingest_pass1(self, hierarchy: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pass 1: Build hierarchy using HTML class names only.
        Creates all nodes and relationships using the extracted hierarchy.
        """
        logger.info("Pass 1: Ingesting hierarchy into Neo4j")

        stats = {
            "nodes_created": {},
            "relationships_created": 0,
            "success": True
        }

        # Create regulation node
        reg = hierarchy["regulation"]
        self.graph.execute_query("""
            CREATE (r:Regulation {
                name: $name,
                number: $number,
                description: $description
            })
        """, {
            "name": reg["name"],
            "number": reg["number"],
            "description": reg["description"]
        })
        stats["nodes_created"]["Regulation"] = 1

        # Create all extracted nodes
        node_types_count = {}
        nodes = hierarchy.get("nodes", [])

        for node in nodes:
            node_type = node.get("type", "Paragraph")
            node_ref = node.get("ref", "")
            text = node.get("text", "")
            html_class = node.get("html_class", "")

            # Build properties dict based on node type
            props = {"ref": node_ref, "text": text}
            if "number" in node:
                props["number"] = node["number"]
            if html_class:
                props["html_class"] = html_class

            # Create the node
            prop_str = ", ".join(f"{k}: ${k}" for k in props.keys())
            self.graph.execute_query(f"""
                CREATE (n:{node_type} {{{prop_str}}})
            """, props)

            node_types_count[node_type] = node_types_count.get(node_type, 0) + 1

        stats["nodes_created"].update(node_types_count)

        # Create relationships between nodes
        relationships = hierarchy.get("relationships", [])
        rel_count = 0

        for rel in relationships:
            source_ref = rel.get("source", "")
            target_ref = rel.get("target", "")
            rel_type = rel.get("type", "REFERENCES")

            # Find the source and target nodes and create the relationship
            self.graph.execute_query(f"""
                MATCH (s {{ref: $source}})
                MATCH (t {{ref: $target}})
                MERGE (s)-[:{rel_type}]->(t)
            """, {
                "source": source_ref,
                "target": target_ref
            })
            rel_count += 1

        stats["relationships_created"] = rel_count

        logger.info(f"Pass 1 complete: {sum(node_types_count.values())} nodes and {rel_count} relationships created")

        return stats

    def ingest_pass2(self) -> Dict[str, Any]:
        """
        Pass 2: Create REFERENCES relationships by reading paragraph content.
        Identifies cross-references within paragraph text and links source section to target section.
        """
        logger.info("Pass 2: Creating cross-reference relationships from paragraph content")

        stats = {
            "references_created": 0,
            "paragraphs_analyzed": 0
        }

        # Get all paragraph nodes with their parent section
        paragraphs = self.graph.execute_query("""
            MATCH (s:Section)-[:CONTAINS_PARAGRAPH]->(p:Paragraph)
            RETURN s.ref as section_ref, s.number as section_num, p.text as text
        """)

        # Track which sections reference which sections (avoid duplicate relationships)
        section_pairs = set()

        # For each paragraph, look for references to other sections
        for para in paragraphs:
            para_text = para.get("text", "")
            section_ref = para.get("section_ref", "")
            section_num = para.get("section_num", "")

            if not para_text or not section_ref:
                stats["paragraphs_analyzed"] += 1
                continue

            # Find references to sections (e.g., "section 1.1.2", "Section 3.15", "Subsection 1.2.3")
            section_references = set()

            # Match "section X.Y.Z" or "Section X.Y.Z" pattern
            matches = re.findall(r'(?:section|Section|Subsection|subsection)\s+([\d.]+)', para_text, re.IGNORECASE)
            section_references.update(matches)

            # Also match standalone numbers that look like section numbers (e.g., "3.15.", "1.2.")
            matches = re.findall(r'(\d{1,2}\.\d{1,2}(?:\.\d+)?)\s*(?:of|and|or|,|\.|\s|$)', para_text)
            section_references.update(matches)

            # Create REFERENCES relationships
            for ref_section in section_references:
                if ref_section and ref_section != section_num:
                    section_pair = (section_ref, ref_section)
                    if section_pair not in section_pairs:
                        try:
                            result = self.graph.execute_query("""
                                MATCH (s1:Section {ref: $source})
                                MATCH (s2:Section {ref: $target})
                                MERGE (s1)-[:REFERENCES]->(s2)
                                RETURN 1
                            """, {
                                "source": section_ref,
                                "target": ref_section
                            })
                            if result:
                                stats["references_created"] += 1
                                section_pairs.add(section_pair)
                        except Exception as e:
                            # Reference target might not exist, that's ok
                            logger.debug(f"Could not create reference from {section_ref} to {ref_section}: {e}")

            stats["paragraphs_analyzed"] += 1

        logger.info(f"Pass 2 complete: {stats['references_created']} references created from {stats['paragraphs_analyzed']} paragraphs")

        return stats


def main():
    """Test OBC ingestion"""
    logging.basicConfig(level=logging.INFO)

    # Load reference HTML
    html_file = Path("ingestion/data/reference-elaw-html.html")
    if not html_file.exists():
        logger.error(f"HTML file not found: {html_file}")
        return False

    with open(html_file) as f:
        html_content = f.read()

    # Parse HTML
    parser = OBCHTMLParser(html_content)
    hierarchy = parser.extract_hierarchy()

    logger.info(f"Extracted hierarchy: {len(hierarchy['nodes'])} nodes from HTML")

    # Ingest to Neo4j
    graph = GraphManager()
    ingester = OBCNeo4jIngester(graph)

    # Pass 1: Build hierarchy
    stats1 = ingester.ingest_pass1(hierarchy)
    logger.info(f"Pass 1 stats: {stats1}")

    # Pass 2: Create relationships
    stats2 = ingester.ingest_pass2()
    logger.info(f"Pass 2 stats: {stats2}")

    # Save results
    results = {
        "pass1": stats1,
        "pass2": stats2,
        "total_nodes": sum(stats1["nodes_created"].values()) + 1,  # +1 for Regulation
        "total_relationships": stats1["relationships_created"] + stats2["references_created"]
    }

    output_file = Path("ingestion/data/obc_ingestion_stats.json")
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Ingestion complete. Results saved to {output_file}")

    graph.close()
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
