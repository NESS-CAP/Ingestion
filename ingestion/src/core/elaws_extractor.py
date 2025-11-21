"""
E-Laws OBC (O. Reg. 332/12) specialized extractor.

Handles extraction and parsing of E-Laws format with:
- Hierarchical structure: Regulation → Division → Part → Section → Clause → SubClause → Item
- Definitions extraction
- Cross-references and internal citations
- Conversion to graph nodes and relationships
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import json

logger = logging.getLogger(__name__)


@dataclass
class OBCNode:
    """Represents a node in the OBC schema"""
    node_id: str
    label: str
    properties: Dict[str, Any]


@dataclass
class OBCRelationship:
    """Represents a relationship in the OBC graph"""
    rel_type: str
    source_id: str
    target_id: str
    properties: Dict[str, Any] = None

    def __post_init__(self):
        if self.properties is None:
            self.properties = {}


class ELawsOBCExtractor:
    """
    Extracts structured data from E-Laws OBC PDF text.

    Parses the hierarchical E-Laws format and converts to graph schema.
    """

    def __init__(self):
        self.regulation_node = None
        self.nodes: List[OBCNode] = []
        self.relationships: List[OBCRelationship] = []
        self.node_map = {}  # Maps (type, number) to node_id for quick lookup
        self.division_stack = []  # Track current division for context
        self.part_stack = []     # Track current part for context
        self.section_stack = []  # Track current section for context
        self.clause_stack = []   # Track current clause for context

    def extract_from_text(self, text: str, regulation_id: str = "332/12", title: str = "Building Code") -> Dict[str, Any]:
        """
        Extract OBC structure from E-Laws text.

        Args:
            text: Raw text from E-Laws PDF
            regulation_id: Regulation identifier (default: 332/12)
            title: Regulation title (default: Building Code)

        Returns:
            Dictionary with nodes and relationships
        """
        # Create regulation node
        self._create_regulation_node(regulation_id, title)

        # Parse the text hierarchically
        lines = text.split('\n')
        self._parse_elaws_text(lines)

        return {
            "nodes": [asdict(node) for node in self.nodes],
            "relationships": [asdict(rel) for rel in self.relationships]
        }

    def extract_section_3_2_2(self, text: str) -> Dict[str, Any]:
        """
        Extract specifically section 3.2.2 and its subsections.

        Args:
            text: Text containing section 3.2.2

        Returns:
            Dictionary with nodes and relationships for section 3.2.2 and children
        """
        # Create regulation node
        self._create_regulation_node("332/12", "Building Code")

        # Find section 3.2.2 in the text - it starts with "3.2.2. "
        section_pattern = r'3\.2\.2\. Building Size and Construction Relative to Occupancy(.*?)(?=3\.\d\.\d\. |\Z)'
        matches = re.finditer(section_pattern, text, re.DOTALL)

        for match in matches:
            section_content = "3.2.2. Building Size and Construction Relative to Occupancy" + match.group(1)
            # Create division A if not exists
            div_id = self._ensure_division("A", "Compliance and Objectives")
            self.division_stack.append(div_id)

            # Create part 3 if not exists
            part_id = self._ensure_part("3", "Fire Protection, Occupant Safety and Accessibility")
            self.part_stack.append(part_id)

            # Parse section 3.2.2
            self._parse_section_322(section_content)

        return {
            "nodes": [asdict(node) for node in self.nodes],
            "relationships": [asdict(rel) for rel in self.relationships]
        }

    def _create_regulation_node(self, regulation_id: str, title: str) -> None:
        """Create root Regulation node"""
        reg_id = f"regulation_{regulation_id.replace('/', '_')}"
        node = OBCNode(
            node_id=reg_id,
            label="Regulation",
            properties={
                "regulation_id": regulation_id,
                "title": title,
                "abbreviation": "O. Reg. 332/12",
                "source_url": "https://www.ontario.ca/laws/regulation/120332"
            }
        )
        self.nodes.append(node)
        self.regulation_node = node
        self.node_map[("Regulation", regulation_id)] = reg_id

    def _ensure_division(self, division_id: str, title: str) -> str:
        """Ensure division exists, create if not"""
        key = ("Division", division_id)
        if key in self.node_map:
            return self.node_map[key]

        div_node_id = f"division_{division_id}"
        node = OBCNode(
            node_id=div_node_id,
            label="Division",
            properties={
                "division_id": division_id,
                "title": title
            }
        )
        self.nodes.append(node)
        self.node_map[key] = div_node_id

        # Link to regulation
        if self.regulation_node:
            rel = OBCRelationship(
                rel_type="HAS_DIVISION",
                source_id=self.regulation_node.node_id,
                target_id=div_node_id,
                properties={"sequence": ord(division_id) - ord('A')}
            )
            self.relationships.append(rel)

        return div_node_id

    def _ensure_part(self, part_number: str, title: str) -> str:
        """Ensure part exists under current division, create if not"""
        key = ("Part", part_number)
        if key in self.node_map:
            return self.node_map[key]

        part_node_id = f"part_{part_number}"
        node = OBCNode(
            node_id=part_node_id,
            label="Part",
            properties={
                "part_number": part_number,
                "title": title,
                "sequence": int(part_number)
            }
        )
        self.nodes.append(node)
        self.node_map[key] = part_node_id

        # Link to division (assume current division A)
        if self.division_stack:
            div_id = self.division_stack[-1]
        else:
            div_id = self._ensure_division("A", "Compliance and Objectives")

        rel = OBCRelationship(
            rel_type="HAS_PART",
            source_id=div_id,
            target_id=part_node_id,
            properties={"sequence": int(part_number)}
        )
        self.relationships.append(rel)

        return part_node_id

    def _ensure_section(self, section_number: str, title: str = "") -> str:
        """Ensure section exists under current part, create if not"""
        key = ("Section", section_number)
        if key in self.node_map:
            return self.node_map[key]

        section_node_id = f"section_{section_number.replace('.', '_')}"
        node = OBCNode(
            node_id=section_node_id,
            label="Section",
            properties={
                "section_number": section_number,
                "title": title,
                "sequence": self._calculate_sequence(section_number)
            }
        )
        self.nodes.append(node)
        self.node_map[key] = section_node_id

        # Link to part (assume current part 3)
        if self.part_stack:
            part_id = self.part_stack[-1]
        else:
            part_id = self._ensure_part("3", "Fire Protection, Occupant Safety and Accessibility")

        rel = OBCRelationship(
            rel_type="HAS_SECTION",
            source_id=part_id,
            target_id=section_node_id,
            properties={"sequence": self._calculate_sequence(section_number)}
        )
        self.relationships.append(rel)

        return section_node_id

    def _parse_section_322(self, section_text: str) -> None:
        """
        Parse section 3.2.2 content including clauses, subclauses, and items.

        E-Laws format (concatenated, no newlines between sections):
        3.2.2. Building Size and Construction Relative to Occupancy3.2.2.1. Application(1) Clause text...
        (a) SubClause text(i) Item text(ii) Item text(b) SubClause text...
        """
        section_number = "3.2.2"

        # Create section node
        section_title = "Building Size and Construction Relative to Occupancy"
        section_id = self._ensure_section(section_number, section_title)
        self.section_stack.append(section_id)

        # Split by subsection headers: 3.2.2.N. where N is a number
        # Use lookahead to split without consuming the pattern
        subsection_parts = re.split(r'(?=3\.2\.2\.(\d+)\.)', section_text)

        # Process each subsection
        current_clause_id = None
        current_subclause_id = None
        global_sequence = 0

        processed_subsections = set()  # Track which subsections we've processed to avoid duplicates

        for subsection_text in subsection_parts:
            if not subsection_text.strip():
                continue

            # Extract subsection number and title from the beginning
            match = re.match(r'3\.2\.2\.(\d+)\.\s*([^(]*?)(.*)$', subsection_text, re.DOTALL)
            if not match:
                continue

            subsection_num = match.group(1)

            # Skip duplicate subsections (they can appear multiple times in references)
            if subsection_num in processed_subsections:
                continue
            processed_subsections.add(subsection_num)

            subsection_title = match.group(2).strip()
            subsection_content = subsection_text

            # Create a sub-section node (e.g., 3.2.2.1)
            subsection_number = f"3.2.2.{subsection_num}"
            subsection_id = self._ensure_section(subsection_number, subsection_title)

            # Now parse clauses within this subsection
            # Pattern: (N) text where N is a digit
            clause_pattern = r'\((\d+)\)(.*?)(?=\((?:\d|[a-z]|[ivx]+)\)|3\.2\.2\.\d+\.|$)'

            clause_sequence = 0
            for clause_match in re.finditer(clause_pattern, subsection_content, re.DOTALL):
                clause_num = clause_match.group(1)
                clause_text = clause_match.group(2).strip()

                clause_number = f"{subsection_number}.({clause_num})"
                current_clause_id = self._create_clause(
                    clause_number, clause_text, subsection_id, clause_sequence
                )
                current_subclause_id = None
                clause_sequence += 1
                global_sequence += 1

                # Parse subclauses and items within this clause
                # Pattern: (a) text, (b) text, etc.
                subclause_pattern = r'\(([a-z])\)(.*?)(?=\((?:[a-z]|[ivx]+)\)|$)'

                subclause_sequence = 0
                for subclause_match in re.finditer(subclause_pattern, clause_text, re.DOTALL):
                    subclause_id_letter = subclause_match.group(1)
                    subclause_text = subclause_match.group(2).strip()

                    current_subclause_id = self._create_subclause(
                        subclause_id_letter, subclause_text, current_clause_id, subclause_sequence
                    )
                    subclause_sequence += 1
                    global_sequence += 1

                    # Parse items within this subclause
                    # Pattern: (i), (ii), (iii), etc.
                    item_pattern = r'\(([ivx]+)\)(.*?)(?=\((?:[ivx]+)\)|$)'

                    item_sequence = 0
                    for item_match in re.finditer(item_pattern, subclause_text, re.DOTALL):
                        item_id = item_match.group(1)
                        item_text = item_match.group(2).strip()

                        self._create_item(
                            item_id, item_text, current_subclause_id, item_sequence
                        )
                        item_sequence += 1
                        global_sequence += 1

    def _create_clause(self, clause_number: str, text: str, section_id: str, sequence: int) -> str:
        """Create a Clause node and link to section"""
        clause_id = f"clause_{clause_number.replace('.', '_').replace('(', '').replace(')', '')}"

        node = OBCNode(
            node_id=clause_id,
            label="Clause",
            properties={
                "clause_number": clause_number,
                "text": text,
                "sequence": sequence
            }
        )
        self.nodes.append(node)

        # Link to section
        rel = OBCRelationship(
            rel_type="HAS_CLAUSE",
            source_id=section_id,
            target_id=clause_id,
            properties={"sequence": sequence}
        )
        self.relationships.append(rel)

        return clause_id

    def _create_subclause(self, subclause_id: str, text: str, clause_id: str, sequence: int) -> str:
        """Create a SubClause node and link to clause"""
        sub_node_id = f"subclause_{subclause_id}"

        node = OBCNode(
            node_id=sub_node_id,
            label="SubClause",
            properties={
                "subclause_id": subclause_id,
                "text": text,
                "sequence": sequence
            }
        )
        self.nodes.append(node)

        # Link to clause
        rel = OBCRelationship(
            rel_type="HAS_SUBCLAUSE",
            source_id=clause_id,
            target_id=sub_node_id,
            properties={"sequence": sequence}
        )
        self.relationships.append(rel)

        return sub_node_id

    def _create_item(self, item_id: str, text: str, subclause_id: str, sequence: int) -> str:
        """Create an Item node and link to subclause"""
        item_node_id = f"item_{item_id}"

        node = OBCNode(
            node_id=item_node_id,
            label="Item",
            properties={
                "item_id": item_id,
                "text": text,
                "sequence": sequence
            }
        )
        self.nodes.append(node)

        # Link to subclause
        rel = OBCRelationship(
            rel_type="HAS_ITEM",
            source_id=subclause_id,
            target_id=item_node_id,
            properties={"sequence": sequence}
        )
        self.relationships.append(rel)

        return item_node_id

    def _calculate_sequence(self, number_string: str) -> int:
        """Convert section number like '3.2.2' to a sortable sequence"""
        try:
            parts = number_string.split('.')
            return sum(int(p) * (1000 ** (4 - i)) for i, p in enumerate(parts[:4]))
        except:
            return 0

    def _parse_elaws_text(self, lines: List[str]) -> None:
        """Parse full E-Laws text (not yet implemented)"""
        # This will parse the full document structure
        # For now, we focus on the extract_section_3_2_2 method
        pass
