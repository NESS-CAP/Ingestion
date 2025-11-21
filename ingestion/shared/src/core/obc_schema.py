"""
Ontario Building Code - Neo4j Graph Schema

Comprehensive schema for OBC compliance checking with:
- Hierarchical code structure (Division → Part → Section → Article → Sentence → Clause)
- Classification nodes (OccupancyGroup, ConstructionType, BuildingClassification)
- Requirement and Component nodes
- Cross-references and standards
- Proposal evaluation for compliance checking
"""


def create_obc_schema():
    """Define the complete OBC Neo4j schema"""

    return {
        "node_types": {
            # Code Structure Nodes
            "Division": {
                "properties": {
                    "id": "string (unique)",
                    "name": "string",
                    "description": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": []
            },
            "Part": {
                "properties": {
                    "id": "string (unique)",
                    "number": "integer",
                    "name": "string",
                    "division": "string",
                    "description": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(number)", "(division)"]
            },
            "Section": {
                "properties": {
                    "id": "string (unique)",
                    "number": "string",
                    "part": "integer",
                    "name": "string",
                    "description": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(number)", "(part)"]
            },
            "Subsection": {
                "properties": {
                    "id": "string (unique)",
                    "number": "string",
                    "section": "string",
                    "name": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(number)"]
            },
            "Article": {
                "properties": {
                    "id": "string (unique)",
                    "number": "string",
                    "subsection": "string",
                    "title": "string",
                    "full_reference": "string",
                    "effective_date": "date",
                    "text": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(number)", "(subsection)"]
            },
            "Sentence": {
                "properties": {
                    "id": "string (unique)",
                    "article": "string",
                    "number": "integer",
                    "text": "string",
                    "is_requirement": "boolean",
                    "is_exception": "boolean"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": []
            },
            "Clause": {
                "properties": {
                    "id": "string (unique)",
                    "sentence_id": "string",
                    "letter": "string",
                    "text": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": []
            },

            # Classification Nodes
            "OccupancyGroup": {
                "properties": {
                    "id": "string (unique)",
                    "group": "string",
                    "name": "string",
                    "description": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(group)"]
            },
            "OccupancyDivision": {
                "properties": {
                    "id": "string (unique)",
                    "group": "string",
                    "division": "integer",
                    "name": "string",
                    "description": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(group, division)"]
            },
            "ConstructionType": {
                "properties": {
                    "id": "string (unique)",
                    "name": "string",
                    "description": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": []
            },
            "BuildingClassification": {
                "properties": {
                    "id": "string (unique)",
                    "article_reference": "string",
                    "occupancy_group": "string",
                    "occupancy_division": "integer",
                    "max_storeys": "integer",
                    "max_building_area": "float",
                    "permitted_construction": "list<string>"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(article_reference)", "(occupancy_group, occupancy_division, max_storeys)"]
            },

            # Requirement Nodes
            "Requirement": {
                "properties": {
                    "id": "string (unique)",
                    "type": "string",
                    "value": "string",
                    "unit": "string",
                    "description": "string",
                    "is_minimum": "boolean",
                    "is_maximum": "boolean"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(type)"]
            },
            "Component": {
                "properties": {
                    "id": "string (unique)",
                    "name": "string",
                    "category": "string",
                    "description": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(name)", "(category)"]
            },

            # Reference Nodes
            "StandardReference": {
                "properties": {
                    "id": "string (unique)",
                    "organization": "string",
                    "standard_number": "string",
                    "title": "string",
                    "year": "integer"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": []
            },
            "SupplementaryStandard": {
                "properties": {
                    "id": "string (unique)",
                    "number": "string",
                    "title": "string",
                    "description": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": []
            },

            # Objective and Functional Statement Nodes
            "Objective": {
                "properties": {
                    "id": "string (unique)",
                    "code": "string",
                    "category": "string",
                    "description": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(code)"]
            },
            "FunctionalStatement": {
                "properties": {
                    "id": "string (unique)",
                    "code": "string",
                    "description": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(code)"]
            },

            # Proposal Evaluation Nodes
            "BuildingProposal": {
                "properties": {
                    "id": "string (unique)",
                    "project_name": "string",
                    "address": "string",
                    "submission_date": "date",
                    "status": "string (enum: under_review, approved, rejected, requires_revision)"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(status)", "(submission_date)"]
            },
            "ProposedBuilding": {
                "properties": {
                    "id": "string (unique)",
                    "proposal_id": "string",
                    "occupancy_group": "string",
                    "occupancy_division": "integer",
                    "storeys_above_grade": "integer",
                    "storeys_below_grade": "integer",
                    "building_area": "float",
                    "building_height": "float",
                    "construction_type": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(proposal_id)"]
            },
            "ComplianceCheck": {
                "properties": {
                    "id": "string (unique)",
                    "proposal_id": "string",
                    "requirement_id": "string",
                    "status": "string (enum: compliant, non_compliant, needs_review)",
                    "checked_date": "datetime",
                    "checked_by": "string",
                    "notes": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(proposal_id)", "(status)"]
            },

            # Optional: Condition and Exception nodes
            "Condition": {
                "properties": {
                    "id": "string (unique)",
                    "parameter": "string",
                    "operator": "string (>, <, =, >=, <=)",
                    "value": "string",
                    "unit": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": []
            },
            "Exception": {
                "properties": {
                    "id": "string (unique)",
                    "article": "string",
                    "description": "string"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": []
            },
            "CodeVersion": {
                "properties": {
                    "id": "string (unique)",
                    "year": "integer",
                    "effective_date": "date",
                    "is_current": "boolean"
                },
                "constraints": ["id IS UNIQUE"],
                "indexes": ["(is_current)"]
            }
        },

        "relationships": {
            # Hierarchical Structure
            "CONTAINS": {
                "from": ["Division", "Part", "Section", "Subsection", "Article", "Sentence", "Clause"],
                "to": ["Part", "Section", "Subsection", "Article", "Sentence", "Clause", "Clause"],
                "properties": {
                    "sequence": "integer"
                }
            },

            # Cross-References
            "REFERENCES": {
                "from": ["Article"],
                "to": ["Article"],
                "properties": {
                    "context": "string (exception, clarification, etc.)",
                    "description": "string"
                }
            },
            "REQUIRES_STANDARD": {
                "from": ["Article"],
                "to": ["StandardReference"]
            },
            "REFERENCES_SB": {
                "from": ["Article"],
                "to": ["SupplementaryStandard"]
            },

            # Classification Relationships
            "HAS_DIVISION": {
                "from": ["OccupancyGroup"],
                "to": ["OccupancyDivision"]
            },
            "FOR_OCCUPANCY": {
                "from": ["BuildingClassification"],
                "to": ["OccupancyDivision"]
            },
            "DEFINED_IN": {
                "from": ["BuildingClassification"],
                "to": ["Article"]
            },
            "PERMITS_CONSTRUCTION": {
                "from": ["BuildingClassification"],
                "to": ["ConstructionType"]
            },

            # Requirements
            "SPECIFIES_REQUIREMENT": {
                "from": ["Sentence", "Clause"],
                "to": ["Requirement"],
                "properties": {
                    "applies_to": "string",
                    "condition": "string"
                }
            },
            "APPLIES_TO": {
                "from": ["Requirement"],
                "to": ["Component"]
            },
            "REQUIRES_IF": {
                "from": ["Requirement"],
                "to": ["Requirement"],
                "properties": {
                    "condition": "string"
                }
            },
            "HAS_CONDITION": {
                "from": ["Requirement"],
                "to": ["Condition"]
            },

            # Objectives and Functional Statements
            "SATISFIES_OBJECTIVE": {
                "from": ["Article"],
                "to": ["Objective"]
            },
            "ADDRESSES_FUNCTION": {
                "from": ["Article"],
                "to": ["FunctionalStatement"]
            },
            "SUPPORTED_BY": {
                "from": ["Objective"],
                "to": ["FunctionalStatement"]
            },

            # Proposal Relationships
            "PROPOSES": {
                "from": ["BuildingProposal"],
                "to": ["ProposedBuilding"]
            },
            "CHECKED_AGAINST": {
                "from": ["ProposedBuilding"],
                "to": ["Article"]
            },
            "HAS_COMPLIANCE_CHECK": {
                "from": ["ProposedBuilding"],
                "to": ["ComplianceCheck"]
            },
            "EVALUATES_REQUIREMENT": {
                "from": ["ComplianceCheck"],
                "to": ["Requirement"]
            },
            "REFERENCES_ARTICLE": {
                "from": ["ComplianceCheck"],
                "to": ["Article"]
            },
            "CLASSIFIED_AS": {
                "from": ["ProposedBuilding"],
                "to": ["OccupancyDivision"]
            },
            "USES_CONSTRUCTION": {
                "from": ["ProposedBuilding"],
                "to": ["ConstructionType"]
            },

            # Version Control
            "VERSION": {
                "from": ["Article", "Requirement", "Clause"],
                "to": ["CodeVersion"]
            },

            # Exceptions
            "HAS_EXCEPTION": {
                "from": ["Article"],
                "to": ["Exception"]
            }
        },

        "indexes_and_constraints": {
            "constraints": [
                "CREATE CONSTRAINT division_id IF NOT EXISTS FOR (n:Division) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT part_id IF NOT EXISTS FOR (n:Part) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT article_id IF NOT EXISTS FOR (n:Article) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT occupancy_group_id IF NOT EXISTS FOR (n:OccupancyGroup) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT requirement_id IF NOT EXISTS FOR (n:Requirement) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT proposal_id IF NOT EXISTS FOR (n:BuildingProposal) REQUIRE n.id IS UNIQUE"
            ],
            "indexes": [
                "CREATE INDEX part_number IF NOT EXISTS FOR (p:Part) ON (p.number)",
                "CREATE INDEX article_number IF NOT EXISTS FOR (a:Article) ON (a.number)",
                "CREATE INDEX occupancy_group IF NOT EXISTS FOR (o:OccupancyGroup) ON (o.group)",
                "CREATE INDEX requirement_type IF NOT EXISTS FOR (r:Requirement) ON (r.type)",
                "CREATE INDEX proposal_status IF NOT EXISTS FOR (p:BuildingProposal) ON (p.status)",
                "CREATE INDEX component_name IF NOT EXISTS FOR (c:Component) ON (c.name)",
                "CREATE COMPOSITE INDEX building_classification_lookup IF NOT EXISTS FOR (bc:BuildingClassification) ON (bc.occupancy_group, bc.occupancy_division, bc.max_storeys)",
                "CREATE COMPOSITE INDEX article_hierarchy IF NOT EXISTS FOR (a:Article) ON (a.subsection)",
                "CREATE FULLTEXT INDEX article_text_search IF NOT EXISTS FOR (n:Article|Sentence|Clause) ON EACH [n.text, n.title]"
            ]
        }
    }


def get_node_creation_cypher(node_type: str, node_data: dict) -> str:
    """Generate Cypher for creating a node of given type"""

    templates = {
        "Division": """CREATE (d:Division {{
            id: $id,
            name: $name,
            description: $description
        }})""",
        "Part": """CREATE (p:Part {{
            id: $id,
            number: $number,
            name: $name,
            division: $division,
            description: $description
        }})""",
        "Article": """CREATE (a:Article {{
            id: $id,
            number: $number,
            subsection: $subsection,
            title: $title,
            full_reference: $full_reference,
            effective_date: $effective_date,
            text: $text
        }})""",
        "Sentence": """CREATE (s:Sentence {{
            id: $id,
            article: $article,
            number: $number,
            text: $text,
            is_requirement: $is_requirement,
            is_exception: $is_exception
        }})""",
        "Clause": """CREATE (c:Clause {{
            id: $id,
            sentence_id: $sentence_id,
            letter: $letter,
            text: $text
        }})""",
        "OccupancyGroup": """CREATE (og:OccupancyGroup {{
            id: $id,
            group: $group,
            name: $name,
            description: $description
        }})""",
        "OccupancyDivision": """CREATE (od:OccupancyDivision {{
            id: $id,
            group: $group,
            division: $division,
            name: $name,
            description: $description
        }})""",
        "Requirement": """CREATE (r:Requirement {{
            id: $id,
            type: $type,
            value: $value,
            unit: $unit,
            description: $description,
            is_minimum: $is_minimum,
            is_maximum: $is_maximum
        }})""",
        "Component": """CREATE (c:Component {{
            id: $id,
            name: $name,
            category: $category,
            description: $description
        }})""",
        "BuildingProposal": """CREATE (bp:BuildingProposal {{
            id: $id,
            project_name: $project_name,
            address: $address,
            submission_date: $submission_date,
            status: $status
        }})""",
        "ProposedBuilding": """CREATE (pb:ProposedBuilding {{
            id: $id,
            proposal_id: $proposal_id,
            occupancy_group: $occupancy_group,
            occupancy_division: $occupancy_division,
            storeys_above_grade: $storeys_above_grade,
            storeys_below_grade: $storeys_below_grade,
            building_area: $building_area,
            building_height: $building_height,
            construction_type: $construction_type
        }})""",
        "ComplianceCheck": """CREATE (cc:ComplianceCheck {{
            id: $id,
            proposal_id: $proposal_id,
            requirement_id: $requirement_id,
            status: $status,
            checked_date: $checked_date,
            checked_by: $checked_by,
            notes: $notes
        }})"""
    }

    return templates.get(node_type, "")


if __name__ == "__main__":
    schema = create_obc_schema()
    import json
    print(json.dumps(schema, indent=2))
