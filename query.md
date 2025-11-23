# OBC Neo4j Cypher Queries

A comprehensive guide to querying the Ontario Building Code graph database.

## Graph Overview

The OBC graph is organized hierarchically:
- **Regulation** - Top level document (Ontario Building Code)
- **Part** - Major sections of the code
- **Section** - Numbered sections (e.g., 9.5)
- **Subsection** - Subsections of sections (e.g., 9.5.1)
- **Clause** - Numbered clauses within subsections
- **Subclause** - Lettered items within clauses
- **Paragraph** - Paragraph content within the hierarchy
- **Article** - Article nodes for organizing content
- **Sentence** - Individual sentences
- **Table** - Tabular data

## Basic Exploration

### Get Graph Statistics
```cypher
MATCH (n)
RETURN labels(n)[0] as node_type, count(n) as count
ORDER BY count DESC
```

### Count All Nodes and Relationships
```cypher
MATCH (n)
WITH count(n) as total_nodes
MATCH ()-[r]->()
RETURN total_nodes, count(r) as total_relationships
```

### List All Parts
```cypher
MATCH (r:Regulation)-[:HAS_PART]->(p:Part)
RETURN p.ref, p.text
ORDER BY p.ref
```

## Section-Level Queries

### Get a Specific Section (e.g., Section 9.5)
```cypher
MATCH (s:Section {ref: "9.5"})
RETURN s
```

### Get All Subsections of a Section
```cypher
MATCH (s:Section {ref: "9.5"})-[:HAS_SUBSECTION]->(ss:Subsection)
RETURN ss.ref, ss.text
ORDER BY ss.ref
```

### Get Full Hierarchy for a Section
Shows section → subsections → clauses in one query:
```cypher
MATCH (s:Section {ref: "9.5"})-[:HAS_SUBSECTION]->(ss:Subsection)
MATCH (ss)-[:HAS_CLAUSE]->(c:Clause)
RETURN s.ref as section, ss.ref as subsection, c.ref as clause, c.text as clause_text
LIMIT 50
```

### Get All Content Under a Section (Flat)
```cypher
MATCH (s:Section {ref: "9.5"})-[*]->(n)
RETURN labels(n)[0] as type, n.ref as ref, n.text as text
ORDER BY n.ref
```

### Count Direct Children by Type
```cypher
MATCH (s:Section {ref: "9.5"})-[r]->(n)
RETURN type(r) as relationship, labels(n)[0] as node_type, count(n) as count
```

## Searching and Filtering

### Find Sections Containing Text (Case-Insensitive)
```cypher
MATCH (s:Section)-[*0..5]->(n)
WHERE n.text CONTAINS "fire"
RETURN DISTINCT s.ref as section, labels(n)[0] as found_in_type, n.text as content
LIMIT 20
```

### Find Clauses Mentioning a Specific Topic
```cypher
MATCH (c:Clause)
WHERE c.text CONTAINS "requirement"
RETURN c.ref, c.text
LIMIT 20
```

### Find All Subsections of a Section That Contain Specific Text
```cypher
MATCH (s:Section {ref: "9.5"})-[:HAS_SUBSECTION]->(ss:Subsection)
WHERE ss.text CONTAINS "material"
RETURN ss.ref, ss.text
```

## Cross-Reference Analysis

### Find References from a Section
```cypher
MATCH (s:Section {ref: "9.5"})-[:REFERENCES]->(target)
RETURN DISTINCT target.ref as referenced_section
ORDER BY target.ref
```

### Find Which Sections Reference a Specific Section
```cypher
MATCH (source)-[:REFERENCES]->(s:Section {ref: "1.2"})
RETURN DISTINCT source.ref as referencing_section
```

### Show All Cross-References as a Network
```cypher
MATCH (s1:Section)-[:REFERENCES]->(s2:Section)
RETURN s1.ref as from_section, s2.ref as to_section
ORDER BY s1.ref, s2.ref
LIMIT 50
```

### Find Circular References
```cypher
MATCH (s1:Section)-[:REFERENCES]->(s2:Section)-[:REFERENCES*]->(s1)
RETURN DISTINCT s1.ref as circular_section,
  [s IN nodes(path) | s.ref][1:-1] as path_through
```

## Hierarchy Navigation

### Get Parent Section of a Subsection
```cypher
MATCH (ss:Subsection {ref: "9.5.1"})<-[:HAS_SUBSECTION]-(s:Section)
RETURN s.ref as parent_section
```

### Get Entire Path from Regulation Down to a Node
```cypher
MATCH path = (r:Regulation)-[*]->(n)
WHERE n.ref = "9.5"
RETURN [node IN nodes(path) | node.ref] as path,
  [rel IN relationships(path) | type(rel)] as relationships
```

### Get All Descendants of a Node (Any Depth)
```cypher
MATCH (s:Section {ref: "9.5"})-[*]->(descendant)
RETURN labels(descendant)[0] as type, count(DISTINCT descendant) as count
```

### Count Nodes at Each Level Under a Section
```cypher
MATCH (s:Section {ref: "9.5"})-[r*..6]->(n)
WITH length(r) as depth, labels(n)[0] as type
RETURN depth, type, count(n) as count
ORDER BY depth, type
```

## Structural Analysis

### Find the Deepest Nesting Level in the Graph
```cypher
MATCH path = (r:Regulation)-[*]->(n)
WITH length(path) - 1 as depth, max(length(path) - 1) as max_depth
MATCH path2 = (r2:Regulation)-[*]->(n2)
WHERE length(path2) - 1 = max_depth
RETURN max_depth, [node IN nodes(path2) | labels(node)[0]][0..3] as sample_path
LIMIT 1
```

### Find Nodes with the Most Relationships
```cypher
MATCH (n)
WITH n, size((n)-[]->()) + size((n)<-[]-()) as relationship_count
RETURN labels(n)[0] as type, n.ref as ref, relationship_count
ORDER BY relationship_count DESC
LIMIT 20
```

### Find Orphaned Nodes (No Relationships)
```cypher
MATCH (n)
WHERE size((n)-[]-()) = 0 AND NOT n:Regulation
RETURN labels(n)[0] as type, n.ref as ref, n.text
```

## Content Analysis

### Get Word Count Statistics
```cypher
MATCH (n:Clause)
WHERE n.text IS NOT NULL
WITH split(n.text, ' ') as words
RETURN labels(n)[0] as type,
  count(n) as clause_count,
  round(avg(size(words))) as avg_words,
  min(size(words)) as min_words,
  max(size(words)) as max_words
```

### Find the Longest Clause
```cypher
MATCH (c:Clause)
WITH c, length(c.text) as text_length
ORDER BY text_length DESC
RETURN c.ref, text_length, substring(c.text, 0, 200) as excerpt
LIMIT 10
```

### Find Empty or Very Short Content
```cypher
MATCH (n)
WHERE (n.text IS NULL OR length(trim(n.text)) < 10)
AND NOT n:Regulation
RETURN labels(n)[0] as type, n.ref as ref, n.text
LIMIT 30
```

## Relationship Queries

### Show All Relationship Types and Counts
```cypher
MATCH ()-[r]->()
RETURN type(r) as relationship_type, count(r) as count
ORDER BY count DESC
```

### Get Statistics by Relationship Type
```cypher
MATCH (source)-[r]->(target)
RETURN type(r) as rel_type,
  labels(source)[0] as source_type,
  labels(target)[0] as target_type,
  count(r) as count
ORDER BY count DESC
```

### Find Direct Containment Relationships
```cypher
MATCH (parent)-[r:HAS_*]->(child)
RETURN type(r) as relationship,
  labels(parent)[0] as parent_type,
  labels(child)[0] as child_type,
  count(r) as count
ORDER BY count DESC
```

## Specific Use Cases

### Get Everything in Section 9.5 (Original Query)
```cypher
MATCH (s:Section {ref: "9.5"})-[:HAS_SUBSECTION]->(ss:Subsection)
WITH s, ss, count(ss) as subsection_count
MATCH (s)-[*]->(a)
WHERE (a:Article OR a:Sentence)
WITH s, subsection_count, count(DISTINCT a) as article_count
RETURN {
  section: s.ref,
  subsections: subsection_count,
  articles: article_count,
  text: s.text
} as result
```

### Find All Requirements (Keyword Search)
```cypher
MATCH (n)
WHERE n.text CONTAINS "shall" OR n.text CONTAINS "must" OR n.text CONTAINS "required"
RETURN labels(n)[0] as type, n.ref as ref, n.text
LIMIT 50
```

### Get Summary Statistics for a Section
```cypher
MATCH (s:Section {ref: "9.5"})
MATCH (s)-[:HAS_SUBSECTION]->(ss)
WITH s, count(ss) as num_subsections
MATCH (s)-[*]->(n:Clause)
WITH s, num_subsections, count(n) as num_clauses
MATCH (s)-[:REFERENCES]->(ref)
RETURN {
  section: s.ref,
  subsections: num_subsections,
  clauses: num_clauses,
  references_to_other_sections: count(ref),
  text: substring(s.text, 0, 150)
} as summary
```

### Find Sections with Most Internal Structure
```cypher
MATCH (s:Section)-[*]->(n)
WITH s, count(DISTINCT n) as descendant_count
ORDER BY descendant_count DESC
RETURN s.ref, descendant_count, s.text
LIMIT 20
```

## Query Performance Tips

1. **Always filter early**: Use `WHERE` clauses close to `MATCH` statements
2. **Limit results**: Use `LIMIT` when exploring large result sets
3. **Use specific refs**: Querying by `ref` is faster than text searches
4. **Index your searches**: For frequently searched text, consider creating indexes

### Create Indexes (Run Once)
```cypher
CREATE INDEX section_ref IF NOT EXISTS FOR (s:Section) ON (s.ref);
CREATE INDEX subsection_ref IF NOT EXISTS FOR (ss:Subsection) ON (ss.ref);
CREATE INDEX clause_ref IF NOT EXISTS FOR (c:Clause) ON (c.ref);
CREATE INDEX text_search IF NOT EXISTS FOR (n) ON (n.text);
```

## Utility Queries

### Clear the Database (Use with Caution)
```cypher
MATCH (n) DETACH DELETE n
```

### Export All Nodes as JSON
```cypher
MATCH (n)
RETURN collect({
  type: labels(n)[0],
  ref: n.ref,
  text: n.text,
  id: id(n)
}) as nodes
```

### Create a Backup View of All Sections
```cypher
MATCH (s:Section)
RETURN collect({
  ref: s.ref,
  text: s.text
}) as all_sections
```

### Find Nodes Modified/Created After a Certain Date
```cypher
MATCH (n)
WHERE n.createdAt > datetime('2024-01-01T00:00:00Z')
RETURN labels(n)[0] as type, n.ref as ref, n.createdAt as created
ORDER BY n.createdAt DESC
```

## Interactive Exploration Pattern

Use this pattern to explore the graph interactively:

```cypher
-- Step 1: Find your starting point
MATCH (s:Section {ref: "9.5"})
RETURN s

-- Step 2: See what's directly connected
MATCH (s:Section {ref: "9.5"})-[r]->(connected)
RETURN type(r) as relationship, labels(connected)[0] as node_type, count(connected) as count

-- Step 3: Explore deeper
MATCH (s:Section {ref: "9.5"})-[:HAS_SUBSECTION]->(ss)
RETURN ss.ref, substring(ss.text, 0, 100) as preview

-- Step 4: Search within results
MATCH (s:Section {ref: "9.5"})-[*]->(n)
WHERE n.text CONTAINS "your_search_term"
RETURN labels(n)[0] as found_in, n.ref as ref, n.text
```

## Common Patterns

### Pattern 1: Navigate from Parent to All Children
```cypher
MATCH (parent {ref: "9.5"})-[*]->(children)
RETURN children
```

### Pattern 2: Find Bi-directional Relationships
```cypher
MATCH (a)-[r]->(b)
WHERE (b)-[r2]->(a)
RETURN a.ref as first, type(r) as forward, type(r2) as backward, b.ref as second
```

### Pattern 3: Find Paths Between Two Nodes
```cypher
MATCH path = shortestPath((s1:Section {ref: "9.5"})-[*]-(s2:Section {ref: "1.2"}))
RETURN [node IN nodes(path) | node.ref] as path
```

### Pattern 4: Aggregate and Group Results
```cypher
MATCH (s:Section)-[:HAS_SUBSECTION]->(ss)
WITH s.ref as section, count(ss) as subsection_count
ORDER BY subsection_count DESC
RETURN section, subsection_count
```

---

## Notes

- All queries assume refs are unique identifiers
- Text searches are case-sensitive by default; use `toLower()` for case-insensitive searches
- For large result sets, paginate using `SKIP` and `LIMIT`
- Performance degrades with very deep traversals; test with `LIMIT` first
- The graph contains approximately 17,000+ nodes and 20,000+ relationships
