# Semantic Extraction Workflow

Extends the OBC hierarchical knowledge graph with semantic concept nodes that enable intelligent, topic-based retrieval for **building code compliance checking**.

---

## Purpose

The primary use case is compliance checking — given a description of a building condition (e.g. *"the window in the 2nd floor bedroom is 50cm wide"*), retrieve all relevant OBC sentences and let an LLM determine whether it complies.

The semantic layer solves the core retrieval problem: **finding the right sentences in a 2000+ node graph without reading everything**.

---

## Architecture

```
WORKFLOW 1: regex_html_generator_divB.py
  HTML → Parse → Create OBC nodes (Division, Part, Section, ..., Sentence)

           ↓  (run after Workflow 1 is complete)

WORKFLOW 2: run_semantic_extraction.py
  Read Sentences → LLM extract concepts → Create semantic nodes → Link to Sentences

           ↓  (at query time)

WORKFLOW 3: Backend retrieval
  User query → Extract concepts → Scored graph query → Context expansion → LLM answer
```

Workflow 2 is **read-only with respect to existing data** — it only creates new nodes and relationships and never modifies or deletes any OBC node.

---

## Semantic Node Types

Five semantic node types are added to the graph, each capturing a different dimension of meaning.

### 1. Topic
Regulatory subjects addressed by the text.

```
(:Topic {
  name:        "WindowsDoorsAndSkylights",
  description: "Performance and installation requirements for glazed openings and entryways",
  keywords:    ["safety glass", "skylight", "window area", "door swing", "air leakage"]
})
```

Full topic list (21 topics):
OccupancyClassification, FireSeparationsAndClosures, MeansOfEgress, FireAlarmAndDetection, StructuralDesignAndLoads, ExcavationAndFoundations, WoodConstruction, MasonryAndConcrete, EnvironmentalSeparation, HVACSystems, PlumbingSystems, SewageSystems, WindowsDoorsAndSkylights, StairsRampsAndGuards, Accessibility, SoundTransmission, PublicPoolsAndSpas, FarmBuildings, RoomDimensions, FireResistanceRatings, LightingAndNaturalLight

---

### 2. BuildingType
The classification of building the text applies to.

```
(:BuildingType {
  name:      "Residential",
  obc_parts: ["9"],
  keywords:  ["house", "dwelling", "apartment", "home"]
})
```

Values: Residential, Industrial, Commercial, Assembly, Institutional, MixedUse

---

### 3. OccupancyClass
OBC occupancy group classifications.

```
(:OccupancyClass {
  code:        "C",
  name:        "Group C - Residential",
  description: "Residential occupancies including dwelling units"
})
```

Values: A (Assembly), B (Institutional), C (Residential), D (Business), E (Mercantile), F (Industrial)

---

### 4. ConstructionType
Building construction methods referenced by the text.

```
(:ConstructionType {
  name:     "Combustible",
  keywords: ["combustible", "wood", "frame", "wood-frame"]
})
```

Values: Combustible, NonCombustible, FireResistive, HeavyTimber

---

### 5. SpaceType
Specific spaces or rooms addressed by the text.

```
(:SpaceType {
  name:     "Bedroom",
  keywords: ["bedroom", "sleeping room", "sleeping area", "dormitory"]
})
```

Full space type list (18 types):
Basement, CrawlSpace, Attic, ServiceRoom, PublicCorridor, ExitStairway, StorageGarage, Kitchen, Bathroom, Bedroom, LivingArea, Mezzanine, Suite, UniversalWashroom, HighHazardRoom, Lobby, Vestibule, Balcony

---

## Graph After Workflow 2

```
(:Topic {name: "WindowsDoorsAndSkylights"}) ─┐
(:Topic {name: "RoomDimensions"})            ─┤─[:APPLIES_TO]→ (:Sentence {ref: "9.7.2.1.(1)"})
(:BuildingType {name: "Residential"})        ─┤
(:SpaceType {name: "Bedroom"})               ─┘
```

One sentence can link to many semantic nodes. One semantic node links to many sentences.

---

## File Structure

```
Ingestion/
├── ingestion/
│   ├── semantic/
│   │   ├── semantic_schema.py         # Node definitions and seed data
│   │   ├── semantic_extractor.py      # LLM extraction logic
│   │   ├── semantic_linker.py         # Neo4j read/write operations
│   │   └── run_semantic_extraction.py # Main pipeline script
│   └── shared/
│       └── config/
│           └── settings.py            # Reads .env for Neo4j + LLM config
└── SEMANTIC_EXTRACTION.md             # This document
```

---

## Configuration

Reads from `.env` (App/App/backend/.env takes priority):

| Variable | Description |
|----------|-------------|
| `NEO4J_URI` | Neo4j connection URI |
| `NEO4J_USER` | Neo4j username |
| `NEO4J_PASSWORD` | Neo4j password |
| `NEO4J_DATABASE` | Database name (default: neo4j) |
| `USE_API` | LLM provider: `groq`, `openai`, or `anthropic` |
| `API_KEY` | API key for the chosen provider |

---

## Running the Pipeline

All commands run from `Ingestion/`:

```bash
# 1. Dry run — extract concepts for 10 sentences but don't write to Neo4j
python3 -m ingestion.semantic.run_semantic_extraction --limit 10 --dry-run

# 2. Test run — process 50 sentences and write to Neo4j
python3 -m ingestion.semantic.run_semantic_extraction --limit 50

# 3. Full run — processes all unlinked sentences (~71 min at Groq free tier)
python3 -m ingestion.semantic.run_semantic_extraction

# 4. Reprocess — re-extract sentences that were already linked
python3 -m ingestion.semantic.run_semantic_extraction --reprocess
```

The pipeline is **idempotent** — running it multiple times will not create duplicate nodes or relationships because all writes use `MERGE`.

---

## LLM Extraction Detail

For each Sentence node, the extractor sends a prompt like:

```
Analyze this Ontario Building Code text and extract relevant semantic categories.

TEXT (Reference: 9.7.2.1.(1)):
"Every bedroom shall have a window or windows with a total openable
 area of not less than 0.35 m²"

CATEGORIES TO EXTRACT:
1. Topics — Valid options: WindowsDoorsAndSkylights, RoomDimensions, ...
2. Building Types — Valid options: Residential, Industrial, ...
...

Return ONLY a valid JSON object.
```

Response:

```json
{
  "topics": ["WindowsDoorsAndSkylights", "RoomDimensions"],
  "building_types": ["Residential"],
  "occupancy_classes": ["C"],
  "construction_types": [],
  "space_types": ["Bedroom"]
}
```

The extractor validates every returned value against the predefined lists in `semantic_schema.py` and silently discards any that are invalid.

---

## Retrieval Strategy (Query Time)

### The problem with AND queries

A strict AND intersection misses relevant sentences. For example, a general window requirement tagged `WindowsDoorsAndSkylights + LivingArea` would be missed when querying for `Bedroom`, even though it applies to all habitable rooms including bedrooms.

### Solution: Score by concept matches, expand context

**Step 1 — Scored OR query** (semantic nodes get us to the right neighbourhood):

```cypher
MATCH (s:Sentence)
WHERE EXISTS {
    MATCH (sem)-[:APPLIES_TO]->(s)
    WHERE (sem:Topic AND sem.name IN ["WindowsDoorsAndSkylights", "RoomDimensions"])
    OR (sem:BuildingType AND sem.name = "Residential")
    OR (sem:SpaceType AND sem.name IN ["Bedroom", "LivingArea"])
}
WITH s,
  [(sem)-[:APPLIES_TO]->(s) | sem.name] AS matched
RETURN s.ref, s.text, size(matched) AS score
ORDER BY score DESC
LIMIT 30
```

Sentences matching more concepts rank higher. Nothing is dropped by strict filtering.

**Step 2 — Context expansion** (pull siblings and tables so the LLM has complete rules):

```cypher
MATCH (a:Article)-[:HAS_SENTENCE]->(s:Sentence)
WHERE s.ref IN $retrieved_refs
OPTIONAL MATCH (s)-[:HAS_TABLE]->(t:Table)
OPTIONAL MATCH (a)-[:HAS_SENTENCE]->(sibling:Sentence)
RETURN s, collect(DISTINCT t) AS tables, collect(DISTINCT sibling) AS siblings
```

This is critical for compliance checking — OBC sentences frequently say *"except as provided in Sentence (2)..."* or *"shall conform to Table 9.7.2.1"*. Without pulling siblings and tables, the LLM gets half the rule.

**Step 3 — Vector similarity re-ranking** (rank the ~30 candidates by closeness to the original query):

The semantic layer narrows 2000+ sentences to ~30 candidates. Vector similarity then re-ranks those candidates by embedding distance to the user query. This handles nuance the semantic layer can't — synonyms, implied topics, and edge cases.

**Step 4 — LLM compliance answer**:

The LLM receives the top-ranked sentences with their full article context and tables, and determines whether the described condition complies, citing specific references.

---

## What the Semantic Layer Does and Does Not Cover

### Covered
- Finding sentences by regulatory topic, building type, occupancy, space type
- Cross-part retrieval (fire requirements in both Part 3 and Part 9 for the same topic)
- Synonym handling (all window-related sentences under one `WindowsDoorsAndSkylights` node)

### Not covered (handled by LLM at query time)
- **Storey/floor level** — "2nd floor" vs "ground floor" distinctions are not in the schema
- **Measurement comparison** — whether 50cm meets a minimum is reasoning, not retrieval
- **Compliance direction** — whether a requirement is a minimum, maximum, or prohibition
- **Cross-references** — handled by context expansion (siblings + tables), not semantic nodes

---

## Verifying the Results

After running extraction, check progress with:

```cypher
// How many sentences are linked?
MATCH (sem)-[:APPLIES_TO]->(s:Sentence)
RETURN labels(sem)[0] AS semantic_type, count(*) AS links
ORDER BY links DESC

// Which topics have the most coverage?
MATCH (t:Topic)-[:APPLIES_TO]->(s:Sentence)
RETURN t.name AS topic, count(s) AS sentence_count
ORDER BY sentence_count DESC

// Spot check a compliance scenario — windows in bedrooms
MATCH (s:Sentence)
WHERE EXISTS { MATCH (t:Topic {name: "WindowsDoorsAndSkylights"})-[:APPLIES_TO]->(s) }
AND EXISTS { MATCH (st:SpaceType {name: "Bedroom"})-[:APPLIES_TO]->(s) }
RETURN s.ref, s.text
```

---

## Extending the Schema

To add a new semantic node type or concept:

1. Add seed data to the appropriate list in `semantic_schema.py`
2. The extractor prompt will automatically include it in the valid options
3. Re-run the pipeline with `--reprocess` to re-evaluate already-linked sentences
