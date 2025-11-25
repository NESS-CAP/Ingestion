import re
import sys
from pathlib import Path
from bs4 import BeautifulSoup, NavigableString
from neo4j import GraphDatabase

def ensure_project_root_on_path():
    current = Path(__file__).resolve()
    project_root = next(
        (p for p in current.parents if (p / "ingestion" / "__init__.py").exists()),
        None,
    )
    if project_root and str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))


ensure_project_root_on_path()

from ingestion.shared.config.settings import NEO4J_CONFIG

# ==========================
# CONFIG
# ==========================

NEO4J_URI = NEO4J_CONFIG["uri"]
NEO4J_USER = NEO4J_CONFIG["user"]
NEO4J_PASSWORD = NEO4J_CONFIG["password"]
NEO4J_DATABASE = NEO4J_CONFIG.get("database")
SESSION_KWARGS = {"database": NEO4J_DATABASE} if NEO4J_DATABASE else {}

HTML_PATH = "./building_code_section9.5.html"

CODE_ID = "ON_BC_332_12"
CODE_TITLE = "Ontario Regulation 332/12 – Building Code"
CODE_JURISDICTION = "Ontario"
CREATED_BY = "du"

# ==========================
# REGEX PATTERNS
# ==========================

# Section heading: "Section 9.5. Design of Areas, Spaces and Doorways"
section_root_re = re.compile(r"^Section\s+(9\.\d+)\.\s*(.+)", re.IGNORECASE)

# Subsection headings: "9.5.1. General", "9.5.2. Barrier-Free Design", ...
subsection_re = re.compile(r"^(9\.5\.\d+)\.\s*(.+)")

# Article headings: "9.5.1.1. Application", "9.5.3.1. Ceiling Heights of Rooms or Spaces"
article_re = re.compile(r"^(\d+(?:\.\d+){3})\.?\s*(.+)")

# Sentences (within subsections): "(1) Some text ..."
sentence_re = re.compile(r"^\(?(\d+)\)?\s*(.+)")

# Clauses: "(a) some clause text"
clause_re = re.compile(r"^\(?([a-z])\)?\s*(.+)", re.IGNORECASE)

# Subclauses: "(i) some subclause text"
subclause_re = re.compile(r"^\(?([ivx]+)\)?\s*(.+)", re.IGNORECASE)

# Table heading: "Table 9.5.3.1. Room Ceiling Heights"
table_heading_re = re.compile(r"^Table\s+(\d+(?:\.\d+){3})\.\s*(.*)", re.IGNORECASE)

# ==========================
# NEO4J SCHEMA HELPERS
# ==========================

def init_constraints(driver):
    with driver.session() as session:
        session.run("""
        CREATE CONSTRAINT section_ref_unique IF NOT EXISTS
        FOR (s:Section)
        REQUIRE s.ref IS UNIQUE
        """)
        session.run("""
        CREATE CONSTRAINT subsection_ref_unique IF NOT EXISTS
        FOR (s:Subsection)
        REQUIRE s.ref IS UNIQUE
        """)
        session.run("""
        CREATE CONSTRAINT article_ref_unique IF NOT EXISTS
        FOR (a:Article)
        REQUIRE a.ref IS UNIQUE
        """)
        session.run("""
        CREATE CONSTRAINT sentence_ref_unique IF NOT EXISTS
        FOR (s:Sentence)
        REQUIRE s.ref IS UNIQUE
        """)
        session.run("""
        CREATE CONSTRAINT clause_ref_unique IF NOT EXISTS
        FOR (c:Clause)
        REQUIRE c.ref IS UNIQUE
        """)
        session.run("""
        CREATE CONSTRAINT subclause_ref_unique IF NOT EXISTS
        FOR (sc:Subclause)
        REQUIRE sc.ref IS UNIQUE
        """)
        session.run("""
        CREATE CONSTRAINT table_ref_unique IF NOT EXISTS
        FOR (t:Table)
        REQUIRE t.ref IS UNIQUE
        """)

def merge_section(tx, ref, title):
    tx.run("""
    MERGE (s:Section {ref: $ref})
    SET s.title = coalesce(s.title, $title),
        s.createdby = $created_by
    """, ref=ref, title=title.strip(), created_by=CREATED_BY)
    return ref

def merge_subsection(tx, section_ref, ref, title):
    tx.run("""
    MERGE (sub:Subsection {ref: $ref})
    SET sub.title = coalesce(sub.title, $title),
        sub.createdby = $created_by
    WITH sub
    MATCH (sec:Section {ref: $section_ref})
    MERGE (sec)-[:HAS_SUBSECTION]->(sub)
    """, ref=ref, title=title.strip(), section_ref=section_ref, created_by=CREATED_BY)
    return ref

def merge_article(tx, subsection_ref, ref, title):
    tx.run("""
    MERGE (a:Article {ref: $ref})
    SET a.title = coalesce(a.title, $title),
        a.createdby = $created_by
    WITH a
    MATCH (sub:Subsection {ref: $subsection_ref})
    MERGE (sub)-[:HAS_ARTICLE]->(a)
    """, ref=ref, title=title.strip(), subsection_ref=subsection_ref, created_by=CREATED_BY)
    return ref

def merge_sentence(tx, article_ref, number, text):
    sent_ref = f"{article_ref}.({number})"
    tx.run("""
    MERGE (s:Sentence {ref: $ref})
    SET s.text = $text,
        s.createdby = $created_by
    WITH s
    MATCH (a:Article {ref: $article_ref})
    MERGE (a)-[:HAS_SENTENCE]->(s)
    """, ref=sent_ref, text=text.strip(), article_ref=article_ref, created_by=CREATED_BY)
    return sent_ref

def append_to_sentence(tx, sentence_ref, extra_text):
    tx.run("""
    MATCH (s:Sentence {ref: $ref})
    SET s.text = coalesce(s.text, '') + ' ' + $extra
    """, ref=sentence_ref, extra=extra_text)

def merge_clause(tx, sentence_ref, letter, text):
    clause_ref = f"{sentence_ref}({letter})"
    tx.run("""
    MERGE (c:Clause {ref: $ref})
    SET c.text = $text,
        c.createdby = $created_by
    WITH c
    MATCH (s:Sentence {ref: $sentence_ref})
    MERGE (s)-[:HAS_CLAUSE]->(c)
    """, ref=clause_ref, text=text.strip(), sentence_ref=sentence_ref, created_by=CREATED_BY)
    return clause_ref

def merge_subclause(tx, clause_ref, roman, text):
    subclause_ref = f"{clause_ref}({roman})"
    tx.run("""
    MERGE (sc:Subclause {ref: $ref})
    SET sc.text = $text,
        sc.createdby = $created_by
    WITH sc
    MATCH (c:Clause {ref: $clause_ref})
    MERGE (c)-[:HAS_SUBCLAUSE]->(sc)
    """, ref=subclause_ref, text=text.strip(), clause_ref=clause_ref, created_by=CREATED_BY)
    return subclause_ref

def merge_table(tx, table_ref, title, text, notes, sentence_refs):
    tx.run("""
    MERGE (t:Table {ref: $ref})
    SET t.title = coalesce(t.title, $title),
        t.text  = $text,
        t.notes = $notes,
        t.createdby = $created_by
    WITH t
    UNWIND $sentence_refs AS sr
    MATCH (s:Sentence {ref: sr})
    MERGE (s)-[:HAS_TABLE]->(t)
    """, ref=table_ref, title=title.strip(), text=text.strip(),
         notes=notes.strip(), sentence_refs=sentence_refs, created_by=CREATED_BY)

def append_to_table(tx, table_ref, extra_note):
    tx.run("""
    MATCH (t:Table {ref: $ref})
    SET t.notes = coalesce(t.notes, '') + ' ' + $extra
    """, ref=table_ref, extra=extra_note)

# ==========================
# TABLE CONTEXT PARSING
# ==========================

def parse_sentence_refs_from_heading(text):
    """
    e.g. "Forming Part of Sentences 9.5.3.1.(1) and (2)"
         "Forming Part of Sentence 9.5.11.1.(1)"
    Returns list like ["9.5.3.1.(1)", "9.5.3.1.(2)"].
    """
    # Find first full "article.(n)" pattern
    m = re.search(r"(\d+(?:\.\d+){3})\.\((\d+)\)", text)
    if not m:
        return []
    article_ref = m.group(1)
    first_num = m.group(2)
    nums = set([first_num])
    # Additional "(n)" patterns (e.g. "and (2)")
    for n in re.findall(r"\((\d+)\)", text):
        nums.add(n)
    return [f"{article_ref}.({n})" for n in sorted(nums, key=int)]

def flatten_table_text(table_tag):
    """
    Flatten HTML <table> into a simple text blob:
    one row per line, cells separated by " | ".
    """
    rows_text = []
    for row in table_tag.find_all("tr"):
        cell_texts = []
        for cell in row.find_all("td"):
            # Concatenate all <p> inside the cell
            ps = cell.find_all("p")
            if ps:
                txt = " ".join(p.get_text(" ", strip=True) for p in ps)
            else:
                txt = cell.get_text(" ", strip=True)
            cell_texts.append(txt)
        if cell_texts:
            rows_text.append(" | ".join(cell_texts))
    return "\n".join(rows_text)

# ==========================
# MAIN PARSER
# ==========================

def parse_html_and_load(driver):
    with open(HTML_PATH, encoding="utf-8") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")
    
     # Try the normal full-code container
    section_div = soup.find("div", class_="WordSection1")

    # If missing (excerpt file), fall back to body or top-level div
    if not section_div:
        # Prefer the only top-level div if present
        top_divs = soup.find_all("div", recursive=False)
        if len(top_divs) == 1:
            section_div = top_divs[0]
        else:
            section_div = soup.body or soup

    current_section_ref = None
    current_subsection_ref = None
    current_article_ref = None
    current_sentence_ref = None
    last_clause_ref = None

    # For tables
    pending_table_meta = None  # {'ref': ..., 'title': ..., 'sentence_refs': [...]}
    last_table_ref = None      # for attaching notes

    with driver.session() as session:
        # Iterate only top-level children, so <p> inside <table> are handled separately
        for elem in section_div.children:
            if isinstance(elem, NavigableString):
                continue

            # TABLE HANDLING
            if elem.name == "table":
                if pending_table_meta:
                    table_ref = pending_table_meta["ref"]
                    title = pending_table_meta["title"]
                    sentence_refs = pending_table_meta["sentence_refs"]
                    table_text = flatten_table_text(elem)
                    notes = ""  # will be filled by following headnote / notes
                    session.execute_write(
                        merge_table,
                        table_ref,
                        title,
                        table_text,
                        notes,
                        sentence_refs,
                    )
                    last_table_ref = table_ref
                    pending_table_meta = None
                continue

            if elem.name != "p":
                continue

            classes = elem.get("class", [])
            text = elem.get_text(" ", strip=True)
            if not text:
                continue

            # TABLE HEADINGS / CONTEXT
            if "Pheading3-e" in classes:
                # e.g. "Table 9.5.3.1. Room Ceiling Heights"
                mt = table_heading_re.match(text)
                if mt:
                    table_ref = mt.group(1)  # "9.5.3.1"
                    title = mt.group(2) or ""
                    pending_table_meta = {
                        "ref": table_ref,
                        "title": title,
                        "sentence_refs": [],
                    }
                continue

            if "heading3-e" in classes and pending_table_meta:
                # e.g. "Forming Part of Sentences 9.5.3.1.(1) and (2)"
                sentence_refs = parse_sentence_refs_from_heading(text)
                pending_table_meta["sentence_refs"] = sentence_refs
                continue

            if "headnote-e" in classes or "paranoindt-e" in classes:
                # Notes to the last table
                if last_table_ref:
                    session.execute_write(
                        append_to_table,
                        last_table_ref,
                        text,
                    )
                continue

            # ========= STRUCTURE =========

            # 1) Section root: "Section 9.5. Design of Areas, Spaces and Doorways"
            if "ruleb-e" in classes:
                ms = section_root_re.match(text)
                if ms:
                    ref = ms.group(1)      # "9.5"
                    title = ms.group(2) or ""
                    current_section_ref = session.execute_write(
                        merge_section, ref, title
                    )
                    current_subsection_ref = None
                    current_article_ref = None
                    current_sentence_ref = None
                    last_clause_ref = None
                    continue

                # 2) Subsections: "9.5.1. General", "9.5.2. Barrier-Free Design", ...
                msub = subsection_re.match(text)
                if msub and current_section_ref:
                    ref = msub.group(1)    # "9.5.1"
                    title = msub.group(2) or ""
                    current_subsection_ref = session.execute_write(
                        merge_subsection, current_section_ref, ref, title
                    )
                    current_article_ref = None
                    current_sentence_ref = None
                    last_clause_ref = None
                    continue

                # Other ruleb-e in this section we can safely ignore
                continue

            # 3) Articles: <p class="section"><b>9.5.1.1. Application</b></p>
            if "section" in classes:
                ma = article_re.match(text)
                if ma and current_subsection_ref:
                    ref = ma.group(1)      # "9.5.1.1"
                    title = ma.group(2) or ""
                    current_article_ref = session.execute_write(
                        merge_article, current_subsection_ref, ref, title
                    )
                    current_sentence_ref = None
                    last_clause_ref = None
                    continue

            # 4) Sentences: <p class="subsection-e"><b>(1)</b> ...</p>
            if "subsection-e" in classes and current_article_ref:
                msent = sentence_re.match(text)
                if msent:
                    num = msent.group(1)      # "1", "2", ...
                    body = msent.group(2) or ""
                    current_sentence_ref = session.execute_write(
                        merge_sentence, current_article_ref, num, body
                    )
                    last_clause_ref = None
                    continue
                else:
                    # Un-numbered para inside a sentence → append
                    if current_sentence_ref:
                        session.execute_write(
                            append_to_sentence,
                            current_sentence_ref,
                            text,
                        )
                    continue

            # 5) Clauses: <p class="clause-e">(a) ...</p>
            if "clause-e" in classes and current_sentence_ref:
                mc = clause_re.match(text)
                if mc:
                    letter = mc.group(1).lower()
                    body = mc.group(2) or ""
                    last_clause_ref = session.execute_write(
                        merge_clause,
                        current_sentence_ref,
                        letter,
                        body,
                    )
                    continue
                else:
                    # Fallback: append to sentence
                    session.execute_write(
                        append_to_sentence, current_sentence_ref, text
                    )
                    continue

            # 6) Subclauses: <p class="subclause-e">(i) ...</p>
            if "subclause-e" in classes and last_clause_ref:
                ms = subclause_re.match(text)
                if ms:
                    roman = ms.group(1).lower()
                    body = ms.group(2) or ""
                    session.execute_write(
                        merge_subclause,
                        last_clause_ref,
                        roman,
                        body,
                    )
                    continue
                else:
                    # Fallback: append to sentence
                    if current_sentence_ref:
                        session.execute_write(
                            append_to_sentence, current_sentence_ref, text
                        )
                    continue

            # 7) Any other paragraph: if we are inside a sentence, append it
            if current_sentence_ref:
                session.execute_write(
                    append_to_sentence, current_sentence_ref, text
                )

        print("Finished parsing Section 9.5 HTML and loading graph.")


def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        #init_constraints(driver)
        parse_html_and_load(driver)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
