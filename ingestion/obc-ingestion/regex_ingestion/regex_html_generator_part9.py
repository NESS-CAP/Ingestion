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

# IMPORTANT: now use the full Part 9 HTML (later you can point this to the full Code)
HTML_PATH = "./building_code_part9.html"

CODE_ID = "ON_BC_332_12"
CODE_TITLE = "Ontario Regulation 332/12 – Building Code"
CODE_JURISDICTION = "Ontario"
CREATED_BY = "du"

# ==========================
# REGEX PATTERNS
# ==========================

# Part heading: "Part 9  Housing and Small Buildings"
part_heading_re = re.compile(r"^Part\s+(\d+)\s*(.*)", re.IGNORECASE)

# Section heading: "Section 9.5. Design of Areas, Spaces and Doorways"
# (works for Section 9.1, 9.2, … as well)
section_root_re = re.compile(r"^Section\s+(\d+\.\d+)\.\s*(.+)", re.IGNORECASE)

# Subsection headings: "9.5.1. General", "9.3.2. Lumber and Wood Products", ...
# NOTE: this is now generic: any "9.x.y." style, not hardcoded to 9.5
subsection_re = re.compile(r"^(\d+\.\d+\.\d+)\.\s*(.+)")

# Article headings: "9.5.1.1. Application", "9.3.1.1. General", ...
article_re = re.compile(r"^(\d+(?:\.\d+){3})\.?\s*(.+)")

# Sentences (within articles): "(1) Some text ..."
sentence_re = re.compile(r"^\(?(\d+)\)?\s*(.+)")

# Clauses: "(a) some clause text"
clause_re = re.compile(r"^\(?([a-z])\)?\s*(.+)", re.IGNORECASE)

# Subclauses: "(i) some subclause text"
subclause_re = re.compile(r"^\(?([ivx]+)\)?\s*(.+)", re.IGNORECASE)

# Table heading: "Table 9.5.3.1. Room Ceiling Heights"
table_heading_re = re.compile(r"^Table\s+(\d+(?:\.\d+){3})\.\s*(.*)", re.IGNORECASE)

# Sentence refs in table heading:
# e.g. "Forming Part of Sentences 9.5.3.1.(1) and (2)"
sentence_ref_in_heading_re = re.compile(
    r"(\d+\.\d+\.\d+\.\d+)\.\((\d+)\)", re.IGNORECASE
)

# ==========================
# NEO4J SCHEMA HELPERS
# ==========================

def init_constraints(driver):
    with driver.session(**SESSION_KWARGS) as session:
        session.run("""
        CREATE CONSTRAINT part_ref_unique IF NOT EXISTS
        FOR (p:Part)
        REQUIRE p.ref IS UNIQUE
        """)
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


def merge_part(tx, ref, title):
    tx.run("""
    MERGE (p:Part {ref: $ref})
    SET p.title        = coalesce(p.title, $title),
        p.code_id      = $code_id,
        p.jurisdiction = $jurisdiction,
        p.createdby    = $created_by
    """, ref=ref, title=title.strip(), code_id=CODE_ID,
           jurisdiction=CODE_JURISDICTION, created_by=CREATED_BY)
    return ref


def merge_section(tx, ref, title):
    tx.run("""
    MERGE (s:Section {ref: $ref})
    SET s.title        = coalesce(s.title, $title),
        s.code_id      = $code_id,
        s.jurisdiction = $jurisdiction,
        s.createdby    = $created_by
    """, ref=ref, title=title.strip(), code_id=CODE_ID,
           jurisdiction=CODE_JURISDICTION, created_by=CREATED_BY)
    return ref


def link_part_to_section(tx, part_ref, section_ref):
    tx.run("""
    MATCH (p:Part {ref: $part_ref})
    MATCH (s:Section {ref: $section_ref})
    MERGE (p)-[:HAS_SECTION]->(s)
    """, part_ref=part_ref, section_ref=section_ref)


def merge_subsection(tx, section_ref, ref, title):
    tx.run("""
    MERGE (sub:Subsection {ref: $ref})
    SET sub.title        = coalesce(sub.title, $title),
        sub.code_id      = $code_id,
        sub.jurisdiction = $jurisdiction,
        sub.createdby    = $created_by
    WITH sub
    MATCH (sec:Section {ref: $section_ref})
    MERGE (sec)-[:HAS_SUBSECTION]->(sub)
    """, ref=ref, title=title.strip(), section_ref=section_ref,
           code_id=CODE_ID, jurisdiction=CODE_JURISDICTION,
           created_by=CREATED_BY)
    return ref


def merge_article(tx, subsection_ref, ref, title):
    tx.run("""
    MERGE (a:Article {ref: $ref})
    SET a.title        = coalesce(a.title, $title),
        a.code_id      = $code_id,
        a.jurisdiction = $jurisdiction,
        a.createdby    = $created_by
    WITH a
    MATCH (sub:Subsection {ref: $subsection_ref})
    MERGE (sub)-[:HAS_ARTICLE]->(a)
    """, ref=ref, title=title.strip(), subsection_ref=subsection_ref,
           code_id=CODE_ID, jurisdiction=CODE_JURISDICTION,
           created_by=CREATED_BY)
    return ref


def merge_sentence(tx, article_ref, num, text):
    sentence_ref = f"{article_ref}.({num})"
    tx.run("""
    MERGE (s:Sentence {ref: $ref})
    SET s.text         = coalesce(s.text, $text),
        s.code_id      = $code_id,
        s.jurisdiction = $jurisdiction,
        s.createdby    = $created_by
    WITH s
    MATCH (a:Article {ref: $article_ref})
    MERGE (a)-[:HAS_SENTENCE]->(s)
    """, ref=sentence_ref, text=text.strip(), article_ref=article_ref,
           code_id=CODE_ID, jurisdiction=CODE_JURISDICTION,
           created_by=CREATED_BY)
    return sentence_ref


def append_to_sentence(tx, sentence_ref, extra_text):
    tx.run("""
    MATCH (s:Sentence {ref: $ref})
    SET s.text = coalesce(s.text, '') + ' ' + $extra
    """, ref=sentence_ref, extra=extra_text.strip())


def merge_clause(tx, sentence_ref, letter, text):
    clause_ref = f"{sentence_ref}({letter})"
    tx.run("""
    MERGE (c:Clause {ref: $ref})
    SET c.text         = coalesce(c.text, $text),
        c.code_id      = $code_id,
        c.jurisdiction = $jurisdiction,
        c.createdby    = $created_by
    WITH c
    MATCH (s:Sentence {ref: $sentence_ref})
    MERGE (s)-[:HAS_CLAUSE]->(c)
    """, ref=clause_ref, text=text.strip(), sentence_ref=sentence_ref,
           code_id=CODE_ID, jurisdiction=CODE_JURISDICTION,
           created_by=CREATED_BY)
    return clause_ref


def merge_subclause(tx, clause_ref, roman, text):
    subclause_ref = f"{clause_ref}({roman})"
    tx.run("""
    MERGE (sc:Subclause {ref: $ref})
    SET sc.text        = coalesce(sc.text, $text),
        sc.code_id     = $code_id,
        sc.jurisdiction = $jurisdiction,
        sc.createdby   = $created_by
    WITH sc
    MATCH (c:Clause {ref: $clause_ref})
    MERGE (c)-[:HAS_SUBCLAUSE]->(sc)
    """, ref=subclause_ref, text=text.strip(), clause_ref=clause_ref,
           code_id=CODE_ID, jurisdiction=CODE_JURISDICTION,
           created_by=CREATED_BY)
    return subclause_ref


def merge_table(tx, table_ref, title, text, notes, sentence_refs):
    tx.run("""
    MERGE (t:Table {ref: $ref})
    SET t.title        = coalesce(t.title, $title),
        t.text         = $text,
        t.notes        = $notes,
        t.code_id      = $code_id,
        t.jurisdiction = $jurisdiction,
        t.createdby    = $created_by
    """, ref=table_ref, title=title.strip(), text=text.strip(),
           notes=notes.strip(), code_id=CODE_ID,
           jurisdiction=CODE_JURISDICTION, created_by=CREATED_BY)

    for sref in sentence_refs:
        tx.run("""
        MATCH (t:Table {ref: $table_ref})
        MATCH (s:Sentence {ref: $sentence_ref})
        MERGE (s)-[:HAS_TABLE]->(t)
        """, table_ref=table_ref, sentence_ref=sref)


def append_to_table(tx, table_ref, extra_note):
    tx.run("""
    MATCH (t:Table {ref: $ref})
    SET t.notes = coalesce(t.notes, '') + ' ' + $extra
    """, ref=table_ref, extra=extra_note.strip())


# ==========================
# TABLE HELPERS
# ==========================

def parse_sentence_refs_from_heading(text):
    """
    e.g. "Forming Part of Sentences 9.5.3.1.(1) and (2)"
    → ["9.5.3.1.(1)", "9.5.3.1.(2)"]
    """
    m = sentence_ref_in_heading_re.search(text)
    if not m:
        return []
    base = m.group(1)   # "9.5.3.1"
    first = m.group(2)  # "1"
    nums = {first}
    for n in re.findall(r"\((\d+)\)", text):
        nums.add(n)
    return [f"{base}.({n})" for n in sorted(nums, key=int)]


def flatten_table_text(table_tag):
    """
    Flatten an HTML <table> into plain text:
    - one row per line
    - cells separated by " | "
    """
    rows = []
    for tr in table_tag.find_all("tr"):
        cells = []
        for td in tr.find_all("td"):
            ps = td.find_all("p")
            if ps:
                txt = " ".join(p.get_text(" ", strip=True) for p in ps)
            else:
                txt = td.get_text(" ", strip=True)
            cells.append(txt)
        if cells:
            rows.append(" | ".join(cells))
    return "\n".join(rows)


# ==========================
# MAIN PARSER
# ==========================

def parse_html_and_load(driver):
    with open(HTML_PATH, encoding="utf-8") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")

    section_div = soup.find("div", class_="WordSection1")
    if not section_div:
        top_divs = soup.find_all("div", recursive=False)
        if len(top_divs) == 1:
            section_div = top_divs[0]
        else:
            section_div = soup

    with driver.session(**SESSION_KWARGS) as session:
        current_part_ref = None
        current_section_ref = None
        current_subsection_ref = None
        current_article_ref = None
        current_sentence_ref = None
        last_clause_ref = None

        pending_table_meta = None  # {'ref', 'title', 'sentence_refs'}
        last_table_ref = None

        for elem in section_div.descendants:
            if isinstance(elem, NavigableString):
                continue

            # Handle <table> body
            if elem.name == "table":
                if pending_table_meta:
                    table_ref = pending_table_meta["ref"]
                    title = pending_table_meta["title"]
                    srefs = pending_table_meta["sentence_refs"]
                    table_text = flatten_table_text(elem)
                    notes = ""
                    session.execute_write(
                        merge_table,
                        table_ref,
                        title,
                        table_text,
                        notes,
                        srefs,
                    )
                    last_table_ref = table_ref
                    pending_table_meta = None
                continue

            if elem.name not in ("p", "h1", "h2", "h3", "h4"):
                continue

            classes = elem.get("class") or []
            text = elem.get_text(" ", strip=True)
            if not text:
                continue

            # ========= PARTS =========
            if "partnum-e" in classes:
                mp = part_heading_re.match(text)
                if mp:
                    part_number = mp.group(1)     # "9"
                    remainder = mp.group(2) or ""
                    title = text if text else f"Part {part_number} {remainder}"
                    current_part_ref = session.execute_write(
                        merge_part, part_number, title
                    )
                    # reset lower levels
                    current_section_ref = None
                    current_subsection_ref = None
                    current_article_ref = None
                    current_sentence_ref = None
                    last_clause_ref = None
                continue

            # ========= TABLE CONTEXT =========

            # Table heading: "Table 9.5.3.1. ..."
            if "Pheading3-e" in classes:
                mt = table_heading_re.match(text)
                if mt:
                    table_ref = mt.group(1)
                    title = mt.group(2) or ""
                    pending_table_meta = {
                        "ref": table_ref,
                        "title": title,
                        "sentence_refs": [],
                    }
                continue

            # "Forming Part of Sentences 9.5.3.1.(1) and (2)"
            if "heading3-e" in classes and pending_table_meta:
                srefs = parse_sentence_refs_from_heading(text)
                pending_table_meta["sentence_refs"] = srefs
                continue

            # Notes for last table
            if ("headnote-e" in classes or "paranoindt-e" in classes) and last_table_ref:
                session.execute_write(
                    append_to_table,
                    last_table_ref,
                    text,
                )
                continue

            # ========= STRUCTURE =========

            # 1) Section roots and subsections share ruleb-e
            if "ruleb-e" in classes:
                m_sec = section_root_re.match(text)
                if m_sec:
                    sec_ref = m_sec.group(1)        # "9.1", "9.5", ...
                    sec_title = m_sec.group(2) or ""
                    current_section_ref = session.execute_write(
                        merge_section, sec_ref, sec_title
                    )
                    # connect to current Part
                    if current_part_ref:
                        session.execute_write(
                            link_part_to_section,
                            current_part_ref,
                            current_section_ref,
                        )
                    current_subsection_ref = None
                    current_article_ref = None
                    current_sentence_ref = None
                    last_clause_ref = None
                    continue

                # If not a "Section ..." heading, see if this is a subsection line: "9.3.1. Concrete"
                m_sub = subsection_re.match(text)
                if m_sub and current_section_ref:
                    sub_ref = m_sub.group(1)        # "9.3.1", "9.5.2", ...
                    sub_title = m_sub.group(2) or ""
                    current_subsection_ref = session.execute_write(
                        merge_subsection, current_section_ref, sub_ref, sub_title
                    )
                    current_article_ref = None
                    current_sentence_ref = None
                    last_clause_ref = None
                    continue

            # (Backup) some files may still use Pheading2-e for subsections
            if "Pheading2-e" in classes and current_section_ref:
                m_sub2 = subsection_re.match(text)
                if m_sub2:
                    sub_ref = m_sub2.group(1)
                    sub_title = m_sub2.group(2) or ""
                    current_subsection_ref = session.execute_write(
                        merge_subsection, current_section_ref, sub_ref, sub_title
                    )
                    current_article_ref = None
                    current_sentence_ref = None
                    last_clause_ref = None
                    continue

            # 2) Articles: <p class="section">9.3.1.1. General</p>
            if "section" in classes and current_subsection_ref:
                ma = article_re.match(text)
                if ma:
                    art_ref = ma.group(1)           # "9.3.1.1"
                    art_title = ma.group(2) or ""
                    current_article_ref = session.execute_write(
                        merge_article, current_subsection_ref, art_ref, art_title
                    )
                    current_sentence_ref = None
                    last_clause_ref = None
                    continue

            # ========= SENTENCES, CLAUSES, SUBCLAUSES =========

            # 3) Sentences: subsection-e and Ssubsection-e
            if ("subsection-e" in classes or "Ssubsection-e" in classes) and current_article_ref:
                msent = sentence_re.match(text)
                if msent:
                    num = msent.group(1)
                    body = msent.group(2) or ""
                    current_sentence_ref = session.execute_write(
                        merge_sentence, current_article_ref, num, body
                    )
                    last_clause_ref = None
                else:
                    if current_sentence_ref:
                        session.execute_write(
                            append_to_sentence,
                            current_sentence_ref,
                            text,
                        )
                continue

            # 4) Clauses: clause-e and Sclause-e
            if ("clause-e" in classes or "Sclause-e" in classes) and current_sentence_ref:
                mcl = clause_re.match(text)
                if mcl:
                    letter = mcl.group(1).lower()
                    body = mcl.group(2) or ""
                    last_clause_ref = session.execute_write(
                        merge_clause, current_sentence_ref, letter, body
                    )
                else:
                    session.execute_write(
                        append_to_sentence,
                        current_sentence_ref,
                        text,
                    )
                continue

            # 5) Subclauses
            if "subclause-e" in classes and last_clause_ref:
                mscl = subclause_re.match(text)
                if mscl:
                    roman = mscl.group(1)
                    body = mscl.group(2) or ""
                    session.execute_write(
                        merge_subclause,
                        last_clause_ref,
                        roman,
                        body,
                    )
                    continue
                else:
                    if current_sentence_ref:
                        session.execute_write(
                            append_to_sentence,
                            current_sentence_ref,
                            text,
                        )
                    continue

            # 6) Any other paragraph while inside a sentence → append to sentence
            if current_sentence_ref:
                session.execute_write(
                    append_to_sentence,
                    current_sentence_ref,
                    text,
                )

        print("Finished parsing Part 9 HTML and loading graph.")


def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        # Run once to create constraints, then comment out
        # init_constraints(driver)
        parse_html_and_load(driver)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
