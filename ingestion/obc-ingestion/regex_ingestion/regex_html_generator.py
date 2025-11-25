import re
import sys
from pathlib import Path
from bs4 import BeautifulSoup
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

HTML_PATH = "./building_code.html"

CODE_ID = "ON_BC_332_12"
CODE_TITLE = "Ontario Regulation 332/12 – Building Code"
CODE_JURISDICTION = "Ontario"

# ==========================
# REGEX PATTERNS
# ==========================

# DIVISION A ...
division_heading_re = re.compile(r"^DIVISION\s+([A-Z])\b(.*)", re.IGNORECASE)

# Part 1 ...
part_heading_re = re.compile(r"^Part\s+(\d+)\b(.*)", re.IGNORECASE)

# Section 1.1. Organization and Application
section_heading_re = re.compile(r"^Section\s+(\d+\.\d+)\.\s*(.*)", re.IGNORECASE)

# Article headings like "1.1.1.1. Scope of Division A" or "1.2.1.1 Compliance ..."
article_heading_re = re.compile(r"^(\d+(?:\.\d+){2,})\.?\s*(.*)")

# Sentences "(1) Some text..."
subsection_number_re = re.compile(r"^\((\d+)\)\s*(.+)")

# ==========================
# INTERNAL REF PATTERNS (unchanged)
# ==========================

article_ref_re = re.compile(
    r"\b[Aa]rticle\s+(\d+(?:\.\d+){2,})\.", re.UNICODE
)

sentence_ref_re = re.compile(
    r"\b[Ss]entences?\s+(\d+(?:\.\d+){2,})\.\((\d+)\)", re.UNICODE
)

bare_sentence_ref_re = re.compile(
    r"\b(\d+(?:\.\d+){2,})\.\((\d+)\)", re.UNICODE
)

# ==========================
# NEO4J SETUP & HELPERS
# ==========================

def init_constraints_and_root(driver):
    with driver.session() as session:
        session.run("""
        CREATE CONSTRAINT code_pk IF NOT EXISTS
        FOR (c:Code)
        REQUIRE c.codeId IS UNIQUE
        """)
        session.run("""
        CREATE CONSTRAINT division_pk IF NOT EXISTS
        FOR (d:Division)
        REQUIRE d.codeId IS UNIQUE
        """)
        session.run("""
        CREATE CONSTRAINT part_pk IF NOT EXISTS
        FOR (p:Part)
        REQUIRE p.codeId IS UNIQUE
        """)
        session.run("""
        CREATE CONSTRAINT section_pk IF NOT EXISTS
        FOR (s:Section)
        REQUIRE s.codeId IS UNIQUE
        """)
        session.run("""
        CREATE CONSTRAINT article_pk IF NOT EXISTS
        FOR (a:Article)
        REQUIRE a.codeId IS UNIQUE
        """)
        session.run("""
        CREATE CONSTRAINT sentence_pk IF NOT EXISTS
        FOR (s:Sentence)
        REQUIRE s.codeId IS UNIQUE
        """)

        # Helpful indexes for REFERS_TO lookup
        session.run("""
        CREATE INDEX article_ref_idx IF NOT EXISTS
        FOR (a:Article)
        ON (a.ref)
        """)
        session.run("""
        CREATE INDEX sentence_ref_idx IF NOT EXISTS
        FOR (s:Sentence)
        ON (s.ref)
        """)

        # Root Code node
        session.run("""
        MERGE (c:Code {codeId: $codeId})
        SET c.title = $title,
            c.jurisdiction = $jurisdiction
        """, codeId=CODE_ID, title=CODE_TITLE,
           jurisdiction=CODE_JURISDICTION)


def merge_division(tx, division_letter, title):
    code_id = f"{CODE_ID}-{division_letter}"
    tx.run("""
    MERGE (d:Division {codeId: $codeId})
    SET d.division = $division,
        d.title = coalesce(d.title, $title)
    WITH d
    MATCH (c:Code {codeId: $rootCodeId})
    MERGE (c)-[:HAS_DIVISION]->(d)
    """, codeId=code_id,
         division=division_letter,
         title=title.strip(),
         rootCodeId=CODE_ID)
    return code_id


def merge_part(tx, division_code_id, part_number, title):
    code_id = f"{division_code_id}-{part_number}"
    tx.run("""
    MERGE (p:Part {codeId: $codeId})
    SET p.partNumber = $partNumber,
        p.title = coalesce(p.title, $title)
    WITH p
    MATCH (d:Division {codeId: $divisionCodeId})
    MERGE (d)-[:HAS_PART]->(p)
    """, codeId=code_id,
         partNumber=int(part_number),
         title=title.strip(),
         divisionCodeId=division_code_id)
    return code_id


def merge_section(tx, part_code_id, section_number, title):
    code_id = f"{part_code_id}-{section_number}"
    tx.run("""
    MERGE (s:Section {codeId: $codeId})
    SET s.sectionNumber = $sectionNumber,
        s.title = coalesce(s.title, $title)
    WITH s
    MATCH (p:Part {codeId: $partCodeId})
    MERGE (p)-[:HAS_SECTION]->(s)
    """, codeId=code_id,
         sectionNumber=section_number,
         title=title.strip(),
         partCodeId=part_code_id)
    return code_id


def merge_article(tx, section_code_id, article_ref, title):
    # article_ref like "1.1.1.1"
    code_id = f"{section_code_id}-{article_ref}"
    tx.run("""
    MERGE (a:Article {codeId: $codeId})
    SET a.ref = $ref,
        a.title = coalesce(a.title, $title)
    WITH a
    MATCH (s:Section {codeId: $sectionCodeId})
    MERGE (s)-[:HAS_ARTICLE]->(a)
    """, codeId=code_id,
         ref=article_ref,
         title=title.strip(),
         sectionCodeId=section_code_id)
    return code_id


def merge_sentence(tx, article_code_id, article_ref, order_in_article, text):
    sent_code_id = f"{article_code_id}-{order_in_article}"
    sent_ref = f"{article_ref}.({order_in_article})"
    tx.run("""
    MERGE (s:Sentence {codeId: $codeId})
    SET s.ref = $ref,
        s.orderInArticle = $orderInArticle,
        s.text = $text
    WITH s
    MATCH (a:Article {codeId: $articleCodeId})
    MERGE (a)-[:HAS_SENTENCE]->(s)
    """, codeId=sent_code_id,
         ref=sent_ref,
         orderInArticle=int(order_in_article),
         text=text.strip(),
         articleCodeId=article_code_id)
    return sent_code_id


# ==========================
# REFERS_TO HELPERS (unchanged)
# ==========================

def create_refers_to_article(tx, src_id, article_ref, ref_text):
    tx.run("""
    MATCH (src:Sentence {codeId: $srcId})
    MATCH (tgt:Article {ref: $ref})
    MERGE (src)-[r:REFERS_TO {refKind: 'Article', refText: $refText}]->(tgt)
    """, srcId=src_id, ref=article_ref, refText=ref_text)


def create_refers_to_sentence(tx, src_id, sentence_ref, ref_text):
    tx.run("""
    MATCH (src:Sentence {codeId: $srcId})
    MATCH (tgt:Sentence {ref: $ref})
    MERGE (src)-[r:REFERS_TO {refKind: 'Sentence', refText: $refText}]->(tgt)
    """, srcId=src_id, ref=sentence_ref, refText=ref_text)


def create_internal_refs(driver):
    """
    Second pass: scan all Sentence.text, detect internal cross-references,
    and create REFERS_TO relationships.
    """
    with driver.session() as session:
        result = session.run("""
            MATCH (s:Sentence)
            RETURN s.codeId AS codeId, s.text AS text
        """)

        for record in result:
            src_id = record["codeId"]
            text = record["text"] or ""

            # Article references
            for m in article_ref_re.finditer(text):
                art_ref = m.group(1)
                ref_text = m.group(0)
                session.execute_write(
                    create_refers_to_article, src_id, art_ref, ref_text
                )

            # Sentence references
            for m in sentence_ref_re.finditer(text):
                art_ref = m.group(1)
                sent_no = m.group(2)
                sent_ref = f"{art_ref}.({sent_no})"
                ref_text = m.group(0)
                session.execute_write(
                    create_refers_to_sentence, src_id, sent_ref, ref_text
                )

            # Bare sentence refs
            for m in bare_sentence_ref_re.finditer(text):
                start = m.start()
                prefix = text[max(0, start - 15):start]
                if re.search(r"[Aa]rticle\s+$|[Ss]entences?\s+$", prefix):
                    continue
                art_ref = m.group(1)
                sent_no = m.group(2)
                sent_ref = f"{art_ref}.({sent_no})"
                ref_text = m.group(0)
                session.execute_write(
                    create_refers_to_sentence, src_id, sent_ref, ref_text
                )


# ==========================
# HTML PARSER (FIRST PASS)
# ==========================

def parse_html_and_load(driver):
    with open(HTML_PATH, encoding="utf-8") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")
    section_div = soup.find("div", class_="WordSection1")
    if not section_div:
        raise RuntimeError("Could not find div.WordSection1 in HTML")

    current_division_code_id = None
    current_part_code_id = None
    current_section_code_id = None
    current_article_code_id = None
    current_article_ref = None
    current_sentence_order = 0  # numeric (1), (2), ...

    last_sentence_code_id = None  # for appending continuation paragraphs

    with driver.session() as session:
        p_elements = section_div.find_all("p")
        total = len(p_elements)
        next_mark = 5

        # Iterate in document order over all <p> inside WordSection1
        for idx, p in enumerate(p_elements, start=1):
            classes = p.get("class", [])
            text = p.get_text(separator=" ", strip=True)
            if not text:
                continue

            # Skip table-of-contents formatting classes
            if "tablebold-e" in classes or "table-e" in classes:
                continue

            # 1) DIVISION / PART
            if "partnum-e" in classes:
                m_div = division_heading_re.match(text)
                m_part = part_heading_re.match(text)

                if m_div:
                    div_letter = m_div.group(1)
                    div_title = m_div.group(2) or ""
                    current_division_code_id = session.execute_write(
                        merge_division, div_letter, div_title
                    )
                    current_part_code_id = None
                    current_section_code_id = None
                    current_article_code_id = None
                    current_article_ref = None
                    current_sentence_order = 0
                    last_sentence_code_id = None
                    continue

                if m_part and current_division_code_id:
                    part_no = m_part.group(1)
                    part_title = m_part.group(2) or ""
                    current_part_code_id = session.execute_write(
                        merge_part, current_division_code_id, part_no, part_title
                    )
                    current_section_code_id = None
                    current_article_code_id = None
                    current_article_ref = None
                    current_sentence_order = 0
                    last_sentence_code_id = None
                    continue

            # 2) SECTION HEADINGS (ruleb-e → "Section 1.1.")
            if "ruleb-e" in classes:
                m_sec = section_heading_re.match(text)
                if m_sec and current_part_code_id:
                    section_no = m_sec.group(1)
                    section_title = m_sec.group(2) or ""
                    current_section_code_id = session.execute_write(
                        merge_section, current_part_code_id, section_no, section_title
                    )
                    current_article_code_id = None
                    current_article_ref = None
                    current_sentence_order = 0
                    last_sentence_code_id = None
                    continue

                # numeric-only ruleb-e (like "1.1.1. Organization of this Code")
                # are grouping headings; we ignore them as nodes for now
                continue

            # 3) ARTICLE HEADINGS (class "section" → "1.1.1.1. Scope ...")
            if "section" in classes:
                m_art = article_heading_re.match(text)
                if m_art and current_section_code_id:
                    article_ref = m_art.group(1)  # "1.1.1.1"
                    article_title = m_art.group(2) or ""
                    current_article_code_id = session.execute_write(
                        merge_article, current_section_code_id, article_ref, article_title
                    )
                    current_article_ref = article_ref
                    current_sentence_order = 0
                    last_sentence_code_id = None
                    continue

            # 4) SENTENCES (class "subsection-e" with "(1)", "(2)", etc.)
            if "subsection-e" in classes and current_article_code_id and current_article_ref:
                m_sent = subsection_number_re.match(text)
                if m_sent:
                    sent_no = m_sent.group(1)
                    sent_text = m_sent.group(2)
                    current_sentence_order = int(sent_no)
                    last_sentence_code_id = session.execute_write(
                        merge_sentence,
                        current_article_code_id,
                        current_article_ref,
                        current_sentence_order,
                        sent_text,
                    )
                    continue
                # If no number pattern, fall through and treat as continuation

            # 5) Continuation / subclauses / definitions → append to last sentence
            if last_sentence_code_id:
                session.run("""
                    MATCH (s:Sentence {codeId: $codeId})
                    SET s.text = s.text + ' ' + $extra
                """, codeId=last_sentence_code_id, extra=text)
                continue

            # Lightweight progress indicator every ~5%
            progress = int(idx / total * 100)
            if progress >= next_mark:
                print(f"Processed {progress}% ({idx}/{total} paragraphs)")
                next_mark += 5

        print("Finished parsing HTML and loading structure.")


# ==========================
# MAIN
# ==========================

def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        init_constraints_and_root(driver)
        parse_html_and_load(driver)
        create_internal_refs(driver)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
