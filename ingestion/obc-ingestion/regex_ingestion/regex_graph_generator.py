import re
import sys
from pathlib import Path
import pdfplumber
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

PDF_PATH = "./building_code.pdf"
MAX_PAGES = 932  # limit ingestion to first N pages

CODE_ID = "ON_BC_332_12"
CODE_TITLE = "Ontario Regulation 332/12 – Building Code"
CODE_JURISDICTION = "Ontario"

# ==========================
# REGEX PATTERNS (STRUCTURE)
# ==========================

division_re = re.compile(r"^DIVISION\s+([A-Z])\s*(.*)", re.IGNORECASE)
part_re = re.compile(r"^PART\s+(\d+)\s+(.*)", re.IGNORECASE)
section_re = re.compile(r"^Section\s+(\d+\.\d+)\.?\s*(.*)", re.IGNORECASE)
article_re = re.compile(r"^(\d+(?:\.\d+){2,})\.\s*(.*)")
sentence_re = re.compile(r"^\((\d+)\)\s*(.+)")

# ==========================
# REGEX PATTERNS (INTERNAL REFS)
# ==========================

# "Article 1.1.2.6."
article_ref_re = re.compile(
    r"\b[Aa]rticle\s+(\d+(?:\.\d+){2,})\.", re.UNICODE
)

# "Sentence 1.1.3.1.(1)" or "Sentences 1.1.3.1.(1)"
sentence_ref_re = re.compile(
    r"\b[Ss]entences?\s+(\d+(?:\.\d+){2,})\.\((\d+)\)", re.UNICODE
)

# Bare "1.1.3.1.(1)" – we’ll treat as a sentence ref if not already
# preceded by "Article"/"Sentence"
bare_sentence_ref_re = re.compile(
    r"\b(\d+(?:\.\d+){2,})\.\((\d+)\)", re.UNICODE
)

# ==========================
# NEO4J SETUP & HELPERS
# ==========================

def init_constraints_and_root(driver):
    with driver.session(**SESSION_KWARGS) as session:
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

        # Code node
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
         orderInArticle=order_in_article,
         text=text.strip(),
         articleCodeId=article_code_id)
    return sent_code_id


# ==========================
# REFERS_TO HELPERS
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
    with driver.session(**SESSION_KWARGS) as session:
        result = session.run("""
            MATCH (s:Sentence)
            RETURN s.codeId AS codeId, s.text AS text
        """)

        for record in result:
            src_id = record["codeId"]
            text = record["text"] or ""

            # Article references: "Article 1.1.2.6."
            for m in article_ref_re.finditer(text):
                art_ref = m.group(1)
                ref_text = m.group(0)
                session.execute_write(
                    create_refers_to_article, src_id, art_ref, ref_text
                )

            # Sentence refs: "Sentence 1.1.3.1.(1)" / "Sentences 1.1.3.1.(1)"
            for m in sentence_ref_re.finditer(text):
                art_ref = m.group(1)
                sent_no = m.group(2)
                sent_ref = f"{art_ref}.({sent_no})"
                ref_text = m.group(0)
                session.execute_write(
                    create_refers_to_sentence, src_id, sent_ref, ref_text
                )

            # Bare "1.1.3.1.(1)" – treat as sentence ref if not already
            # directly preceded by "Article"/"Sentence"
            for m in bare_sentence_ref_re.finditer(text):
                start = m.start()
                prefix = text[max(0, start - 15):start]
                if re.search(r"[Aa]rticle\s+$|[Ss]entences?\s+$", prefix):
                    # already handled by the explicit patterns
                    continue
                art_ref = m.group(1)
                sent_no = m.group(2)
                sent_ref = f"{art_ref}.({sent_no})"
                ref_text = m.group(0)
                session.execute_write(
                    create_refers_to_sentence, src_id, sent_ref, ref_text
                )


# ==========================
# PARSER (FIRST PASS)
# ==========================

def parse_pdf_and_load(driver):
    current_division_code_id = None
    current_part_code_id = None
    current_section_code_id = None
    current_article_code_id = None
    current_article_ref = None
    current_sentence_order = 0

    with driver.session(**SESSION_KWARGS) as session, pdfplumber.open(PDF_PATH) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            if page_index > MAX_PAGES:
                print(f"Reached page limit ({MAX_PAGES}); stopping ingestion.")
                break
            text = page.extract_text()
            if not text:
                continue

            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue

                # Division
                div_match = division_re.match(line)
                if div_match:
                    division_letter = div_match.group(1)
                    div_title = div_match.group(2) or ""
                    current_division_code_id = session.execute_write(
                        merge_division, division_letter, div_title
                    )
                    current_part_code_id = None
                    current_section_code_id = None
                    current_article_code_id = None
                    current_article_ref = None
                    current_sentence_order = 0
                    continue

                # Part
                part_match = part_re.match(line)
                if part_match and current_division_code_id:
                    part_no = part_match.group(1)
                    part_title = part_match.group(2) or ""
                    current_part_code_id = session.execute_write(
                        merge_part, current_division_code_id, part_no, part_title
                    )
                    current_section_code_id = None
                    current_article_code_id = None
                    current_article_ref = None
                    current_sentence_order = 0
                    continue

                # Section
                section_match = section_re.match(line)
                if section_match and current_part_code_id:
                    section_no = section_match.group(1)
                    section_title = section_match.group(2) or ""
                    current_section_code_id = session.execute_write(
                        merge_section, current_part_code_id, section_no, section_title
                    )
                    current_article_code_id = None
                    current_article_ref = None
                    current_sentence_order = 0
                    continue

                # Article
                article_match = article_re.match(line)
                if article_match and current_section_code_id:
                    article_ref = article_match.group(1)
                    article_title = article_match.group(2) or ""
                    current_article_code_id = session.execute_write(
                        merge_article, current_section_code_id, article_ref, article_title
                    )
                    current_article_ref = article_ref
                    current_sentence_order = 0
                    continue

                # Sentence
                sentence_match = sentence_re.match(line)
                if sentence_match and current_article_code_id and current_article_ref:
                    sent_no = int(sentence_match.group(1))
                    sent_text = sentence_match.group(2)
                    current_sentence_order = sent_no
                    session.execute_write(
                        merge_sentence,
                        current_article_code_id,
                        current_article_ref,
                        current_sentence_order,
                        sent_text
                    )
                    continue

                # Continuation of last sentence
                if current_article_code_id and current_article_ref and current_sentence_order > 0:
                    sent_code_id = f"{current_article_code_id}-{current_sentence_order}"
                    session.run("""
                    MATCH (s:Sentence {codeId: $codeId})
                    SET s.text = s.text + ' ' + $extra
                    """, codeId=sent_code_id, extra=line)

            print(f"Finished page {page_index}")


# ==========================
# MAIN
# ==========================

def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD), connection_timeout=10)
    try:
        init_constraints_and_root(driver)
        parse_pdf_and_load(driver)
        create_internal_refs(driver)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
