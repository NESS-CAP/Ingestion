"""HTML-based e-laws ingestion pipeline"""

def __getattr__(name):
    """Lazy load modules to avoid import errors for optional dependencies"""
    if name == "HTMLIngestPipeline":
        from .main import HTMLIngestPipeline
        return HTMLIngestPipeline
    elif name == "HTMLLoader":
        from .stage1_html_loader import HTMLLoader
        return HTMLLoader
    elif name == "Neo4jHTMLIngester":
        from .stage3_neo4j_html_ingestion import Neo4jHTMLIngester
        return Neo4jHTMLIngester
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "HTMLIngestPipeline",
    "HTMLLoader",
    "Neo4jHTMLIngester"
]
