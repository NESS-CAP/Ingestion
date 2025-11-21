import os
from dotenv import load_dotenv

load_dotenv()

# Neo4j configuration from environment variables
NEO4J_CONFIG = {
    "uri": os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    "user": os.getenv("NEO4J_USERNAME", os.getenv("NEO4J_USER", "neo4j")),
    "password": os.getenv("NEO4J_PASSWORD"),
    "database": os.getenv("NEO4J_DATABASE", "neo4j"),
}

# Embedding model configuration
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

# Document chunking configuration
CHUNK_SIZE = 512
CHUNK_OVERLAP = 50

# HTML Pipeline Configuration
HTML_PIPELINE_CONFIG = {
    "header_levels": [
        ("h1", "regulation"),
        ("h2", "division"),
        ("h3", "part"),
        ("h4", "section"),
        ("h5", "subsection"),
        ("h6", "clause_group"),
    ],
    "chunk_size_fine": 200,  # Finer chunks for clauses
    "chunk_overlap_fine": 30,
    "gpt_model": "gpt-4o-mini",
    "gpt_rate_limit_delay": 1.0,  # seconds between API calls
    "enable_embeddings": True,
    "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
}
