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
