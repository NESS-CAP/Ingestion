"""
Shared configuration settings for ingestion pipelines.
Loads from environment variables via .env file.
"""

import os
from pathlib import Path

# Load environment variables
try:
    from dotenv import load_dotenv
    # Look for .env in the Ingestion directory or App backend
    ingestion_root = Path(__file__).resolve().parents[3]
    # Check backend .env first (has all variables), then Ingestion .env
    env_paths = [
        ingestion_root.parent / "App" / "App" / "backend" / ".env",
        ingestion_root / ".env",
    ]
    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            break
except ImportError:
    pass


# Neo4j Configuration
NEO4J_CONFIG = {
    "uri": os.getenv("NEO4J_URI", ""),
    "user": os.getenv("NEO4J_USER", ""),
    "password": os.getenv("NEO4J_PASSWORD", ""),
    "database": os.getenv("NEO4J_DATABASE", "neo4j"),
}

# LLM Configuration for semantic extraction
LLM_CONFIG = {
    "provider": os.getenv("USE_API", "groq"),
    "api_key": os.getenv("API_KEY", ""),
    "groq_model": os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"),
    "openai_model": os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
    "anthropic_model": os.getenv("ANTHROPIC_MODEL", "claude-3-haiku-20240307"),
}
