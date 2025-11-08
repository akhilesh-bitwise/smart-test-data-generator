"""Configuration management for Smart TDG."""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Application configuration."""
    
    # LLM Configuration - OpenAI
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    OPENAI_TEMPERATURE: float = float(os.getenv("OPENAI_TEMPERATURE", "0.7"))
    
    # Cache Configuration
    CACHE_DIR: Path = Path(os.getenv("CACHE_DIR", "./cache"))
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", "3600"))
    CACHE_MAX_SIZE_GB: int = int(os.getenv("CACHE_MAX_SIZE_GB", "10"))
    
    # Output Configuration
    OUTPUT_DIR: Path = Path(os.getenv("OUTPUT_DIR", "./output"))
    LOG_DIR: Path = Path(os.getenv("LOG_DIR", "./logs"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # Database Connections
    POSTGRES_URI: Optional[str] = os.getenv("POSTGRES_URI")
    SQLITE_PATH: Path = Path(os.getenv("SQLITE_PATH", "./output/test_data.db"))
    MONGO_URI: Optional[str] = os.getenv("MONGO_URI")
    
    # Generation Defaults
    DEFAULT_SEED: int = int(os.getenv("DEFAULT_SEED", "42"))
    ML_EPOCHS: int = int(os.getenv("ML_EPOCHS", "100"))
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
    STREAM_BATCH_SIZE: int = int(os.getenv("STREAM_BATCH_SIZE", "10000"))
    
    # Quality Reporting
    ENABLE_PSI_CALCULATION: bool = os.getenv("ENABLE_PSI_CALCULATION", "true").lower() == "true"
    PSI_BINS: int = int(os.getenv("PSI_BINS", "10"))
    
    @classmethod
    def ensure_directories(cls):
        """Ensure all required directories exist."""
        cls.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cls.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def validate_openai_key(cls):
        """Validate OpenAI API key is set."""
        if not cls.OPENAI_API_KEY:
            raise ValueError(
                "OPENAI_API_KEY not set. Please set it in .env file or environment variables."
            )


# Ensure directories exist on import
Config.ensure_directories()
