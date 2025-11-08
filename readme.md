smart-test-data-generator/
│
├── README.md                          # Project documentation
├── requirements.txt                   # Python dependencies
├── setup.py                          # Package installation config
├── .env.example                      # Environment variables template
├── .gitignore                        # Git ignore file
│
├── src/                              # Main source code
│   └── smart_tdg/                    # Main package
│       ├── __init__.py
│       │
│       ├── core/                     # Core functionality modules
│       │   ├── __init__.py
│       │   ├── schema_ingestion.py  # Schema parsing & inference
│       │   ├── data_generator.py    # Hybrid generation engine
│       │   ├── scenario_engine.py   # Scenario DSL & NL parser
│       │   └── quality_reporter.py  # Validation & metrics
│       │
│       ├── models/                   # Data models & schemas
│       │   ├── __init__.py
│       │   ├── schema_models.py     # Schema data structures
│       │   └── scenario_models.py   # Scenario data structures
│       │
│       ├── parsers/                  # Format-specific parsers
│       │   ├── __init__.py
│       │   ├── sql_parser.py        # SQL DDL parser
│       │   ├── openapi_parser.py    # OpenAPI/JSON Schema
│       │   └── avro_parser.py       # Avro/Parquet parser
│       │
│       ├── generators/               # Data generation strategies
│       │   ├── __init__.py
│       │   ├── rule_based.py        # Faker-based generation
│       │   ├── ml_based.py          # CTGAN/TVAE generation
│       │   └── edge_case.py         # Edge case generators
│       │
│       ├── exporters/                # Export & loader modules
│       │   ├── __init__.py
│       │   ├── file_exporters.py    # CSV, JSON, Parquet
│       │   ├── sql_exporters.py     # SQL statements
│       │   └── db_loaders.py        # Direct DB loading
│       │
│       ├── cache/                    # Caching mechanisms
│       │   ├── __init__.py
│       │   ├── disk_cache.py        # DiskCache implementation
│       │   └── cache_manager.py     # Cache management
│       │
│       ├── utils/                    # Utility functions
│       │   ├── __init__.py
│       │   ├── graph_utils.py       # FK graph operations
│       │   ├── validation.py        # Validation helpers
│       │   └── config.py            # Configuration management
│       │
│       └── cli/                      # Command-line interface
│           ├── __init__.py
│           └── main.py              # CLI entry point
│
├── templates/                        # Pre-built scenario templates
│   ├── hr_system.yaml
│   ├── ecommerce.yaml
│   ├── payments.yaml
│   └── healthcare.yaml
│
├── tests/                            # Test suite
│   ├── __init__.py
│   ├── test_schema_ingestion.py
│   ├── test_data_generator.py
│   ├── test_scenario_engine.py
│   └── fixtures/                    # Test data
│       ├── sample_ddl.sql
│       └── sample_openapi.yaml
│
├── examples/                         # Usage examples
│   ├── basic_usage.py
│   ├── ml_generation.py
│   └── custom_scenarios.py
│
├── docs/                            # Documentation
│   ├── architecture.md
│   ├── api_reference.md
│   └── user_guide.md
│
├── cache/                           # Cache directory (gitignored)
├── output/                          # Generated data output (gitignored)
└── logs/                            # Application logs (gitignored)
