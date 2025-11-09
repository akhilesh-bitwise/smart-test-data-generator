# SmartTDG - Smart Test Data Generator

## 🚀 Overview

SmartTDG generates synthetic relational data based on schema definitions and scenario configurations. It supports rule-based and learned synthetic data generation, with FK enforcement and quality validation.

***

## 🧩 Modules & Workflow

### 1. Schema Ingestion (`core.schema_ingestion`)

- Parses SQL DDL schema file.
- Builds schema metadata including tables, columns, and foreign key graphs.
- Fixes constraints like CHECK clauses automatically.
- **Caching:** Uses caching to avoid reparsing schemas repeatedly, improving CLI performance.

### 2. Scenario Modeling (`models.scenario_models`)

- Defines data classes (`Scenario`, `TableScenario`, `CorrelationConfig`, etc.) to structure scenario details.
- Includes `from_dict()` for converting YAML to typed scenario objects.
- Supports plural/singular keys flexibly to align with YAML changes.

### 3. Scenario Engine (`core.scenario_engine`)

- Converts natural language input to scenario YAML definitions.
- Provides initial human-friendly interface for scenario creation.

### 4. Data Generation Engines

- **Rule-Based Generator (`core.data_generator`)**  
  Generates synthetic data using distribution rules, cardinalities, and constraints defined in scenario and schema.
  
- **Learned Generator (`core.learned_data_generator`)**  
  Trains generative models (CTGAN) on real input data (CSV/Parquet)  
  Generates synthetic data preserving learned correlations.  
  Applies FK enforcement post-generation.

### 5. Foreign Key Enforcement (`core.fk_utils`)

- Adjusts synthetic data to ensure foreign key relationships remain consistent across tables.

### 6. Exporters (`exporters.file_exporters`)

- Exports generated or existing data to CSV or Parquet.
- Exports detailed quality and validation reports.

### 7. Quality Reporter (`reporter.quality_reporter`)

- Validates generated/existing datasets against scenario constraints and schema rules.
- Produces report summaries and detailed findings.

### 8. CLI (`cli.cli_main`)

- Commands:
  - `gen_scenario`: Generate scenario YAML from natural language.
  - `gen_data`: Generate synthetic data using rule-based or learned models.
  - `quality_report`: Validate data quality and scenario compliance.

- **CLI Caching:** Uses caching for schema and scenario parsing, speeding repeated runs.

***

## ⚙️ CLI Usage & Options

### 1. `gen_scenario`  
Generate scenario YAML from natural language:

```
python -m cli.cli_main gen_scenario \
  --nl "Describe retail sales scenario" \
  --output ./scenarios/my_scenario.yaml
```

- `--nl`: Natural language description of scenario (prompted if missing)
- `--output`: Path to save generated scenario YAML (default: `./scenarios/generated_scenario.yaml`)

***

### 2. `gen_data`  
Generate synthetic data:

```
python -m cli.cli_main gen_data \
  --schema path/to/schema.sql \
  --scenario path/to/scenario.yaml \
  --output_dir path/to/output \
  --generation_model [rule_based|learned] \
  --output_format [csv|parquet] \
  [--seed 42]
```

- `--schema`: SQL DDL schema filepath.
- `--scenario`: Scenario YAML filepath.
- `--output_dir`: Directory to export generated data.
- `--generation_model`:  
  - `rule_based`: Use hand-coded rules for data generation.  
  - `learned`: Train models on example data (CSV/Parquet in `inputs` folder) and sample synthetic data.
- `--output_format`: Output format `"csv"` (default) or `"parquet"`.
- `--seed`: (Optional) Random seed for reproducibility (default: 42).

***

### 3. `quality_report`  
Run validation on existing data:

```
python -m cli.cli_main quality_report \
  --data_dir path/to/data \
  --schema path/to/schema.sql \
  --scenario path/to/scenario.yaml \
  --output_dir path/to/report
```

- `--data_dir`: Directory containing CSV/Parquet data files.
- `--schema`: Schema SQL filepath.
- `--scenario`: Scenario YAML filepath.
- `--output_dir`: Destination for saving quality reports.

***

## 🔄 Workflow Summary

1. **Schema parsing & caching:** Schema is parsed once and cached for reuse.
2. **Scenario creation:** From YAML or natural language, parsed into typed `Scenario` objects.
3. **Model selection:** Choose between rule-based or learned data generator.
4. **Data generation:** Synthetic data created per table cardinalities and constraints.
5. **Foreign key integrity:** FK relationships enforced across generated tables.
6. **Data export:** Generated datasets exported in chosen formats.
7. **Quality check:** Validate datasets against scenario and schema rules with summary reports.

***

## 🎉 Start generating and validating synthetic data efficiently with SmartTDG!