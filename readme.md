# SmartTDG - Smart Test Data Generator

## 🚀 Overview

SmartTDG is a synthetic data generator for relational databases. It creates realistic datasets based on schema definitions and human-readable scenarios using both rule-based and learned machine learning methods. Key focuses include data integrity, scenario-driven customization, and extensible CLI operations.

***

## 🧩 Modules Explained

### `core.schema_ingestion`
- Parses SQL schema files.
- Extracts tables, columns, constraints, and foreign key relationships.

### `models.scenario_models`
- Defines `Scenario`, `TableScenario`, and related data classes.
- Provides `from_dict()` for parsing YAML scenario files into typed objects.

### `core.scenario_engine`
- Generates scenario definitions from natural language.

### `core.data_generator`
- Rule-based synthetic data generator complying with scenario constraints.

### `core.learned_data_generator`
- Trains generative models (CTGAN) on real data (CSV/Parquet).
- Samples synthetic data respecting learned distributions and correlations.
- Enforces foreign keys post-generation for consistency.

### `core.fk_utils`
- Utilities to enforce foreign key constraints in generated datasets.

### `exporters.file_exporters`
- Exports synthetic or real data in CSV or Parquet formats.
- Exports quality reports.

### `reporter.quality_reporter`
- Validates data against scenario expectations.
- Outputs detailed quality summaries.

### `cli.cli_main`
- Command-line interface with commands to generate scenarios, generate data (rule-based or learned), and run quality reports.

***

## 🛠️ CLI Usage

### 1. Generate Scenario from Natural Language

```bash
python -m cli.cli_main gen_scenario --nl "Describe your scenario here" --output ./scenarios/my_scenario.yaml
```

### 2. Generate Synthetic Data

```bash
python -m cli.cli_main gen_data --schema path/to/schema.sql --scenario path/to/scenario.yaml --output_dir path/to/output --generation_model [rule_based|learned] --output_format [csv|parquet]
```

- `--schema` : Path to your DB schema in SQL.
- `--scenario` : Path to scenario YAML file.
- `--seed` : (Optional) Random seed, default 42.
- `--output_dir` : Directory to save generated datasets.
- `--generation_model` :
  - `rule_based` – uses predefined rules and distributions.
  - `learned` – trains and generates data using learned models on input CSV/Parquet data located next to your schema file.
- `--output_format` : `csv` or `parquet`.

### 3. Run Quality Report

```bash
python -m cli.cli_main quality_report --data_dir path/to/data --schema path/to/schema.sql --scenario path/to/scenario.yaml --output_dir path/to/report
```

- `--data_dir` : Directory containing data files (CSV or Parquet) for validation.
- `--schema` : Schema SQL file.
- `--scenario` : Scenario YAML file.
- `--output_dir` : Directory to save generated quality reports.

***

## 📋 Notes

- The CLI will infer input data files (CSV/Parquet) automatically for learned models by looking in a folder named `inputs` next to your schema SQL file.
- Scenario YAML keys like `entities`, `distribution`, `correlation`, and `temporal_pattern` are parsed into structured model classes with `from_dict()`.
- Foreign key constraints are enforced after synthetic data generation, ensuring referential integrity.
- Quality reports validate generated or existing data against scenario and schema constraints.

***

# Enjoy generating high-quality synthetic data with SmartTDG! 🎉