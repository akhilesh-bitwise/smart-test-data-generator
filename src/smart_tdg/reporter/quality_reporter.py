"""Quality Reporter for synthesized data validation and metrics."""

from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
import json
from models.schema_models import DatabaseSchema, TableSchema
from models.scenario_models import Scenario
from pathlib import Path
import matplotlib.pyplot as plt
import base64
import io
from pandas import DataFrame
from pathlib import Path

class QualityReporter:
    def __init__(
        self,
        schema: DatabaseSchema,
        scenario: Scenario,
        generated_data: Dict[str, pd.DataFrame]
    ):
        self.schema = schema
        self.scenario = scenario
        self.data = generated_data
        self.result = {}

    def validate_all(self) -> Dict[str, Any]:
        self.result = {}
        for table, df in self.data.items():
            schema_obj = self.schema.get_table(table)
            self.result[table] = self.validate_table(table, schema_obj, df)
        return self.result

    def validate_table(self, table_name: str, schema: TableSchema, df: pd.DataFrame) -> Dict[str, Any]:
        result = {}
        # Primary Key
        if schema.primary_key:
            pk_cols = schema.primary_key.columns
            duplicated = df.duplicated(subset=pk_cols).sum()
            result['primary_key'] = {
                "columns": pk_cols,
                "duplicates": int(duplicated),
                "valid": duplicated == 0
            }

        # Unique constraints
        result['unique'] = []
        for unique in schema.unique_constraints:
            ucols = unique.columns
            udup = df.duplicated(subset=ucols).sum()
            result['unique'].append({
                "columns": ucols,
                "duplicates": int(udup),
                "valid": udup == 0
            })

        # Foreign Key constraints
        result['foreign_key'] = []
        for fk in schema.foreign_keys:
            parent = fk.references_table
            parent_cols = fk.references_columns
            child_cols = fk.columns
            if parent in self.data:
                parent_vals = self.data[parent][parent_cols[0]].unique()
                missing_fk = ~df[child_cols[0]].isin(parent_vals)
                n_missing = int(missing_fk.sum())
                result['foreign_key'].append({
                    "child_columns": child_cols,
                    "parent_table": parent,
                    "missing": n_missing,
                    "valid": n_missing == 0
                })

        # Null counts
        col_nulls = {col: int(df[col].isna().sum()) for col in df.columns}
        result['nulls'] = col_nulls

        # Value counts (for all columns)
        result['value_counts'] = {}
        for col in df.columns:
            vc = df[col].value_counts(dropna=False)
            # Convert all keys to string (especially if datetime, date, etc.)
            vc_str_keys = {str(k): v for k, v in vc.items()}
            result['value_counts'][col] = vc_str_keys

        # Ranges (if int/float)
        result['ranges'] = {
            col: {"min": float(df[col].min()), "max": float(df[col].max())}
            for col in df.select_dtypes(include=[np.number]).columns
        }

        # Scenario-specific validation (distributions + PSI)
        result["distribution_validation"] = {}
        scenario_tab = self.scenario.tables.get(table_name)
        if scenario_tab:
            for col, dist in scenario_tab.distributions.items():
                if isinstance(dist, dict) and col in df.columns:
                    vc = df[col].value_counts(normalize=True, dropna=False)
                    actual_dist = {str(k): v for k, v in vc.items()}
                    # PSI
                    psi_val = self.calculate_psi(dist, actual_dist)
                    result["distribution_validation"][col] = {
                        "expected": dist,
                        "actual": actual_dist,
                        "psi": psi_val
                    }
        return result

    def calculate_psi(self, expected: Dict[str, float], actual: Dict[str, float], eps=1e-8) -> float:
        # Union of categories
        all_bins = set(map(str, expected)) | set(map(str, actual))
        psi = 0.0
        for b in all_bins:
            p_expected = float(expected.get(b, eps))
            p_actual = float(actual.get(b, eps))
            # Avoid division by zero
            psi += (p_expected - p_actual) * np.log((p_expected + eps) / (p_actual + eps))
        return float(round(psi, 5))

    def print_summary(self):
        if not self.result:
            print("No results. Run validate_all() first.")
            return
        for table, report in self.result.items():
            print(f"\n==== Table: {table} ====")
            print("Primary key valid:", report['primary_key']['valid'] if 'primary_key' in report else "N/A")
            for i, u in enumerate(report['unique']):
                print(f"Unique #{i+1} valid: {u['valid']} columns: {u['columns']}")
            for i, fk in enumerate(report['foreign_key']):
                print(f"Foreign Key #{i+1} valid: {fk['valid']}")
            print("Null count:", report['nulls'])
            if 'distribution_validation' in report and report['distribution_validation']:
                for col, dist_report in report['distribution_validation'].items():
                    print(f"Distribution for '{col}': PSI={dist_report['psi']:.4f}")
                    print("  Expected:", dist_report['expected'])
                    print("  Actual  :", dist_report['actual'])

    def to_json(self, output_file: str = None):
        # Export results as JSON
        if not self.result:
            self.validate_all()
        output_file = output_file or "./output/quality_report.json"
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(self.result, f, indent=2, default=str)
        print(f"✓ Quality report written to {output_file}")

    def to_html(self, output_file: str = None):
        # Very basic HTML rendering
        if not self.result:
            self.validate_all()
        
        output_file = output_file or "./output/quality_report.html"
        output_file = Path(output_file)
        lines = ["<html><head><title>Quality Report</title></head><body>"]
        lines.append("<h1>Data Quality Report</h1>")
        for table, report in self.result.items():
            lines.append(f"<h2>Table: {table}</h2>")
            lines.append("<ul>")
            if 'primary_key' in report:
                lines.append(f"<li>Primary key valid: {report['primary_key']['valid']}</li>")
            for i, u in enumerate(report['unique']):
                lines.append(f"<li>Unique #{i+1} valid: {u['valid']} Columns: {u['columns']}</li>")
            for i, fk in enumerate(report['foreign_key']):
                lines.append(f"<li>Foreign Key #{i+1} valid: {fk['valid']}</li>")
            lines.append(f"<li>Nulls: {report['nulls']}</li>")
            lines.append("</ul>")
            if "distribution_validation" in report:
                for col, dist_report in report['distribution_validation'].items():
                    img_tag = self._generate_psi_chart(col, dist_report, output_file.parent)
                    lines.append(f"<p><b>{col} PSI:</b> {dist_report['psi']:.4f}</p>")
                    lines.append(img_tag)
                    lines.append("<table border=1><tr><th>Value</th><th>Expected</th><th>Actual</th></tr>")
                    all_keys = set(dist_report['expected']) | set(dist_report['actual'])
                    for k in all_keys:
                        lines.append(f"<tr><td>{k}</td><td>{dist_report['expected'].get(k,0)}</td><td>{dist_report['actual'].get(k,0):.3f}</td></tr>")
                    lines.append("</table>")
        lines.append("</body></html>")
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            f.write('\n'.join(lines))
        print(f"✓ Quality report written to {output_file}")

    def _generate_psi_chart(self, col: str, dist_report: Dict[str, Any], output_dir: str) -> str:
        """
        Generate horizontal bar chart for PSI distribution and return base64 image string.
        """
        expected = dist_report['expected']
        actual = dist_report['actual']

        categories = sorted(set(expected.keys()) | set(actual.keys()))
        x_expected = [expected.get(cat, 0) for cat in categories]
        x_actual = [actual.get(cat, 0) for cat in categories]

        width = 0.35
        y_pos = range(len(categories))

        plt.figure(figsize=(8, 4))
        plt.barh(y_pos, x_expected, width, label='Expected', color='skyblue')
        plt.barh([p + width for p in y_pos], x_actual, width, label='Actual', color='lightgreen')

        plt.yticks([p + width / 2 for p in y_pos], categories)
        plt.xlabel('Proportion')
        plt.title(f'PSI Distribution for {col}')
        plt.legend()

        buffer = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buffer, format='png')
        plt.close()
        buffer.seek(0)

        img_str = base64.b64encode(buffer.read()).decode('utf-8')
        img_tag = f'<img src="data:image/png;base64,{img_str}" alt="PSI chart for {col}" />'
        
        # Save image file as well
        file_path = Path(output_dir) / f"psi_chart_{col}.png"
        with open(file_path, 'wb') as f:
            f.write(base64.b64decode(img_str))

        return img_tag

