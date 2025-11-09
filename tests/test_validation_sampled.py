import pandas as pd
import json
import numpy as np
from pathlib import Path

def compare_csv_sampled(file_generated: Path, file_expected: Path, sample_size=5, tol=1e-5):
    df_gen = pd.read_csv(file_generated)
    df_exp = pd.read_csv(file_expected)

    if df_exp.shape[0] < sample_size:
        sample_size = df_exp.shape[0]

    # Sample expected rows
    expected_sample = df_exp.sample(n=sample_size, random_state=42).reset_index(drop=True)

    # Try to sample generated rows with same keys if possible
    key_cols = [col for col in df_gen.columns if 'id' in col.lower()]
    if key_cols:
        keys = expected_sample[key_cols]
        matched_rows = df_gen[df_gen[key_cols].apply(tuple, axis=1).isin(keys.apply(tuple, axis=1))]
        if matched_rows.shape[0] >= sample_size:
            generated_sample = matched_rows.sample(n=sample_size, random_state=42).reset_index(drop=True)
        else:
            generated_sample = df_gen.sample(n=sample_size, random_state=42).reset_index(drop=True)
    else:
        generated_sample = df_gen.sample(n=sample_size, random_state=42).reset_index(drop=True)
    
    # Compare sampled rows - allow approximate numerical equality
    try:
        pd.testing.assert_frame_equal(
            generated_sample.select_dtypes(include=[np.number]),
            expected_sample.select_dtypes(include=[np.number]),
            rtol=tol,
            atol=tol,
            check_like=True,
            check_dtype=False,
        )
        numeric_result = True
    except AssertionError as e:
        numeric_result = False
        print(f"Numeric values mismatch: {e}")

    # Compare categorical columns exactly
    cat_cols = expected_sample.select_dtypes(exclude=[np.number]).columns.intersection(generated_sample.columns)
    cat_match = all(
        (generated_sample[col].astype(str) == expected_sample[col].astype(str)).all()
        for col in cat_cols
    )

    if numeric_result and cat_match:
        return True, "PASS"
    else:
        return False, "Failed sampling based CSV comparison"

def compare_json(file_generated: Path, file_expected: Path):
    with open(file_generated) as f1, open(file_expected) as f2:
        json1 = json.load(f1)
        json2 = json.load(f2)
    if json1 == json2:
        return True, "PASS"
    else:
        return False, "JSON contents differ"

def run_validation_sampled(test_folder: Path):
    gen_data_dir = test_folder / "outputs"
    expected_dir = test_folder / "expected_outputs"

    results = {}

    for expected_csv in expected_dir.glob("*.csv"):
        generated_csv = gen_data_dir / expected_csv.name
        if not generated_csv.exists():
            results[expected_csv.name] = "Missing generated CSV"
            continue
        passed, message = compare_csv_sampled(generated_csv, expected_csv)
        results[expected_csv.name] = message

    expected_quality = expected_dir / "quality_report.json"
    generated_quality = gen_data_dir.parent / "quality_report" / "quality_report.json"
    if expected_quality.exists() and generated_quality.exists():
        passed, message = compare_json(generated_quality, expected_quality)
        results["quality_report.json"] = message
    else:
        results["quality_report.json"] = "Missing quality report JSON file"

    return results

if __name__ == "__main__":
    import sys
    folder = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("examples/example1_retail_orders")
    print(f"Running sampled validation on folder: {folder}")
    validation_results = run_validation_sampled(folder)
    for key, val in validation_results.items():
        print(f"{key}: {val}")
