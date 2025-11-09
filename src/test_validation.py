import pandas as pd
import json
from pathlib import Path

def compare_csv(file1: Path, file2: Path, tolerance=1e-5):
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    if df1.shape != df2.shape:
        return False, f"Shape mismatch: {df1.shape} vs {df2.shape}"

    try:
        pd.testing.assert_frame_equal(df1, df2, check_dtype=False, rtol=tolerance, atol=tolerance)
        return True, "PASS"
    except AssertionError as e:
        return False, str(e)

def compare_json(file1: Path, file2: Path):
    with open(file1) as f1, open(file2) as f2:
        json1 = json.load(f1)
        json2 = json.load(f2)
    if json1 == json2:
        return True, "PASS"
    else:
        return False, "JSON contents differ"

def run_validation(test_folder: Path):
    generated_data_dir = test_folder / "outputs"
    expected_data_dir = test_folder / "expected_outputs"

    results = {}

    # Compare CSV files
    for csv_file in expected_data_dir.glob("*.csv"):
        gen_file = generated_data_dir / csv_file.name
        if not gen_file.exists():
            results[csv_file.name] = "Missing generated file"
            continue
        passed, message = compare_csv(gen_file, csv_file)
        results[csv_file.name] = message

    # Compare quality report JSON
    gen_quality = generated_data_dir.parent / "quality_report/quality_report.json"
    expected_quality = expected_data_dir / "quality_report.json"
    if gen_quality.exists() and expected_quality.exists():
        passed, message = compare_json(gen_quality, expected_quality)
        results["quality_report.json"] = message
    else:
        results["quality_report.json"] = "Missing quality report JSON file"

    return results

if __name__ == "__main__":
    import sys
    test_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("examples/example1_retail_orders")
    print(f"Running validation on: {test_dir}")
    results = run_validation(test_dir)
    for file, status in results.items():
        print(f"{file}: {status}")

# C:/Users/akhileshn/Downloads/smart-test-data-generator/src/examples