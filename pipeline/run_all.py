import subprocess
import sys

def run(script):
    print(f"\nRunning {script}...\n")
    result = subprocess.run(
        [sys.executable, script],
        check=True
    )
    return result

if __name__ == "__main__":
    run("pipeline/elt.py")
    run("pipeline/export_parquet.py")
    run("pipeline/geo_analysis.py")

    print("\n Pipeline run is a success")
