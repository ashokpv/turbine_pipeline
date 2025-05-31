import sys
from pipeline import run_pipeline

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <input_path>")
        sys.exit(1)
    input_path = sys.argv[1]
    # input_path = "dbfs:/FileStore/ashok/*.csv"
    run_pipeline(input_path)