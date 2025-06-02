import sys
from pipeline import TurbinePipeline

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <input_path>")
        sys.exit(1)
    input_path = sys.argv[1]
    pipeline = TurbinePipeline(input_path)
    pipeline.run()