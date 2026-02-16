import subprocess
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
import grade

class FastGrader(grade.Grader):
    def compile_code(self):
        print("✓ Using existing build")
        return True

if __name__ == "__main__":
    grader = FastGrader()
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--suite')
    parser.add_argument('--type')
    args = parser.parse_args()
    grader.run(filter_suite=args.suite, filter_type=args.type)
