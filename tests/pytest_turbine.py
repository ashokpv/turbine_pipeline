import pytest
import os
import sys

# Get the path to the directory for this file in the workspace.
dir_root = os.path.dirname(os.path.realpath(__file__))
# Switch to the root directory.
os.chdir(dir_root)

# Skip writing .pyc files to the bytecode cache on the cluster.
sys.dont_write_bytecode = True

retcode = pytest.main(sys.argv[1:])