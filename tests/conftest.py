import os
from pathlib import Path

# The tests resolve `configurations/`, `datasets/` and file_metadata paths relative
# to the current working directory. pytest is normally launched from the repo root
# (see [tool.pytest.ini_options] in pyproject.toml), so pin cwd to this directory at
# collection time -- pytest imports conftest before collecting -- and the tests work
# regardless of where pytest was invoked from.
os.chdir(Path(__file__).parent)
