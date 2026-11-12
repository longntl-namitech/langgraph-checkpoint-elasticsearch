import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"


@pytest.mark.parametrize("script", ["resume.py", "scoping.py"])
def test_example_runs(script):
    env = {**os.environ, "ES_URL": os.environ.get("ES_URL", "http://localhost:9200")}
    result = subprocess.run(
        [sys.executable, str(EXAMPLES / script)],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip()
