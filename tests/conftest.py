from pathlib import Path

import pytest


@pytest.fixture
def database_path(tmp_path: Path) -> Path:
    return tmp_path / "control.db"
