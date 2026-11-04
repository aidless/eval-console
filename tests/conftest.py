import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from evalconsole import db  # noqa: E402


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "fleet.db")
    db.init_db(path)
    return path


@pytest.fixture()
def conn(db_path):
    connection = db.connect(db_path)
    yield connection
    connection.close()
