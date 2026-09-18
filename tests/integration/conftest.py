import os
from pathlib import Path

import psycopg
import pytest
from alembic import command
from alembic.config import Config

from tests.integration.recovery_scenarios import Scenario


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def reset_scenario_database(url, *, expected_name):
    with psycopg.connect(
        url,
        autocommit=True,
        connect_timeout=3,
    ) as connection:
        actual_name = connection.execute(
            "SELECT current_database()"
        ).fetchone()[0]

        if actual_name != expected_name:
            raise RuntimeError(
                f"Refusing database reset: expected {expected_name!r}, "
                f"found {actual_name!r}"
            )

        connection.execute("DROP SCHEMA public CASCADE")
        connection.execute("CREATE SCHEMA public")

    configuration = Config(str(PROJECT_ROOT / "alembic.ini"))
    configuration.set_main_option(
        "script_location",
        str(PROJECT_ROOT / "migrations"),
    )
    configuration.attributes["database_url"] = url

    command.upgrade(configuration, "head")

    return url


@pytest.fixture
def scenario_database_url():
    url = os.environ.get("THREADLINE_TEST_DATABASE_URL")

    if not url:
        pytest.skip("THREADLINE_TEST_DATABASE_URL is not configured")

    return reset_scenario_database(
        url,
        expected_name="threadline_test",
    )


@pytest.fixture
def scenario(scenario_database_url, tmp_path):
    return Scenario(scenario_database_url, tmp_path)


@pytest.fixture
def outage_scenario(tmp_path):
    url = os.environ.get("THREADLINE_OUTAGE_TEST_DATABASE_URL")

    if not url:
        pytest.skip(
            "Set THREADLINE_OUTAGE_TEST_DATABASE_URL to run "
            "the real database-outage test"
        )

    reset_scenario_database(
        url,
        expected_name="threadline_outage_test",
    )

    return Scenario(url, tmp_path)