"""Construct the production recovery worker from runtime configuration."""

from __future__ import annotations

import importlib
import os

from collections.abc import Callable
from typing import Any

from threadline.full_rebuild_recovery import FullRebuildRecovery


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()

    if not value:
        raise RuntimeError(
            f"{name} must be configured before recovery can run"
        )

    return value


def _load_callable(
    import_path: str,
    *,
    setting_name: str,
) -> Callable[..., Any]:
    """Load a callable using the form package.module:function."""
    module_name, separator, attribute_name = import_path.partition(":")

    if (
        separator != ":"
        or not module_name
        or not attribute_name
        or ":" in attribute_name
    ):
        raise RuntimeError(
            f"{setting_name} must use package.module:function"
        )

    module = importlib.import_module(module_name)
    function = getattr(module, attribute_name)

    if not callable(function):
        raise TypeError(
            f"The callable configured by {setting_name} is not callable"
        )

    return function


def create_recovery_worker() -> FullRebuildRecovery:
    database_url = _required_environment(
        "THREADLINE_DATABASE_URL"
    )

    builder_path = _required_environment(
        "THREADLINE_RECOVERY_BUILDER"
    )

    validator_path = os.environ.get(
        "THREADLINE_RECOVERY_VALIDATOR",
        "threadline.publication:validate_for_publication",
    ).strip()

    if not validator_path:
        raise RuntimeError(
            "THREADLINE_RECOVERY_VALIDATOR must not be empty"
        )

    build_candidate = _load_callable(
        builder_path,
        setting_name="THREADLINE_RECOVERY_BUILDER",
    )

    validate_domain = _load_callable(
        validator_path,
        setting_name="THREADLINE_RECOVERY_VALIDATOR",
    )

    publication_name = os.environ.get(
        "THREADLINE_PUBLICATION_NAME",
        "threadline",
    ).strip()

    if not publication_name:
        raise RuntimeError(
            "THREADLINE_PUBLICATION_NAME must not be empty"
        )

    return FullRebuildRecovery(
        database_url=database_url,
        build_candidate=build_candidate,
        validate_domain=validate_domain,
        publication_name=publication_name,
    )