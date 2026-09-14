"""Command-line runner for Threadline golden scenarios.

Usage:

    python -m threadline.cli run tests/fixtures/golden

    python -m threadline.cli run \
        tests/fixtures/golden/g01_clean_order.json

    python -m threadline.cli run \
        tests/fixtures/golden/g01_clean_order.json \
        --publish build/published-result.json
"""

from __future__ import annotations

import argparse
import difflib
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from threadline.canonicalize import (
    RecordEnvelope,
    canonicalize,
)
from threadline.completeness import (
    DataFileEvidence,
    ManifestEvidence,
    ReportEvidence,
    evaluate_expected_reports,
    sha256_bytes,
)
from threadline.contracts import (
    ContractViolation,
    EntityType,
    ReportType,
    SourceSystem,
    parse_manifest,
    parse_source_record,
    quarantine_from_violation,
)
from threadline.publication import (
    AtomicFilePublicationStore,
    result_to_dict,
)
from threadline.reconcile import (
    ReconciliationResult,
    reconcile,
)


class ScenarioError(ValueError):
    """Raised when a golden scenario is malformed or fails."""


@dataclass(frozen=True, slots=True)
class ScenarioOutcome:
    scenario_id: str
    path: Path
    result: ReconciliationResult
    comparison_view: dict[str, Any]


REPORT_TO_ENTITY = {
    ReportType.ORDERS: EntityType.ORDER,
    ReportType.PAYMENTS: EntityType.PAYMENT,
    ReportType.REFUNDS: EntityType.REFUND,
    ReportType.FEES: EntityType.FEE,
    ReportType.SETTLEMENT_LINES: EntityType.SETTLEMENT_LINE,
    ReportType.PAYOUTS: EntityType.PAYOUT,
}


REPORT_TO_SOURCE = {
    ReportType.ORDERS: SourceSystem.THREADLINE_SHOP,
    ReportType.PAYMENTS: SourceSystem.MOCKPAY,
    ReportType.REFUNDS: SourceSystem.MOCKPAY,
    ReportType.FEES: SourceSystem.MOCKPAY,
    ReportType.SETTLEMENT_LINES: SourceSystem.MOCKPAY,
    ReportType.PAYOUTS: SourceSystem.MOCKPAY,
}


SUPPORTED_REPORT_MODES = frozenset(
    {
        "VALID",
        "MISSING",
        "DATA_ONLY",
        "MANIFEST_ONLY",
        "INVALID_MANIFEST",
        "ROW_COUNT_MISMATCH",
        "CHECKSUM_MISMATCH",
    }
)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ScenarioError(
            f"could not read {path}: {exc}"
        ) from exc

    if not isinstance(value, dict):
        raise ScenarioError(
            f"{path} must contain one JSON object"
        )

    return value


def _parse_timestamp(
    value: Any,
    *,
    field_name: str,
) -> datetime:
    if not isinstance(value, str):
        raise ScenarioError(
            f"{field_name} must be a string"
        )

    normalized = (
        f"{value[:-1]}+00:00"
        if value.endswith("Z")
        else value
    )

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ScenarioError(
            f"{field_name} must be an ISO-8601 timestamp"
        ) from exc

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ScenarioError(
            f"{field_name} must contain an offset"
        )

    return parsed.astimezone(timezone.utc)


def _parse_business_date(value: Any) -> date:
    if not isinstance(value, str):
        raise ScenarioError(
            "business_date must be a string"
        )

    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ScenarioError(
            "business_date must use YYYY-MM-DD"
        ) from exc

    if parsed.isoformat() != value:
        raise ScenarioError(
            "business_date must use canonical YYYY-MM-DD"
        )

    return parsed


def _canonical_file_bytes(
    payloads: list[Mapping[str, Any]],
) -> bytes:
    return json.dumps(
        payloads,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _unwrap_source_row(
    *,
    report_type: ReportType,
    row_number: int,
    item: Any,
) -> tuple[str, Mapping[str, Any]]:
    if not isinstance(item, Mapping):
        raise ScenarioError(
            f"{report_type.value}[{row_number}] "
            "must be a JSON object"
        )

    if "payload" not in item:
        receipt_id = (
            f"{report_type.value}.json:{row_number}"
        )
        return receipt_id, item

    allowed_wrapper_fields = {
        "receipt_id",
        "payload",
    }

    unexpected = set(item) - allowed_wrapper_fields

    if unexpected:
        raise ScenarioError(
            f"{report_type.value}[{row_number}] "
            f"contains unexpected wrapper fields: "
            f"{', '.join(sorted(unexpected))}"
        )

    receipt_id = item.get("receipt_id")
    payload = item.get("payload")

    if (
        not isinstance(receipt_id, str)
        or not receipt_id
    ):
        raise ScenarioError(
            f"{report_type.value}[{row_number}].receipt_id "
            "must be a non-empty string"
        )

    if not isinstance(payload, Mapping):
        raise ScenarioError(
            f"{report_type.value}[{row_number}].payload "
            "must be a JSON object"
        )

    return receipt_id, payload


def _read_input_rows(
    scenario: Mapping[str, Any],
) -> dict[ReportType, list[Any]]:
    inputs = scenario.get("inputs")

    if not isinstance(inputs, Mapping):
        raise ScenarioError(
            "scenario.inputs must be a JSON object"
        )

    unknown = set(inputs) - {
        report_type.value
        for report_type in ReportType
    }

    if unknown:
        raise ScenarioError(
            f"unknown input datasets: "
            f"{', '.join(sorted(unknown))}"
        )

    rows_by_report: dict[
        ReportType,
        list[Any],
    ] = {}

    for report_type in ReportType:
        rows = inputs.get(report_type.value, [])

        if not isinstance(rows, list):
            raise ScenarioError(
                f"inputs.{report_type.value} must be an array"
            )

        rows_by_report[report_type] = rows

    return rows_by_report


def _parse_financial_records(
    rows_by_report: Mapping[ReportType, list[Any]],
) -> tuple[
    list[RecordEnvelope],
    list[Any],
    dict[ReportType, list[Mapping[str, Any]]],
]:
    envelopes: list[RecordEnvelope] = []
    quarantine_records: list[Any] = []
    payloads_by_report: dict[
        ReportType,
        list[Mapping[str, Any]],
    ] = {}

    for report_type in ReportType:
        entity_type = REPORT_TO_ENTITY[report_type]
        raw_rows = rows_by_report[report_type]
        report_payloads: list[Mapping[str, Any]] = []

        for index, item in enumerate(
            raw_rows,
            start=1,
        ):
            receipt_id, payload = _unwrap_source_row(
                report_type=report_type,
                row_number=index,
                item=item,
            )
            report_payloads.append(payload)

            try:
                record = parse_source_record(
                    entity_type,
                    payload,
                )
            except ContractViolation as violation:
                quarantine_records.append(
                    quarantine_from_violation(
                        entity_type,
                        payload,
                        violation,
                    )
                )
                continue

            envelopes.append(
                RecordEnvelope(
                    receipt_id=receipt_id,
                    record=record,
                )
            )

        payloads_by_report[report_type] = (
            report_payloads
        )

    return (
        envelopes,
        quarantine_records,
        payloads_by_report,
    )


def _report_configuration(
    scenario: Mapping[str, Any],
    report_type: ReportType,
) -> Mapping[str, Any]:
    reports = scenario.get("reports")

    if not isinstance(reports, Mapping):
        raise ScenarioError(
            "scenario.reports must be a JSON object"
        )

    configuration = reports.get(report_type.value)

    if configuration is None:
        return {"mode": "MISSING"}

    if not isinstance(configuration, Mapping):
        raise ScenarioError(
            f"reports.{report_type.value} "
            "must be a JSON object"
        )

    return configuration


def _build_report_evidence(
    *,
    scenario_id: str,
    report_type: ReportType,
    business_date: date,
    as_of: datetime,
    configuration: Mapping[str, Any],
    payloads: list[Mapping[str, Any]],
) -> ReportEvidence:
    mode = configuration.get("mode", "VALID")

    if not isinstance(mode, str):
        raise ScenarioError(
            f"reports.{report_type.value}.mode "
            "must be a string"
        )

    mode = mode.upper()

    if mode not in SUPPORTED_REPORT_MODES:
        allowed = ", ".join(
            sorted(SUPPORTED_REPORT_MODES)
        )
        raise ScenarioError(
            f"unsupported report mode {mode!r}; "
            f"expected one of: {allowed}"
        )

    if mode == "MISSING":
        return ReportEvidence()

    received_at = _parse_timestamp(
        configuration.get(
            "received_at_utc",
            as_of.isoformat(),
        ),
        field_name=(
            f"reports.{report_type.value}."
            "received_at_utc"
        ),
    )

    generated_at = _parse_timestamp(
        configuration.get(
            "generated_at_utc",
            received_at.isoformat(),
        ),
        field_name=(
            f"reports.{report_type.value}."
            "generated_at_utc"
        ),
    )

    batch_id = configuration.get(
        "batch_id",
        (
            f"{scenario_id}-"
            f"{report_type.value}-"
            f"{business_date.isoformat()}"
        ),
    )

    if not isinstance(batch_id, str) or not batch_id:
        raise ScenarioError(
            f"reports.{report_type.value}.batch_id "
            "must be a non-empty string"
        )

    source_system = REPORT_TO_SOURCE[report_type]
    file_content = _canonical_file_bytes(payloads)
    file_checksum = sha256_bytes(file_content)
    row_count = len(payloads)

    data_file: DataFileEvidence | None = None

    if mode != "MANIFEST_ONLY":
        data_file = DataFileEvidence(
            batch_id=batch_id,
            source_system=source_system,
            report_type=report_type,
            business_date=business_date,
            received_at_utc=received_at,
            parsed_row_count=row_count,
            sha256=file_checksum,
        )

    manifest_evidence: ManifestEvidence | None = None

    if mode != "DATA_ONLY":
        if mode == "INVALID_MANIFEST":
            manifest_evidence = ManifestEvidence(
                received_at_utc=received_at,
                validation_error=(
                    "synthetic invalid manifest"
                ),
            )
        else:
            manifest_row_count = row_count
            manifest_checksum = file_checksum

            if mode == "ROW_COUNT_MISMATCH":
                manifest_row_count += 1

            if mode == "CHECKSUM_MISMATCH":
                manifest_checksum = (
                    "0" * 64
                    if file_checksum != "0" * 64
                    else "f" * 64
                )

            manifest = parse_manifest(
                {
                    "batch_id": batch_id,
                    "source_system": (
                        source_system.value
                    ),
                    "report_type": report_type.value,
                    "business_date": (
                        business_date.isoformat()
                    ),
                    "schema_version": 1,
                    "generated_at_utc": (
                        generated_at.isoformat()
                    ),
                    "row_count": manifest_row_count,
                    "sha256": manifest_checksum,
                }
            )

            manifest_evidence = ManifestEvidence(
                received_at_utc=received_at,
                manifest=manifest,
            )

    return ReportEvidence(
        data_file=data_file,
        manifest=manifest_evidence,
    )


def _comparison_view(
    result: ReconciliationResult,
    *,
    canonicalization: Any,
) -> dict[str, Any]:
    serialized = result_to_dict(result)

    def without(
        records: list[dict[str, Any]],
        removed_fields: set[str],
    ) -> list[dict[str, Any]]:
        return [
            {
                key: value
                for key, value in record.items()
                if key not in removed_fields
            }
            for record in records
        ]

    return {
        "evidence_summary": {
            "accepted": canonicalization.accepted_count,
            "duplicate": canonicalization.duplicate_count,
            "stale": canonicalization.stale_count,
            "conflicted": (
                canonicalization.conflicted_count
            ),
        },
        "source_completeness": without(
            serialized["source_completeness"],
            {"reason"},
        ),
        "transactions": without(
            serialized["transactions"],
            {"run_id", "contract_version"},
        ),
        "expected_movements": serialized[
            "expected_movements"
        ],
        "payouts": without(
            serialized["payouts"],
            {"run_id", "contract_version"},
        ),
        "exceptions": without(
            serialized["exceptions"],
            {
                "run_id",
                "contract_version",
                "detected_at_utc",
                "status",
            },
        ),
        "quarantine_records": serialized[
            "quarantine_records"
        ],
    }


def _assert_expected(
    *,
    scenario_id: str,
    expected: Any,
    actual: dict[str, Any],
) -> None:
    if not isinstance(expected, Mapping):
        raise ScenarioError(
            f"{scenario_id}.expected must be a JSON object"
        )

    expected_json = json.dumps(
        expected,
        sort_keys=True,
        indent=2,
        ensure_ascii=False,
    ).splitlines()

    actual_json = json.dumps(
        actual,
        sort_keys=True,
        indent=2,
        ensure_ascii=False,
    ).splitlines()

    if expected_json == actual_json:
        return

    difference = "\n".join(
        difflib.unified_diff(
            expected_json,
            actual_json,
            fromfile=f"{scenario_id}:expected",
            tofile=f"{scenario_id}:actual",
            lineterm="",
        )
    )

    raise ScenarioError(
        f"{scenario_id} output did not match:\n{difference}"
    )


def run_scenario(path: Path) -> ScenarioOutcome:
    scenario = _load_json(path)

    scenario_id = scenario.get("scenario_id")

    if (
        not isinstance(scenario_id, str)
        or not scenario_id
    ):
        raise ScenarioError(
            f"{path}: scenario_id must be non-empty"
        )

    business_date = _parse_business_date(
        scenario.get("business_date")
    )
    as_of = _parse_timestamp(
        scenario.get("as_of"),
        field_name="as_of",
    )

    run_id = scenario.get(
        "run_id",
        f"golden:{scenario_id}",
    )

    if not isinstance(run_id, str) or not run_id:
        raise ScenarioError(
            "run_id must be a non-empty string"
        )

    rows_by_report = _read_input_rows(scenario)

    (
        envelopes,
        quarantine_records,
        payloads_by_report,
    ) = _parse_financial_records(rows_by_report)

    canonicalization = canonicalize(envelopes)

    evidence_by_report = {
        report_type: _build_report_evidence(
            scenario_id=scenario_id,
            report_type=report_type,
            business_date=business_date,
            as_of=as_of,
            configuration=_report_configuration(
                scenario,
                report_type,
            ),
            payloads=payloads_by_report[
                report_type
            ],
        )
        for report_type in ReportType
    }

    completeness_results = evaluate_expected_reports(
        business_date=business_date,
        as_of=as_of,
        evidence_by_report=evidence_by_report,
    )

    result = reconcile(
        run_id=run_id,
        detected_at=as_of,
        canonicalization=canonicalization,
        completeness_results=completeness_results,
        quarantine_records=quarantine_records,
    )

    comparison_view = _comparison_view(
        result,
        canonicalization=canonicalization,
    )

    if "expected" not in scenario:
        raise ScenarioError(
            f"{scenario_id} has no expected result"
        )

    _assert_expected(
        scenario_id=scenario_id,
        expected=scenario["expected"],
        actual=comparison_view,
    )

    return ScenarioOutcome(
        scenario_id=scenario_id,
        path=path,
        result=result,
        comparison_view=comparison_view,
    )


def discover_scenarios(path: Path) -> list[Path]:
    if path.is_file():
        if path.suffix.lower() != ".json":
            raise ScenarioError(
                f"scenario file must end with .json: {path}"
            )
        return [path]

    if path.is_dir():
        scenarios = sorted(path.rglob("*.json"))

        if not scenarios:
            raise ScenarioError(
                f"no JSON scenarios found under {path}"
            )

        return scenarios

    raise ScenarioError(
        f"scenario path does not exist: {path}"
    )


def _write_actual_output(
    *,
    output_directory: Path,
    outcome: ScenarioOutcome,
) -> Path:
    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_directory
        / f"{outcome.scenario_id}.actual.json"
    )

    output_path.write_text(
        json.dumps(
            outcome.comparison_view,
            sort_keys=True,
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    return output_path


def run_command(args: argparse.Namespace) -> int:
    scenario_paths = discover_scenarios(
        Path(args.path)
    )

    if args.publish and len(scenario_paths) != 1:
        raise ScenarioError(
            "--publish requires exactly one scenario file"
        )

    outcomes: list[ScenarioOutcome] = []
    failures: list[tuple[Path, Exception]] = []

    for scenario_path in scenario_paths:
        try:
            outcome = run_scenario(scenario_path)
            outcomes.append(outcome)

            print(
                f"PASS {outcome.scenario_id} "
                f"({scenario_path.name})"
            )

            if args.output:
                output_path = _write_actual_output(
                    output_directory=Path(args.output),
                    outcome=outcome,
                )
                print(f"     output: {output_path}")

        except Exception as exc:
            failures.append((scenario_path, exc))
            print(
                f"FAIL {scenario_path.name}: {exc}",
                file=sys.stderr,
            )

    if failures:
        print(
            f"\nScenarios: {len(scenario_paths)} | "
            f"Passed: {len(outcomes)} | "
            f"Failed: {len(failures)}",
            file=sys.stderr,
        )
        return 1

    if args.publish:
        outcome = outcomes[0]
        publisher = AtomicFilePublicationStore(
            args.publish
        )
        receipt = publisher.publish(outcome.result)

        print(f"Published: {receipt.destination}")
        print(f"SHA-256:  {receipt.payload_sha256}")

    print(
        f"\nScenarios: {len(scenario_paths)} | "
        f"Passed: {len(outcomes)} | Failed: 0"
    )

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="threadline",
        description=(
            "Run Threadline financial replay "
            "golden scenarios."
        ),
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    run_parser = subparsers.add_parser(
        "run",
        help="run one scenario or a scenario directory",
    )
    run_parser.add_argument(
        "path",
        help="JSON scenario file or directory",
    )
    run_parser.add_argument(
        "--output",
        help=(
            "optional directory for actual comparison "
            "JSON files"
        ),
    )
    run_parser.add_argument(
        "--publish",
        help=(
            "atomically publish the complete result; "
            "requires one scenario file"
        ),
    )
    run_parser.set_defaults(handler=run_command)

    return parser


def main(
    argv: Sequence[str] | None = None,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        return args.handler(args)
    except ScenarioError as exc:
        parser.error(str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())