"""Retryable archival of committed Threadline JSON source files."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import tempfile

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from uuid import UUID
from datetime import date, datetime
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from threadline.contracts import ReportType


LOGGER = logging.getLogger(__name__)

# Separate from FINANCIAL_STATE_LOCK = 740031.
# All archive writers must follow this locking convention.
ARCHIVE_STATE_LOCK = 740032

FailureHook = Callable[[str], None]


class ArchiveError(RuntimeError):
    pass


class ArchiveIntegrityError(ArchiveError):
    pass


class ArchiveEvidenceMissing(ArchiveError):
    pass


@dataclass(frozen=True, slots=True)
class ArchiveOutcome:
    batch_id: UUID
    archive_path: str
    file_checksum: str
    reused_existing_object: bool


def sha256_file(path: Path) -> str:
    if not path.is_file():
        raise ArchiveEvidenceMissing(
            f"Required file is missing or is not a regular file: {path}"
        )

    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)

    return digest.hexdigest()


def verify_checksum(path: Path, expected: str) -> None:
    actual = sha256_file(path)

    if actual != expected:
        raise ArchiveIntegrityError(
            f"Checksum mismatch for {path}: "
            f"expected {expected}, observed {actual}"
        )


def archive_relative_path(
    report_type: ReportType | str,
    report_date: date,
    checksum: str,
) -> Path:
    """Build a content-addressed archive path.

    Accept a ReportType instance, its member name, or its value.
    Use the member name for the archive directory.
    """
    if isinstance(report_type, ReportType):
        normalized_type = report_type

    elif isinstance(report_type, str):
        try:
            # Supports database strings such as "PAYMENTS".
            normalized_type = ReportType[report_type]
        except KeyError:
            # Also supports the enum's actual value.
            normalized_type = ReportType(report_type)

    else:
        raise TypeError(
            "report_type must be a ReportType or string"
        )

    if (
        not isinstance(report_date, date)
        or isinstance(report_date, datetime)
    ):
        raise TypeError(
            "report_date must be a date, not a datetime"
        )

    if (
        not isinstance(checksum, str)
        or len(checksum) != 64
        or any(
            character not in "0123456789abcdef"
            for character in checksum
        )
    ):
        raise ValueError(
            "checksum must be a lowercase SHA-256 hexadecimal string"
        )

    return (
        Path(normalized_type.name)
        / report_date.isoformat()
        / checksum[:2]
        / f"{checksum}.json"
    )


def _within_root(root: Path, relative_path: str) -> Path:
    relative = Path(relative_path)

    if relative.is_absolute() or ".." in relative.parts:
        raise ArchiveIntegrityError(
            "Expected a relative path without parent traversal"
        )

    resolved = (root / relative).resolve()

    if resolved == root or not resolved.is_relative_to(root):
        raise ArchiveIntegrityError("File path escapes its configured root")

    return resolved


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | os.O_DIRECTORY,
    )

    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _durable_mkdir(path: Path) -> None:
    if path.exists():
        if not path.is_dir():
            raise ArchiveIntegrityError(
                f"Expected a directory: {path}"
            )
        return

    _durable_mkdir(path.parent)

    try:
        path.mkdir()
    except FileExistsError:
        if not path.is_dir():
            raise

    _fsync_directory(path.parent)
    _fsync_directory(path)


def _cleanup_temporary(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        # A leftover uniquely named temporary file is not a final object.
        # Report cleanup failure without hiding the original exception.
        LOGGER.warning(
            "Could not remove archival temporary file %s",
            path,
            exc_info=True,
        )


def _install_verified_object(
    *,
    source: Path,
    destination: Path,
    checksum: str,
    failure_hook: FailureHook,
) -> bool:
    """Return True when an existing matching object was reused.

    Caller must hold ARCHIVE_STATE_LOCK.
    """

    if destination.exists():
        verify_checksum(destination, checksum)
        failure_hook("after_final_checksum")
        return True

    verify_checksum(source, checksum)
    _durable_mkdir(destination.parent)

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{checksum}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    temporary = Path(temporary_name)

    try:
        with os.fdopen(descriptor, "wb") as writer:
            with source.open("rb") as reader:
                shutil.copyfileobj(
                    reader,
                    writer,
                    length=1024 * 1024,
                )

            writer.flush()
            failure_hook("after_copy")

            os.fsync(writer.fileno())
            failure_hook("after_temp_fsync")

        verify_checksum(temporary, checksum)
        failure_hook("after_temp_checksum")

        # Recheck before replacement. Cooperating archive workers are
        # serialized by the archive advisory lock.
        if destination.exists():
            verify_checksum(destination, checksum)
            reused = True
        else:
            os.replace(temporary, destination)
            reused = False

        _fsync_directory(destination.parent)
        failure_hook("after_rename")

        verify_checksum(destination, checksum)
        failure_hook("after_final_checksum")

        return reused

    finally:
        _cleanup_temporary(temporary)


def _restore_source(
    *,
    source: Path,
    destination: Path,
    checksum: str,
) -> None:
    if source.exists() or not destination.exists():
        return

    # Never restore from an unverified archive.
    verify_checksum(destination, checksum)

    _install_verified_object(
        source=destination,
        destination=source,
        checksum=checksum,
        failure_hook=lambda point: None,
    )


class ArchiveService:
    def __init__(
        self,
        *,
        database_url: str,
        inbox_root: Path | str,
        archive_root: Path | str,
        failure_hook: FailureHook | None = None,
    ) -> None:
        if os.name != "posix":
            raise RuntimeError("Run this archival service inside WSL/Linux")

        self.database_url = database_url
        self.inbox_root = Path(inbox_root).resolve()
        self.archive_root = Path(archive_root).resolve()
        self.failure_hook = failure_hook or (lambda point: None)

        if (
            self.inbox_root.is_relative_to(self.archive_root)
            or self.archive_root.is_relative_to(self.inbox_root)
        ):
            raise ValueError(
                "Inbox and archive roots must not overlap"
            )

    def archive_batch(
        self,
        batch_id: UUID | str,
        *,
        source_relative_path: str | None = None,
    ) -> ArchiveOutcome:
        batch_id = UUID(str(batch_id))

        with psycopg.connect(
            self.database_url,
            autocommit=True,
            row_factory=dict_row,
        ) as connection:
            connection.execute(
                "SELECT pg_advisory_lock(%s)",
                (ARCHIVE_STATE_LOCK,),
            )

            # Closing this dedicated connection releases the session lock.
            batch = self._prepare_attempt(
                connection,
                batch_id,
                source_relative_path,
            )

            source = _within_root(
                self.inbox_root,
                batch["source_relative_path"],
            )
            destination = _within_root(
                self.archive_root,
                batch["archive_path"],
            )
            checksum = batch["file_checksum"]

            marker_attempted = False

            try:
                reused = _install_verified_object(
                    source=source,
                    destination=destination,
                    checksum=checksum,
                    failure_hook=self.failure_hook,
                )

                if source.exists():
                    # Detect a changed source before deleting it.
                    verify_checksum(source, checksum)
                    source.unlink()
                    _fsync_directory(source.parent)

                self.failure_hook("after_source_remove")
                self.failure_hook("before_status_update")

                marker_attempted = True

                with connection.transaction():
                    connection.execute(
                        """
                        UPDATE ingestion_batch
                        SET
                            archive_status = 'ARCHIVED',
                            archived_at_utc = COALESCE(
                                archived_at_utc,
                                clock_timestamp()
                            ),
                            archive_error_message = NULL
                        WHERE batch_id = %s
                          AND ingestion_status = 'COMMITTED'
                        """,
                        (batch_id,),
                    )

                return ArchiveOutcome(
                    batch_id=batch_id,
                    archive_path=str(destination),
                    file_checksum=checksum,
                    reused_existing_object=reused,
                )

            except Exception as error:
                try:
                    _restore_source(
                        source=source,
                        destination=destination,
                        checksum=checksum,
                    )
                except Exception as restoration_error:
                    error.add_note(
                        "Source restoration failed: "
                        f"{type(restoration_error).__name__}: "
                        f"{restoration_error}"
                    )

                try:
                    self._record_failure(
                        connection,
                        batch_id,
                        error,
                        preserve_committed_marker=marker_attempted,
                    )
                except Exception as metadata_error:
                    error.add_note(
                        "Archive failure metadata could not be recorded: "
                        f"{type(metadata_error).__name__}: "
                        f"{metadata_error}"
                    )

                raise

    def _prepare_attempt(
        self,
        connection: psycopg.Connection,
        batch_id: UUID,
        source_relative_path: str | None,
    ) -> dict:
        with connection.transaction():
            batch = connection.execute(
                """
                SELECT *
                FROM ingestion_batch
                WHERE batch_id = %s
                FOR UPDATE
                """,
                (batch_id,),
            ).fetchone()

            if batch is None:
                raise ArchiveError("Batch does not exist")

            if batch["ingestion_status"] != "COMMITTED":
                raise ArchiveError(
                    "Only committed batches can be archived"
                )

            saved_source = batch["source_relative_path"]

            if source_relative_path is not None:
                _within_root(self.inbox_root, source_relative_path)

                if saved_source not in (None, source_relative_path):
                    raise ArchiveIntegrityError(
                        "Retry cannot change the recorded source path"
                    )

                saved_source = source_relative_path

            if saved_source is None:
                raise ArchiveError(
                    "Supply source_relative_path on the first attempt"
                )

            _within_root(self.inbox_root, saved_source)

            relative_destination = archive_relative_path(
                batch["report_type"],
                batch["report_date"],
                batch["file_checksum"],
            ).as_posix()

            if batch["archive_path"] not in (
                None,
                relative_destination,
            ):
                raise ArchiveIntegrityError(
                    "Recorded archive path differs from the "
                    "content-addressed destination"
                )

            connection.execute(
                """
                UPDATE ingestion_batch
                SET
                    source_relative_path = %s,
                    archive_path = %s,
                    archive_attempt_count = archive_attempt_count + 1,
                    archive_last_attempt_at_utc = clock_timestamp(),
                    archive_error_message = NULL
                WHERE batch_id = %s
                """,
                (
                    saved_source,
                    relative_destination,
                    batch_id,
                ),
            )

            batch["source_relative_path"] = saved_source
            batch["archive_path"] = relative_destination

            return batch

    def _record_failure(
        self,
        connection: psycopg.Connection,
        batch_id: UUID,
        error: Exception,
        *,
        preserve_committed_marker: bool,
    ) -> None:
        message = f"{type(error).__name__}: {error}"

        notes = getattr(error, "__notes__", ())
        if notes:
            message += " | " + " | ".join(notes)

        with connection.transaction():
            connection.execute(
                """
                UPDATE ingestion_batch
                SET
                    archive_status = 'FAILED',
                    archived_at_utc = NULL,
                    archive_error_message = %s
                WHERE batch_id = %s
                  AND ingestion_status = 'COMMITTED'
                  AND (
                      NOT %s
                      OR archive_status <> 'ARCHIVED'
                  )
                """,
                (
                    message[:4000],
                    batch_id,
                    preserve_committed_marker,
                ),
            )