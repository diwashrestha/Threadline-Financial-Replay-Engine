"""Entry point for checking and registering source deliveries."""

from threadline.file_ingestion import ingest_file
from threadline.source_files import FileGateState


def run_ingestion(source_path, *, ledger_service):
    result = ingest_file(
        source_path,
        ledger_service=ledger_service,
    )

    print("File state:", result.state.value)
    print("Reason:", result.reason_code)

    if result.state is FileGateState.READY:
        print("Ledger result:", result.ledger_result)
        # Your existing canonical-resolution and commit steps
        # can be connected here.

    return result