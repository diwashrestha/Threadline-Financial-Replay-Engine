# Threadline Golden Scenarios

## Purpose

These scenarios are the independent, hand-calculated oracle for the Stage 2 reference implementation. Production reconciliation code must not generate its own expected test answers.

Unless a scenario says otherwise:

- all money is EUR;
- all records are structurally valid current version `1` records;
- all required source reports are complete and their deadlines have passed;
- payments use `CARD`;
- the card fee schedule is EUR 0.20 plus 1.80%, rounded half up;
- settlement details are omitted when they are irrelevant to the rule being tested;
- each scenario starts from an empty state and is evaluated independently.

## G01 — Clean order-to-payout flow

### Input

```text
Order ORD-001: PAID, 100.00
Payment PAY-001: CAPTURED, order ORD-001, 100.00, available 2026-01-02
Fee FEE-001: payment PAY-001, 2.00, available 2026-01-02
Settlement SL-001: payout PO-001, CAPTURE/PAY-001, +100.00
Settlement SL-002: payout PO-001, FEE/FEE-001, -2.00
Payout PO-001: payout date 2026-01-02, reported 98.00
```

### Expected

```text
captured_total = 100.00
collection_variance = 0.00
expected_fee = 2.00
fee_variance = 0.00
expected_payout = 98.00
reported_line_total = 98.00
provider_report_variance = 0.00
end_to_end_payout_variance = 0.00
transaction_state = RECONCILED
payout_state = RECONCILED
exceptions = none
```

## G02 — Missing payment after a complete report

### Input

```text
Order ORD-002: PAID, 89.00
Payments report: COMPLETE
Captured payments: none
```

### Expected

```text
captured_total = 0.00
collection_variance = -89.00
transaction_state = EXCEPTION
exception = MISSING_PAYMENT
expected_amount = 89.00
actual_amount = 0.00
variance = -89.00
```

## G03 — Payment report not due yet

### Input

```text
Order ORD-003: PAID, 89.00
Payments report: absent
Evaluation cutoff: before the payment-report deadline
```

### Expected

```text
transaction_state = PENDING
source completeness = PENDING
MISSING_PAYMENT is not emitted
```

## G04 — Payment report overdue

### Input

```text
Order ORD-004: PAID, 89.00
Payments report: absent
Evaluation cutoff: after the payment-report deadline
```

### Expected

```text
transaction_state = INCOMPLETE
source completeness = INCOMPLETE
exception = SOURCE_REPORT_MISSING
MISSING_PAYMENT is not emitted because the report is not complete
```

## G05 — Captured amount differs from order

### Input

```text
Order ORD-005: PAID, 120.00
Payment PAY-005: CAPTURED, 100.00
```

### Expected

```text
captured_total = 100.00
collection_variance = -20.00
transaction_state = EXCEPTION
exception = PAYMENT_AMOUNT_MISMATCH
expected_amount = 120.00
actual_amount = 100.00
variance = -20.00
```

## G06 — Identical payment delivered twice

### Input

```text
Order ORD-006: PAID, 100.00
Payment PAY-006 version 1: CAPTURED, 100.00
The identical PAY-006 version 1 payload is delivered a second time
```

### Expected

```text
current logical payments = 1
captured_total = 100.00
duplicate receipts = 1
financial result equals the result with one delivery
no MULTIPLE_CAPTURE exception
```

## G07 — Conflicting records at the same version

### Input

```text
Order ORD-007: PAID, 100.00
Payment PAY-007 version 1 payload A: CAPTURED, 100.00
Payment PAY-007 version 1 payload B: CAPTURED, 90.00
```

### Expected

```text
PAY-007 state = CONFLICTED
neither payload is selected as trusted current state
transaction_state = INCOMPLETE
exception = CONFLICTING_SOURCE_VERSION
result is independent of which payload arrived first
```

## G08 — Two distinct successful captures

### Input

```text
Order ORD-008: PAID, 100.00
Payment PAY-008-A: CAPTURED, 100.00
Payment PAY-008-B: CAPTURED, 100.00
```

### Expected

```text
current logical payments = 2
captured_total = 200.00
collection_variance = +100.00
transaction_state = EXCEPTION
exceptions:
  MULTIPLE_CAPTURE
  PAYMENT_AMOUNT_MISMATCH
```

## G09 — Orphan payment

### Input

```text
Payment PAY-009: CAPTURED, order ORD-UNKNOWN, 75.00
No order ORD-UNKNOWN exists in a complete order report
```

### Expected

```text
exception = ORPHAN_PAYMENT
actual_amount = 75.00
```

## G10 — Valid partial refund

### Input

```text
Order ORD-010: PAID, 100.00
Payment PAY-010: CAPTURED, 100.00
Fee FEE-010: 2.00
Refund REF-010: SUCCEEDED, payment PAY-010, 30.00
```

### Expected

```text
captured_total = 100.00
successful_refund_total = 30.00
expected_fee = 2.00
lifetime_net_after_fee = 68.00
refund exceptions = none
```

The refund can belong to a later payout and does not invalidate an earlier correctly reconciled payout.

## G11 — Orphan refund

### Input

```text
Refund REF-011: SUCCEEDED, payment PAY-UNKNOWN, 55.00
No payment PAY-UNKNOWN exists in a complete payment report
```

### Expected

```text
exception = ORPHAN_REFUND
actual_amount = 55.00
```

## G12 — Refunds exceed the captured amount

### Input

```text
Payment PAY-012: CAPTURED, 100.00
Refund REF-012-A: SUCCEEDED, 60.00
Refund REF-012-B: SUCCEEDED, 50.00
```

### Expected

```text
successful_refund_total = 110.00
maximum_refundable = 100.00
excess = 10.00
exception = EXCESS_REFUND
expected_amount = 100.00
actual_amount = 110.00
variance = +10.00
```

## G13 — Fee rounding

### Input

```text
Payment PAY-013: CARD, CAPTURED, 19.99
Reported fee FEE-013: 0.56
```

### Expected

```text
unrounded fee = 0.20 + (19.99 * 0.018) = 0.55982
expected fee after ROUND_HALF_UP = 0.56
fee_variance = 0.00
fee exceptions = none
```

## G14 — Incorrect provider fee

### Input

```text
Order ORD-014: PAID, 100.00
Payment PAY-014: CARD, CAPTURED, 100.00, available 2026-01-02
Reported fee FEE-014: 3.00, available 2026-01-02
Settlement line SL-014-A: CAPTURE/PAY-014, +100.00
Settlement line SL-014-B: FEE/FEE-014, -3.00
Payout PO-014: reported 97.00
```

### Expected

```text
expected_fee = 2.00
reported_fee = 3.00
fee_variance = +1.00
reported_line_total = 97.00
provider_report_variance = 0.00
expected_payout = 98.00
end_to_end_payout_variance = -1.00
exception = FEE_MISMATCH
```

MockPay's detail agrees with its payout total, but the charged fee violates the contract.

## G15 — Payout header does not equal itemized lines

### Input

```text
Payment PAY-015: CAPTURED, 100.00, available 2026-01-02
Fee FEE-015: 2.00, available 2026-01-02
Settlement line SL-015-A: +100.00
Settlement line SL-015-B: -2.00
Payout PO-015: reported 97.40
```

### Expected

```text
expected_payout = 98.00
reported_line_total = 98.00
provider_report_variance = 97.40 - 98.00 = -0.60
end_to_end_payout_variance = -0.60
payout_state = EXCEPTION
exception = PAYOUT_TOTAL_MISMATCH
```

## G16 — Missing eligible settlement movement

### Input

```text
Payment PAY-016: CAPTURED, 100.00, available 2026-01-02
Fee FEE-016: expected and reported 2.00, available 2026-01-02
Settlement line: CAPTURE/PAY-016, +100.00
No settlement line exists for FEE/FEE-016
Payout PO-016: reported 100.00
Settlement report is complete and its deadline has passed
```

### Expected

```text
expected_payout = 98.00
reported_line_total = 100.00
provider_report_variance = 0.00
end_to_end_payout_variance = +2.00
exception = MISSING_SETTLEMENT_LINE for FEE-016
```

## G17 — Higher version corrects a payment

### Input sequence

```text
Order ORD-017: PAID, 100.00
Payment PAY-017 version 1: CAPTURED, 90.00
First evaluation: after version 1 is received
Payment PAY-017 version 2: CAPTURED, 100.00
Second evaluation: after version 2 is received
Version 1 is delivered again
```

### Expected

```text
first collection variance = -10.00
first exception = PAYMENT_AMOUNT_MISMATCH

second current version = 2
second collection variance = 0.00
second payment-amount exception = resolved

late version 1 disposition = stale
late version 1 does not change the second result
```

## G18 — Late refund and historical replay

### Input sequence

```text
Day 1: order ORD-018 = 100.00 and capture PAY-018 = 100.00
Day 2: fee 2.00 and payout A = 98.00 reconcile
Day 4: refund REF-018 succeeds for 30.00 and is available for payout B
Day 5: the Day 4 refund report arrives late
The refund report is then delivered identically a second time
```

### Expected

```text
payout A remains reconciled at 98.00
lifetime net after the refund = 100.00 - 2.00 - 30.00 = 68.00
payout B and dependent summaries are recalculated when the refund arrives
the duplicate refund delivery has no additional financial effect
incremental results equal a clean rebuild at the Day 5 cutoff
```

## G19 — Malformed financial amount

### Input

```text
Payment PAY-019 amount = "one hundred"
```

### Expected

```text
source row disposition = quarantined
quarantine reason = INVALID_AMOUNT
PAY-019 does not enter trusted current state
source completeness is evaluated separately from row validity
```

## G20 — Failed candidate publication

### Input sequence

```text
Run RUN-A passes all required checks and is published
Run RUN-B builds candidate results but fails a required validation check
```

### Expected

```text
published_run_id remains RUN-A
RUN-B remains available for diagnosis but is not exposed as trusted current output
```

## Acceptance matrix

| Contract area | Covered by |
|---|---|
| Clean reconciliation | G01 |
| Pending versus missing evidence | G02–G04 |
| Collection mismatch | G05 |
| Duplicate delivery | G06 |
| Same-version conflict | G07 |
| Multiple captures | G08 |
| Orphan payment | G09 |
| Refund validity | G10–G12 |
| Decimal rounding and fee mismatch | G13–G14 |
| Settlement and payout integrity | G15–G16 |
| Corrections and stale versions | G17 |
| Late data and replay | G18 |
| Quarantine | G19 |
| Atomic publication | G20 |

## Stage 2 requirement

Stage 2 must encode these examples as small, readable fixtures and compare reference-implementation output with the expected values above. If implementation reveals an ambiguity, update this contract deliberately and record the contract-version change rather than silently changing a test expectation.
