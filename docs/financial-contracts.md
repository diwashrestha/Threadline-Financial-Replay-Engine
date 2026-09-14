# Threadline Financial Contracts

## 1. Contract version

This document defines `threadline_financial_contract_v1`. Every reconciliation run records both the financial-contract version and the fee-schedule version used to produce its results.

These are fictional Threadline and MockPay rules created for a reproducible engineering project. They are not claims about a real payment provider.

## 2. Shared representation rules

### Money

- Currency is `EUR` in the MVP.
- Source files encode money as base-10 strings with exactly two fractional digits, for example `"19.99"`.
- Python uses `Decimal`; PostgreSQL uses `NUMERIC(18,2)`.
- Binary floating point must not be used for persisted financial calculations.
- Calculated fees use `ROUND_HALF_UP` to two fractional digits.
- Domain amounts are non-negative magnitudes.
- Only `settlement_lines.signed_amount` uses signs: captures are positive; refunds and fees are negative.

### Time

- Event timestamps are ISO-8601 timestamps with an explicit offset and are normalized to UTC.
- The business timezone is `Europe/Berlin`.
- A business date is interpreted in that timezone.
- `effective_at_utc` says when a business event happened.
- `available_on` says the MockPay payout date for which a movement is eligible.
- `received_at_utc` says when Threadline received the source record.
- `evaluation_cutoff_utc` says which received evidence a reconciliation run may use.
- Records received after the cutoff cannot affect that run, even if their effective time is earlier.

### Source identity and versions

The logical identity of a source entity is:

```text
(source_system, entity_type, source_id)
```

Every mutable source entity has a positive integer `source_version`. The pipeline retains every received version and derives current state as follows:

| Condition | Deterministic treatment |
|---|---|
| Same identity, version, and content hash | Duplicate delivery; retain receipt evidence and apply no second financial effect |
| Same identity and version, different content hash | Conflict; mark the entity `CONFLICTED` and exclude it from trusted results until a higher valid version resolves it |
| Higher valid version | Correction; make it current and recompute affected results |
| Lower version | Stale delivery; retain for audit and leave current state unchanged |

Arrival order never selects a winner between conflicting records of the same version.

The identity of a delivered file is `(source_system, report_type, batch_id)`. Reusing that identity with a different checksum produces `BATCH_CONFLICT`. Reusing it with the same checksum is a duplicate file delivery.

## 3. Source datasets

### Orders

**Grain:** one version of one Threadline order.

**Logical identity:** `(threadline_shop, ORDER, order_id)`.

| Field | Type | Required | Contract |
|---|---|---:|---|
| `order_id` | string | yes | Stable source identifier |
| `created_at_utc` | timestamp | yes | Explicit offset; normalized to UTC |
| `status` | enum | yes | `PAYMENT_PENDING`, `PAID`, or `CANCELLED` |
| `currency` | string | yes | Must equal `EUR` |
| `order_total` | decimal | yes | Greater than zero |
| `source_version` | integer | yes | Positive and increasing for corrections |

A `PAID` order expects captured payments equal to `order_total`. A payment captured for a nonexistent order produces `ORPHAN_PAYMENT`; a capture for a `CANCELLED` order produces `CAPTURE_FOR_NON_PAYABLE_ORDER`.

### Payments

**Grain:** one version of one payment attempt.

**Logical identity:** `(mockpay, PAYMENT, payment_id)`.

| Field | Type | Required | Contract |
|---|---|---:|---|
| `payment_id` | string | yes | Stable MockPay payment-attempt identifier |
| `order_id` | string | yes | Referenced Threadline order |
| `attempt_number` | integer | yes | Positive within an order |
| `payment_method` | enum | yes | `CARD` or `WALLET` |
| `status` | enum | yes | `FAILED` or `CAPTURED` |
| `amount` | decimal | yes | Requested amount; financially effective only when captured |
| `currency` | string | yes | Must equal `EUR` |
| `effective_at_utc` | timestamp | yes | Attempt/capture time |
| `available_on` | date | conditional | Required for `CAPTURED`; absent for `FAILED` |
| `source_version` | integer | yes | Positive source version |

Failed attempts contribute zero to captured totals and incur no fee. Different payment IDs are different attempts, even when they reference the same order.

### Refunds

**Grain:** one version of one refund attempt.

**Logical identity:** `(mockpay, REFUND, refund_id)`.

| Field | Type | Required | Contract |
|---|---|---:|---|
| `refund_id` | string | yes | Stable MockPay refund identifier |
| `payment_id` | string | yes | Referenced captured payment |
| `status` | enum | yes | `FAILED` or `SUCCEEDED` |
| `amount` | decimal | yes | Greater than zero |
| `currency` | string | yes | Must equal `EUR` |
| `effective_at_utc` | timestamp | yes | Refund processing time |
| `available_on` | date | conditional | Required when successful |
| `source_version` | integer | yes | Positive source version |

Only successful refunds affect money. Cumulative successful refunds for a payment must not exceed its captured amount.

### Fees

**Grain:** one version of one reported MockPay fee.

**Logical identity:** `(mockpay, FEE, fee_id)`.

| Field | Type | Required | Contract |
|---|---|---:|---|
| `fee_id` | string | yes | Stable MockPay fee identifier |
| `payment_id` | string | yes | Referenced captured payment |
| `fee_type` | enum | yes | `PROCESSING` |
| `amount` | decimal | yes | Positive magnitude |
| `currency` | string | yes | Must equal `EUR` |
| `effective_at_utc` | timestamp | yes | Fee assessment time |
| `available_on` | date | yes | Same payout-eligibility date as the capture in v1 |
| `source_version` | integer | yes | Positive source version |

The MVP expects exactly one processing-fee entity per successful capture. Failed payment attempts have no fee. Original processing fees are not reversed when a refund succeeds.

### Settlement lines

**Grain:** one version of one itemized movement in a MockPay payout.

**Logical identity:** `(mockpay, SETTLEMENT_LINE, settlement_line_id)`.

| Field | Type | Required | Contract |
|---|---|---:|---|
| `settlement_line_id` | string | yes | Stable line identifier |
| `payout_id` | string | yes | Referenced payout |
| `movement_type` | enum | yes | `CAPTURE`, `REFUND`, or `FEE` |
| `movement_id` | string | yes | Payment, refund, or fee ID according to type |
| `signed_amount` | decimal | yes | Capture positive; refund and fee negative |
| `currency` | string | yes | Must equal `EUR` |
| `source_version` | integer | yes | Positive source version |

One logical financial movement may appear in at most one current settlement line. Two different line IDs referencing the same current movement produce `DUPLICATE_SETTLEMENT_MOVEMENT`.

### Payouts

**Grain:** one version of one MockPay payout header.

**Logical identity:** `(mockpay, PAYOUT, payout_id)`.

| Field | Type | Required | Contract |
|---|---|---:|---|
| `payout_id` | string | yes | Stable payout identifier |
| `payout_date` | date | yes | MockPay availability date represented by this payout |
| `currency` | string | yes | Must equal `EUR` |
| `reported_net_amount` | decimal | yes | Amount MockPay says it paid |
| `source_version` | integer | yes | Positive source version |

The MVP has at most one current payout header per payout date and currency.

### Batch manifests

**Grain:** one manifest for one source report file.

| Field | Type | Required | Contract |
|---|---|---:|---|
| `batch_id` | string | yes | Stable delivery identity |
| `source_system` | enum | yes | `threadline_shop` or `mockpay` |
| `report_type` | enum | yes | One of the six source datasets |
| `business_date` | date | yes | Source reporting date |
| `schema_version` | integer | yes | Supported positive version |
| `generated_at_utc` | timestamp | yes | Source generation timestamp |
| `row_count` | integer | yes | Must equal parsed source-row count |
| `sha256` | string | yes | Must equal the file checksum |

A file is complete only when both its data file and valid manifest are visible. Files without a valid manifest remain unprocessed rather than being treated as empty reports.

## 4. MockPay fee schedule v1

| Payment method | Fixed component | Percentage component |
|---|---:|---:|
| `CARD` | EUR 0.20 | 1.80% |
| `WALLET` | EUR 0.25 | 2.00% |

For each captured payment:

```text
expected_fee = round_half_up(fixed_component + amount * percentage_component, 2)
```

Example for a EUR 19.99 card capture:

```text
0.20 + (19.99 * 0.018) = 0.55982
expected_fee = 0.56
```

## 5. Reconciliation rules

### Order collection

For each current, valid `PAID` order:

```text
captured_total = sum(current CAPTURED payment amounts)
collection_variance = captured_total - order_total
```

- Zero captured payments after a complete payment report and its deadline produces `MISSING_PAYMENT`.
- A nonzero variance produces `PAYMENT_AMOUNT_MISMATCH`.
- More than one captured payment produces `MULTIPLE_CAPTURE`, even when their sum equals the order total.
- Rules are evaluated independently, so an order may have both `MULTIPLE_CAPTURE` and `PAYMENT_AMOUNT_MISMATCH`.

### Refund integrity

For each successful refund:

- its referenced payment must exist and be captured;
- its currency must equal the payment currency;
- cumulative successful refunds must not exceed the capture.

Violations produce `ORPHAN_REFUND`, `REFUND_CURRENCY_MISMATCH`, or `EXCESS_REFUND` respectively.

### Fee integrity

For each captured payment:

```text
fee_variance = reported_processing_fee - expected_processing_fee
```

- No reported fee after a complete fee report produces `MISSING_FEE`.
- A nonzero variance produces `FEE_MISMATCH`.
- A fee attached to a failed or missing payment produces `UNEXPECTED_FEE`.

### Expected payout movements

The engine constructs expected signed movements from accepted business evidence:

```text
CAPTURE = +captured payment amount
REFUND  = -successful refund amount
FEE     = -expected processing fee
```

For payout date `D`:

```text
expected_payout(D)
  = sum(expected signed movements where available_on = D)
```

Each expected movement is compared with settlement lines by `(movement_type, movement_id)`. A missing eligible movement after the settlement deadline produces `MISSING_SETTLEMENT_LINE`. A line with the wrong amount produces `SETTLEMENT_LINE_AMOUNT_MISMATCH`.

### Payout integrity

For each payout:

```text
reported_line_total
  = sum(current settlement line signed_amount for payout_id)

provider_report_variance
  = reported_net_amount - reported_line_total

end_to_end_payout_variance
  = reported_net_amount - expected_payout(payout_date)
```

- Nonzero `provider_report_variance` produces `PAYOUT_TOTAL_MISMATCH`.
- Nonzero `end_to_end_payout_variance` is retained as the payout's unexplained end-to-end variance.
- Transaction exception amounts may overlap and must not be summed to derive unexplained payout value. The payout-level variance is authoritative for that measure.

## 6. Source deadlines and completeness

The following are fictional MockPay service-level assumptions for the project. They are interpreted in `Europe/Berlin` for the previous business date.

| Report | Deadline on the following day |
|---|---:|
| Orders | 01:00 |
| Payments | 02:00 |
| Refunds | 02:00 |
| Fees | 12:00 |
| Settlement lines | 12:00 |
| Payouts | 12:00 |

The main daily evaluation cutoff is 12:30. A catch-up evaluation runs at 16:00 only when new evidence or a deadline transition makes results stale.

| Evidence state | Meaning |
|---|---|
| `PENDING` | Required report has not arrived, but its deadline has not passed |
| `INCOMPLETE` | Required report is absent or invalid after its deadline |
| `COMPLETE` | Valid file and manifest are available at the evaluation cutoff |

Missing entity exceptions such as `MISSING_PAYMENT` are emitted only when the corresponding report is `COMPLETE`. A missing or invalid report produces `SOURCE_REPORT_MISSING` or `SOURCE_REPORT_INVALID`, and affected financial results cannot be `RECONCILED`.

## 7. Result states and publication

Transaction and payout results use these states:

| State | Meaning |
|---|---|
| `PENDING` | Required evidence is not due yet |
| `INCOMPLETE` | Required evidence is overdue or invalid |
| `RECONCILED` | All required evidence is complete and every blocking rule passes |
| `EXCEPTION` | Evidence is complete and one or more blocking financial rules fail |

Every run builds candidate results under a unique `run_id`. Candidate results become published only after required validation succeeds. A failed run leaves the previous published result unchanged.

## 8. Exception contract

The MVP exception taxonomy is:

```text
BATCH_CONFLICT
CAPTURE_FOR_NON_PAYABLE_ORDER
CONFLICTING_SOURCE_VERSION
DUPLICATE_SETTLEMENT_MOVEMENT
EXCESS_REFUND
FEE_MISMATCH
MISSING_FEE
MISSING_PAYMENT
MISSING_SETTLEMENT_LINE
MULTIPLE_CAPTURE
ORPHAN_PAYMENT
ORPHAN_REFUND
PAYMENT_AMOUNT_MISMATCH
PAYOUT_TOTAL_MISMATCH
REFUND_CURRENCY_MISMATCH
SETTLEMENT_LINE_AMOUNT_MISMATCH
SOURCE_REPORT_INVALID
SOURCE_REPORT_MISSING
UNEXPECTED_FEE
```

Invalid row structure, unsupported schema versions, invalid timestamps, invalid currency, and invalid decimal encodings are quarantine reasons. They do not become transaction-level financial exceptions until source completeness is evaluated.

One entity may have multiple exceptions. Each exception records:

```text
run_id
contract_version
rule_id
exception_type
entity_type
entity_id
expected_amount
actual_amount
variance
detected_at_utc
status
supporting_source_record_ids
```

## 9. Required outputs

| Output | Grain |
|---|---|
| `transaction_reconciliation` | one order per reconciliation run |
| `payout_reconciliation` | one payout per reconciliation run |
| `reconciliation_exceptions` | one failed rule per entity per reconciliation run |
| `source_completeness` | one report type and business date per reconciliation run |
| `quarantine_records` | one rejected source row or file-level defect |

## 10. Falsifiable invariants

### Deterministic replay

```text
same accepted source versions + same rules + same cutoff
=> same financial results
```

### Duplicate safety

```text
result(history + identical deliveries) == result(history)
```

### Arrival-order independence

```text
result(permutation(history)) == result(history)
```

This applies after all valid records have arrived and conflicts are represented explicitly.

### Incremental/full-rebuild equivalence

```text
incremental_result(history, cutoff) == full_rebuild_result(history, cutoff)
```

Equality includes totals, allocations, completeness, and exception states.

### Evidence conservation

Every received record has exactly one auditable disposition:

```text
accepted_current | accepted_historical | duplicate | stale | conflicted | quarantined
```

### No false reconciliation

```text
required evidence incomplete => result != RECONCILED
```

### Payout arithmetic

For a fully reconciled payout:

```text
reported_net_amount
  == sum(reported settlement lines)
  == sum(expected eligible movements)
```

### Atomic publication

```text
failed candidate run => published_run_id remains unchanged
```

## 11. Decisions deferred beyond v1

- Negative provider balances and payout carry-forward
- Fee reversals and refund fees
- Provider reserves and manual adjustments
- Multiple captures and split tender as valid business behavior
- Chargebacks and disputes
- Multi-currency conversion
- Bank statement confirmation
- Product-level return reconciliation
