import grpc
import pytest

from shop.payment.v1 import payment_pb2


def charge(payment_stub, idempotency_key="key-1", customer_id="customer-1", amount_cents=500, currency_code="USD"):
    return payment_stub.Charge(
        payment_pb2.ChargeRequest(
            idempotency_key=idempotency_key,
            customer_id=customer_id,
            amount_cents=amount_cents,
            currency_code=currency_code,
        )
    )


def cancel(payment_stub, idempotency_key="key-1") -> int:
    return payment_stub.CancelCharge(payment_pb2.CancelChargeRequest(idempotency_key=idempotency_key)).outcome


def charge_status(payment_stub, idempotency_key="key-1") -> int:
    return payment_stub.GetCharge(payment_pb2.GetChargeRequest(idempotency_key=idempotency_key)).status


def assert_charge_fails(payment_stub, code: grpc.StatusCode, **charge_args) -> str:
    with pytest.raises(grpc.RpcError) as error_info:
        charge(payment_stub, **charge_args)
    assert error_info.value.code() == code
    return error_info.value.details()


def test_charge_succeeds_and_is_idempotent(payment_stub):
    first = charge(payment_stub)
    retried = charge(payment_stub)
    assert first.payment_id.startswith("pay_")
    assert first.payment_id == retried.payment_id


def test_distinct_keys_create_distinct_payments(payment_stub):
    assert charge(payment_stub, idempotency_key="a").payment_id != charge(payment_stub, idempotency_key="b").payment_id


def test_decline_is_persisted_and_replayed(payment_stub):
    for _ in range(2):
        details = assert_charge_fails(payment_stub, grpc.StatusCode.FAILED_PRECONDITION, amount_cents=150_000)
        assert "declined" in details


def test_reusing_key_with_different_amount_is_rejected(payment_stub):
    charge(payment_stub, amount_cents=500)
    assert_charge_fails(payment_stub, grpc.StatusCode.INVALID_ARGUMENT, amount_cents=900)


def test_reusing_key_with_different_currency_is_rejected(payment_stub):
    charge(payment_stub, currency_code="USD")
    assert_charge_fails(payment_stub, grpc.StatusCode.INVALID_ARGUMENT, currency_code="EUR")


@pytest.mark.parametrize("currency_code", ["", "usd", "US", "USDX", "U\x00D"])
def test_malformed_currency_code_is_invalid(payment_stub, currency_code):
    details = assert_charge_fails(payment_stub, grpc.StatusCode.INVALID_ARGUMENT, currency_code=currency_code)
    assert "currency_code" in details


def test_get_charge_reports_each_stored_outcome(payment_stub):
    with pytest.raises(grpc.RpcError) as error_info:
        charge_status(payment_stub, "never-charged")
    assert error_info.value.code() == grpc.StatusCode.NOT_FOUND

    paid = charge(payment_stub, idempotency_key="paid")
    response = payment_stub.GetCharge(payment_pb2.GetChargeRequest(idempotency_key="paid"))
    assert (response.payment_id, response.status) == (paid.payment_id, payment_pb2.CHARGE_STATUS_SUCCEEDED)

    assert_charge_fails(payment_stub, grpc.StatusCode.FAILED_PRECONDITION, idempotency_key="big", amount_cents=150_000)
    assert charge_status(payment_stub, "big") == payment_pb2.CHARGE_STATUS_DECLINED

    cancel(payment_stub, "paid")
    assert charge_status(payment_stub, "paid") == payment_pb2.CHARGE_STATUS_REFUNDED

    cancel(payment_stub, "fenced")
    assert charge_status(payment_stub, "fenced") == payment_pb2.CHARGE_STATUS_CANCELLED


def test_missing_idempotency_key_is_invalid(payment_stub):
    assert_charge_fails(payment_stub, grpc.StatusCode.INVALID_ARGUMENT, idempotency_key="")


def test_cancel_after_success_refunds_and_blocks_retries(payment_stub):
    charge(payment_stub)

    assert cancel(payment_stub) == payment_pb2.CANCEL_OUTCOME_REFUNDED
    assert cancel(payment_stub) == payment_pb2.CANCEL_OUTCOME_REFUNDED
    assert "refunded" in assert_charge_fails(payment_stub, grpc.StatusCode.FAILED_PRECONDITION)


def test_cancel_before_charge_fences_late_charge(payment_stub):
    assert cancel(payment_stub) == payment_pb2.CANCEL_OUTCOME_FENCED

    assert "cancelled" in assert_charge_fails(payment_stub, grpc.StatusCode.FAILED_PRECONDITION)
    assert cancel(payment_stub) == payment_pb2.CANCEL_OUTCOME_FENCED


def test_cancel_of_declined_charge_has_nothing_to_refund(payment_stub):
    assert_charge_fails(payment_stub, grpc.StatusCode.FAILED_PRECONDITION, amount_cents=150_000)
    assert cancel(payment_stub) == payment_pb2.CANCEL_OUTCOME_NOTHING_TO_REFUND
