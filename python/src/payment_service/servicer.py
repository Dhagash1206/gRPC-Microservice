import logging

import grpc
from opentelemetry import metrics

from payment_service.repository import (
    STATUS_CANCELLED,
    STATUS_DECLINED,
    STATUS_REFUNDED,
    STATUS_SUCCEEDED,
    PaymentRepository,
)
from shop.payment.v1 import payment_pb2, payment_pb2_grpc

logger = logging.getLogger(__name__)

meter = metrics.get_meter("grpcshop.payment")
charge_results = meter.create_counter("grpcshop.payment.charges", description="Charge results by stored status")
charged_cents = meter.create_counter("grpcshop.payment.charged_cents", unit="cents", description="Amount charged")
cancel_results = meter.create_counter("grpcshop.payment.cancellations", description="CancelCharge outcomes")

CANCEL_OUTCOME_BY_STATUS = {
    STATUS_CANCELLED: payment_pb2.CANCEL_OUTCOME_FENCED,
    STATUS_REFUNDED: payment_pb2.CANCEL_OUTCOME_REFUNDED,
    STATUS_DECLINED: payment_pb2.CANCEL_OUTCOME_NOTHING_TO_REFUND,
}


class PaymentServicer(payment_pb2_grpc.PaymentServiceServicer):
    """Fake processor: declines any charge above max_charge_cents."""

    def __init__(self, repository: PaymentRepository, max_charge_cents: int) -> None:
        self._repository = repository
        self._max_charge_cents = max_charge_cents

    def Charge(self, request, context):
        if not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required")
        if not request.customer_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "customer_id is required")
        if request.amount_cents <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "amount_cents must be positive")

        if request.amount_cents > self._max_charge_cents:
            status = STATUS_DECLINED
            decline_reason = f"payment declined: {request.amount_cents} cents exceeds limit of {self._max_charge_cents}"
        else:
            status, decline_reason = STATUS_SUCCEEDED, None

        payment = self._repository.record_once(
            request.idempotency_key, request.customer_id, request.amount_cents, status, decline_reason
        )

        charge_results.add(1, {"result": payment.status})
        # Checked before parameters: a fence row has no customer or amount to compare.
        if payment.status in (STATUS_CANCELLED, STATUS_REFUNDED):
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, f"charge was {payment.status}")
        if (payment.customer_id, payment.amount_cents) != (request.customer_id, request.amount_cents):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key reused with different parameters")
        if payment.status == STATUS_DECLINED:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, payment.decline_reason)

        charged_cents.add(payment.amount_cents)
        logger.info("charged %d cents to %s (%s)", payment.amount_cents, payment.customer_id, payment.id)
        return payment_pb2.ChargeResponse(payment_id=payment.id)

    def CancelCharge(self, request, context):
        if not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required")
        status = self._repository.cancel(request.idempotency_key)
        cancel_results.add(1, {"result": status})
        logger.info("cancel charge %s -> %s", request.idempotency_key, status)
        return payment_pb2.CancelChargeResponse(outcome=CANCEL_OUTCOME_BY_STATUS[status])
