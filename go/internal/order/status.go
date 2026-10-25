package order

import (
	orderv1 "grpcshop/gen/shop/order/v1"
)

// Database representation of each order status.
var statusNames = map[orderv1.OrderStatus]string{
	orderv1.OrderStatus_ORDER_STATUS_PENDING:      "pending",
	orderv1.OrderStatus_ORDER_STATUS_CONFIRMED:    "confirmed",
	orderv1.OrderStatus_ORDER_STATUS_COMPENSATING: "compensating",
	orderv1.OrderStatus_ORDER_STATUS_FAILED:       "failed",
	orderv1.OrderStatus_ORDER_STATUS_CANCELLING:   "cancelling",
	orderv1.OrderStatus_ORDER_STATUS_CANCELLED:    "cancelled",
}

var statusesByName = func() map[string]orderv1.OrderStatus {
	statuses := make(map[string]orderv1.OrderStatus, len(statusNames))
	for status, name := range statusNames {
		statuses[name] = status
	}
	return statuses
}()

func statusName(status orderv1.OrderStatus) string {
	return statusNames[status]
}

func statusFromName(name string) orderv1.OrderStatus {
	return statusesByName[name]
}
