package notification

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/smtp"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	eventsv1 "grpcshop/gen/shop/events/v1"
)

// SMTPNotifier emails customers over SMTP, refusing to send unless STARTTLS succeeds.
type SMTPNotifier struct {
	Address     string // host:port
	From        string
	EmailDomain string // customers are addressed as <customer_id>@EmailDomain
	TLSConfig   *tls.Config
	Logger      *slog.Logger
}

func (notifier SMTPNotifier) SendOrderConfirmation(ctx context.Context, event *eventsv1.OrderConfirmed) error {
	return notifier.send(ctx, event.GetCustomerId(),
		fmt.Sprintf("Order %s confirmed", event.GetOrderId()),
		fmt.Sprintf("Thanks for your order!\r\n\r\nOrder: %s\r\nItems: %d\r\nTotal: $%s\r\n",
			event.GetOrderId(), event.GetItemCount(), formatCents(event.GetTotalCents())))
}

func (notifier SMTPNotifier) SendOrderCancellation(ctx context.Context, event *eventsv1.OrderCancelled) error {
	return notifier.send(ctx, event.GetCustomerId(),
		fmt.Sprintf("Order %s cancelled", event.GetOrderId()),
		fmt.Sprintf("Your order %s was cancelled.\r\nRefunded: $%s\r\n",
			event.GetOrderId(), formatCents(event.GetRefundedCents())))
}

func (notifier SMTPNotifier) send(ctx context.Context, customerID, subject, body string) (err error) {
	ctx, span := tracer.Start(ctx, "smtp send", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("server.address", notifier.Address)))
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()
	if strings.ContainsAny(customerID, "\r\n<>,;: ") {
		return fmt.Errorf("refusing unsafe customer id %q", customerID)
	}
	recipient := customerID + "@" + notifier.EmailDomain

	host, _, err := net.SplitHostPort(notifier.Address)
	if err != nil {
		return err
	}
	dialer := net.Dialer{Timeout: 5 * time.Second}
	connection, err := dialer.DialContext(ctx, "tcp", notifier.Address)
	if err != nil {
		return err
	}
	client, err := smtp.NewClient(connection, host)
	if err != nil {
		connection.Close()
		return err
	}
	defer client.Close()

	tlsConfig := notifier.TLSConfig.Clone()
	tlsConfig.ServerName = host
	if err := client.StartTLS(tlsConfig); err != nil {
		return fmt.Errorf("starttls: %w", err)
	}
	if err := client.Mail(notifier.From); err != nil {
		return err
	}
	if err := client.Rcpt(recipient); err != nil {
		return err
	}
	writer, err := client.Data()
	if err != nil {
		return err
	}
	message := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n%s",
		notifier.From, recipient, subject, time.Now().UTC().Format(time.RFC1123Z), body)
	if _, err := writer.Write([]byte(message)); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	notifier.Logger.InfoContext(ctx, "email sent", "to", recipient, "subject", subject)
	return client.Quit()
}

func formatCents(cents int64) string {
	return fmt.Sprintf("%d.%02d", cents/100, cents%100)
}
