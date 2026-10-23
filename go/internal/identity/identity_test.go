package identity

import (
	"context"
	"encoding/base64"
	"testing"

	"google.golang.org/grpc/metadata"
)

func contextWithPayload(payload string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(JWTPayloadMetadataKey, payload))
}

func TestCustomerIDReadsSubjectClaim(t *testing.T) {
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"customer-42","aud":"grpcshop-api"}`))

	customerID, err := CustomerID(contextWithPayload(payload))

	if err != nil || customerID != "customer-42" {
		t.Fatalf("got (%q, %v), want customer-42", customerID, err)
	}
}

func TestCustomerIDRejectsMissingOrMalformedPayload(t *testing.T) {
	testCases := map[string]context.Context{
		"no metadata":   context.Background(),
		"not base64":    contextWithPayload("%%%"),
		"not json":      contextWithPayload(base64.RawURLEncoding.EncodeToString([]byte("nope"))),
		"empty subject": contextWithPayload(base64.RawURLEncoding.EncodeToString([]byte(`{"sub":""}`))),
		"header injection": contextWithPayload(base64.RawURLEncoding.EncodeToString(
			[]byte(`{"sub":"x\r\nBcc: everyone@example.com"}`))),
		"duplicate header": metadata.NewIncomingContext(context.Background(), metadata.Pairs(
			JWTPayloadMetadataKey, base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"a"}`)),
			JWTPayloadMetadataKey, base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"b"}`)))),
	}
	for name, ctx := range testCases {
		t.Run(name, func(t *testing.T) {
			if _, err := CustomerID(ctx); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
