// Package identity extracts the end-user identity that Envoy forwards after
// validating the JWT. It is trustworthy only because the order service accepts
// connections exclusively from mTLS-authenticated, allowlisted callers (Envoy).
package identity

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"google.golang.org/grpc/metadata"
)

// JWTPayloadMetadataKey matches forward_payload_header in envoy.yaml.
const JWTPayloadMetadataKey = "x-jwt-payload"

var ErrMissingIdentity = errors.New("missing or invalid end-user identity")

// The subject ends up in database rows, logs and email addresses, so anything
// outside this conservative alphabet (e.g. CR/LF header injection) is refused.
var validCustomerID = regexp.MustCompile(`^[A-Za-z0-9._@|-]{1,128}$`)

// CustomerID returns the JWT `sub` claim forwarded by Envoy (base64url-encoded JSON).
func CustomerID(ctx context.Context) (string, error) {
	incomingMetadata, _ := metadata.FromIncomingContext(ctx)
	values := incomingMetadata.Get(JWTPayloadMetadataKey)
	// More than one value means something upstream appended instead of replacing; refuse to guess.
	if len(values) != 1 {
		return "", ErrMissingIdentity
	}
	payloadJSON, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(values[0], "="))
	if err != nil {
		return "", ErrMissingIdentity
	}
	var claims struct {
		Subject string `json:"sub"`
	}
	if err := json.Unmarshal(payloadJSON, &claims); err != nil || !validCustomerID.MatchString(claims.Subject) {
		return "", ErrMissingIdentity
	}
	return claims.Subject, nil
}
