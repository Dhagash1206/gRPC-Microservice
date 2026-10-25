// Package grpcmw holds gRPC interceptors shared by the Go services.
package grpcmw

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const RequestIDMetadataKey = "x-request-id"

// Health and reflection stay reachable by any CA-signed peer (probes, grpcurl).
var authorizationExemptPrefixes = []string{"/grpc.health.v1.Health/", "/grpc.reflection."}

type requestIDKey struct{}

func RequestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDKey{}).(string)
	return requestID
}

func ContextWithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, requestID)
}

// PeerCommonName returns the CN of the verified mTLS client certificate, or "".
func PeerCommonName(ctx context.Context) string {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return ""
	}
	return tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
}

// UnaryServerObservability attaches the incoming (or a new) request ID to the
// context and logs every call with caller, status and latency.
func UnaryServerObservability(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		requestID := firstMetadataValue(ctx, RequestIDMetadataKey)
		if requestID == "" {
			requestID = uuid.NewString()[:12]
		}
		ctx = ContextWithRequestID(ctx, requestID)
		// Lets a request ID quoted by a client be found in the tracing backend.
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("request.id", requestID))

		startTime := time.Now()
		response, err := handler(ctx, request)
		logger.InfoContext(ctx, "rpc",
			"rid", requestID,
			"caller", PeerCommonName(ctx),
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"duration_ms", float64(time.Since(startTime).Microseconds())/1000,
		)
		return response, err
	}
}

// UnaryServerPeerAuthorization rejects callers whose certificate CN is not allowed.
func UnaryServerPeerAuthorization(allowedCallers []string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		for _, prefix := range authorizationExemptPrefixes {
			if strings.HasPrefix(info.FullMethod, prefix) {
				return handler(ctx, request)
			}
		}
		callerName := PeerCommonName(ctx)
		if !slices.Contains(allowedCallers, callerName) {
			return nil, status.Errorf(codes.PermissionDenied, "caller %q not allowed", callerName)
		}
		return handler(ctx, request)
	}
}

// UnaryClientRequestIDPropagation forwards the request ID to downstream services.
func UnaryClientRequestIDPropagation() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, reply any, conn *grpc.ClientConn, invoker grpc.UnaryInvoker, options ...grpc.CallOption) error {
		if requestID := RequestIDFromContext(ctx); requestID != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, RequestIDMetadataKey, requestID)
		}
		return invoker(ctx, method, request, reply, conn, options...)
	}
}

func firstMetadataValue(ctx context.Context, key string) string {
	incomingMetadata, _ := metadata.FromIncomingContext(ctx)
	if values := incomingMetadata.Get(key); len(values) > 0 {
		return values[0]
	}
	return ""
}
