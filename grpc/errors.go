package grpc

import (
	"errors"

	tigerbeetle_go "github.com/tigerbeetle/tigerbeetle-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// translateTBError maps TigerBeetle 0.17 sentinel errors to gRPC status
// codes so clients can react programmatically. Unknown errors fall
// through as codes.Internal with the original message.
func translateTBError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, tigerbeetle_go.ErrTooMuchData):
		// Caller requested a larger batch / page than TB will return.
		// Reducing Limit on AccountFilter/QueryFilter fixes it.
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, tigerbeetle_go.ErrClientEvicted),
		errors.Is(err, tigerbeetle_go.ErrClientClosed),
		errors.Is(err, tigerbeetle_go.ErrNetworkSubsystem),
		errors.Is(err, tigerbeetle_go.ErrOutOfMemory),
		errors.Is(err, tigerbeetle_go.ErrSystemResources):
		// The server's native TB client is no longer usable. Caller should
		// retry; operator likely needs to investigate or restart the API.
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, tigerbeetle_go.ErrClientReleaseTooLow),
		errors.Is(err, tigerbeetle_go.ErrClientReleaseTooHigh):
		// Release mismatch between this server's embedded TB client and
		// the TB cluster. Needs a coordinated upgrade.
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, tigerbeetle_go.ErrUnexpected),
		errors.Is(err, tigerbeetle_go.ErrInvalidOperation):
		return status.Error(codes.Internal, err.Error())
	}
	// Fallback: surface as Internal but preserve the message.
	return status.Error(codes.Internal, err.Error())
}
