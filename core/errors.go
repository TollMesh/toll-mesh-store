package core

import "fmt"

// ErrorCode represents a machine-readable error identifier
type ErrorCode int

const (
	// Success
	ErrorCodeOK ErrorCode = 0

	// Client errors (4xx)
	ErrorCodeInvalidRequest ErrorCode = 400
	ErrorCodeNotFound       ErrorCode = 404
	ErrorCodeConflict       ErrorCode = 409

	// Rate limiting
	ErrorCodeRateLimited ErrorCode = 429

	// Server errors (5xx)
	ErrorCodeInternal     ErrorCode = 500
	ErrorCodeUnavailable  ErrorCode = 503
	ErrorCodeDeadlineExceeded ErrorCode = 504

	// TollMesh-specific errors (1000+)
	ErrorCodeReplayDetected      ErrorCode = 1001
	ErrorCodeCacheMiss           ErrorCode = 1002
	ErrorCodeInvalidNamespace    ErrorCode = 1003
	ErrorCodeInvalidKey          ErrorCode = 1004
	ErrorCodeInvalidTTL          ErrorCode = 1005
	ErrorCodeInvalidValue        ErrorCode = 1006
	ErrorCodePeerUnavailable     ErrorCode = 1007
	ErrorCodeGossipFailed        ErrorCode = 1008
	ErrorCodeTransactionFailed   ErrorCode = 1009
	ErrorCodeScriptError         ErrorCode = 1010
	ErrorCodeSearchFailed        ErrorCode = 1011
	ErrorCodeGraphError          ErrorCode = 1012
)

// Error represents an application error with code and message
type Error struct {
	Code    ErrorCode
	Message string
	Details map[string]interface{}
}

// NewError creates a new error with code and message
func NewError(code ErrorCode, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Details: make(map[string]interface{}),
	}
}

// WithDetail adds contextual details to an error
func (e *Error) WithDetail(key string, value interface{}) *Error {
	if e.Details == nil {
		e.Details = make(map[string]interface{})
	}
	e.Details[key] = value
	return e
}

// Error implements the error interface
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("Error %d: %s", e.Code, e.Message)
}

// Is checks if an error matches a specific code
func (e *Error) Is(code ErrorCode) bool {
	if e == nil {
		return code == ErrorCodeOK
	}
	return e.Code == code
}

// Common error instances for reuse
var (
	ErrInvalidRequest = NewError(ErrorCodeInvalidRequest, "invalid request parameters")
	ErrNotFound       = NewError(ErrorCodeNotFound, "resource not found")
	ErrRateLimited    = NewError(ErrorCodeRateLimited, "rate limit exceeded")
	ErrInternal       = NewError(ErrorCodeInternal, "internal server error")
	ErrUnavailable    = NewError(ErrorCodeUnavailable, "service unavailable")
	ErrReplayDetected = NewError(ErrorCodeReplayDetected, "replay attack detected")
	ErrCacheMiss      = NewError(ErrorCodeCacheMiss, "cache miss")
)

// ErrorCodeString returns the string representation of an error code
func ErrorCodeString(code ErrorCode) string {
	switch code {
	case ErrorCodeOK:
		return "OK"
	case ErrorCodeInvalidRequest:
		return "INVALID_REQUEST"
	case ErrorCodeNotFound:
		return "NOT_FOUND"
	case ErrorCodeConflict:
		return "CONFLICT"
	case ErrorCodeRateLimited:
		return "RATE_LIMITED"
	case ErrorCodeInternal:
		return "INTERNAL"
	case ErrorCodeUnavailable:
		return "UNAVAILABLE"
	case ErrorCodeDeadlineExceeded:
		return "DEADLINE_EXCEEDED"
	case ErrorCodeReplayDetected:
		return "REPLAY_DETECTED"
	case ErrorCodeCacheMiss:
		return "CACHE_MISS"
	case ErrorCodeInvalidNamespace:
		return "INVALID_NAMESPACE"
	case ErrorCodeInvalidKey:
		return "INVALID_KEY"
	case ErrorCodeInvalidTTL:
		return "INVALID_TTL"
	case ErrorCodeInvalidValue:
		return "INVALID_VALUE"
	case ErrorCodePeerUnavailable:
		return "PEER_UNAVAILABLE"
	case ErrorCodeGossipFailed:
		return "GOSSIP_FAILED"
	case ErrorCodeTransactionFailed:
		return "TRANSACTION_FAILED"
	case ErrorCodeScriptError:
		return "SCRIPT_ERROR"
	case ErrorCodeSearchFailed:
		return "SEARCH_FAILED"
	case ErrorCodeGraphError:
		return "GRAPH_ERROR"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", code)
	}
}

// StringToErrorCode converts a string to an error code
func StringToErrorCode(s string) ErrorCode {
	switch s {
	case "OK":
		return ErrorCodeOK
	case "INVALID_REQUEST":
		return ErrorCodeInvalidRequest
	case "NOT_FOUND":
		return ErrorCodeNotFound
	case "CONFLICT":
		return ErrorCodeConflict
	case "RATE_LIMITED":
		return ErrorCodeRateLimited
	case "INTERNAL":
		return ErrorCodeInternal
	case "UNAVAILABLE":
		return ErrorCodeUnavailable
	case "DEADLINE_EXCEEDED":
		return ErrorCodeDeadlineExceeded
	case "REPLAY_DETECTED":
		return ErrorCodeReplayDetected
	case "CACHE_MISS":
		return ErrorCodeCacheMiss
	case "INVALID_NAMESPACE":
		return ErrorCodeInvalidNamespace
	case "INVALID_KEY":
		return ErrorCodeInvalidKey
	case "INVALID_TTL":
		return ErrorCodeInvalidTTL
	case "INVALID_VALUE":
		return ErrorCodeInvalidValue
	case "PEER_UNAVAILABLE":
		return ErrorCodePeerUnavailable
	case "GOSSIP_FAILED":
		return ErrorCodeGossipFailed
	case "TRANSACTION_FAILED":
		return ErrorCodeTransactionFailed
	case "SCRIPT_ERROR":
		return ErrorCodeScriptError
	case "SEARCH_FAILED":
		return ErrorCodeSearchFailed
	case "GRAPH_ERROR":
		return ErrorCodeGraphError
	default:
		return ErrorCodeInternal
	}
}
