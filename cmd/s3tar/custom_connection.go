package main

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"net"
	"net/url"
	"strings"
)

// CustomRetryableConnectionError is a rewrite of aws' RetryableConnectionError
// that does not retry NXDOMAIN errors
type CustomRetryableConnectionError struct{}

// IsErrorRetryable returns if the error is caused by and HTTP connection
// error, and should be retried.
func (r CustomRetryableConnectionError) IsErrorRetryable(err error) aws.Ternary {
	if err == nil {
		return aws.UnknownTernary
	}
	var retryable bool

	var conErr interface{ ConnectionError() bool }
	var tempErr interface{ Temporary() bool }
	var timeoutErr interface{ Timeout() bool }
	var urlErr *url.Error
	var netOpErr *net.OpError
	var dnsError *net.DNSError

	switch {
	case errors.As(err, &dnsError):
		retryable = false
	case errors.As(err, &conErr) && conErr.ConnectionError():
		retryable = true

	case strings.Contains(err.Error(), "connection reset"):
		retryable = true

	case errors.As(err, &urlErr):
		// Refused connections should be retried as the service may not yet be
		// running on the port. Go TCP dial considers refused connections as
		// not temporary.
		if strings.Contains(urlErr.Error(), "connection refused") {
			retryable = true
		} else {
			return r.IsErrorRetryable(errors.Unwrap(urlErr))
		}

	case errors.As(err, &netOpErr):
		// Network dial, or temporary network errors are always retryable.
		if strings.EqualFold(netOpErr.Op, "dial") || netOpErr.Temporary() {
			retryable = true
		} else {
			return r.IsErrorRetryable(errors.Unwrap(netOpErr))
		}

	case errors.As(err, &tempErr) && tempErr.Temporary():
		// Fallback to the generic temporary check, with temporary errors
		// retryable.
		retryable = true

	case errors.As(err, &timeoutErr) && timeoutErr.Timeout():
		// Fallback to the generic timeout check, with timeout errors
		// retryable.
		retryable = true

	default:
		return aws.UnknownTernary
	}

	return aws.BoolTernary(retryable)

}
