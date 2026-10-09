package awsprotocol

import (
	"fmt"
	"io"
	"math"
)

// ReadBoundedBody reads at most maxBytes+1 bytes. The extra byte belongs to the
// caller's oversize check; protocol-specific limits and error mapping remain in
// the service. Content-Length is only an allocation hint, never a read boundary.
func ReadBoundedBody(reader io.Reader, contentLength, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 || maxBytes == math.MaxInt64 {
		return nil, fmt.Errorf("body limit must be positive and bounded")
	}
	limit := maxBytes + 1
	capacity := int(min(int64(512), limit))
	maxInt := int64(int(^uint(0) >> 1))
	if contentLength > 0 && contentLength <= maxBytes && contentLength < maxInt {
		// The sentinel slot lets a complete declared body reach EOF without growing
		// another backing allocation. A longer actual body is still read and gated.
		capacity = int(contentLength + 1)
	}
	body := make([]byte, 0, capacity)
	bounded := io.LimitReader(reader, limit)
	for {
		n, err := bounded.Read(body[len(body):cap(body)])
		body = body[:len(body)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return body, err
		}
		if int64(len(body)) == limit {
			return body, nil
		}
		if len(body) == cap(body) {
			body = append(body, 0)[:len(body)]
		}
	}
}
