/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package middleware

import (
	"context"
	"net/http"
)

var requestIDKey = requestIDKeyType{}

type requestIDKeyType struct{}

// RequestID extracts the request ID from the context.
func RequestID(ctx context.Context) string {
	if reqID, ok := ctx.Value(requestIDKey).(string); ok {
		return reqID
	}
	return "unknown"
}

// GenerateIDFunc is a function that generates a unique request ID.
type GenerateIDFunc func() string

type requestID struct {
	generateID GenerateIDFunc
	next       http.Handler
}

// WithRequestID returns a middleware that adds a request ID to each request.
func WithRequestID(generator GenerateIDFunc) Middleware {
	return func(next http.Handler) http.Handler {
		return &requestID{next: next, generateID: generator}
	}
}

func (r *requestID) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	reqID := req.Header.Get("X-Request-Id")
	if reqID == "" {
		reqID = r.generateID()
		req.Header.Set("X-Request-Id", reqID)
	}

	ctx := context.WithValue(req.Context(), requestIDKey, reqID)
	req = req.WithContext(ctx)

	w.Header().Add("X-Request-Id", reqID)

	r.next.ServeHTTP(w, req)
}
