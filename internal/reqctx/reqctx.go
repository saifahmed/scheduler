// Package reqctx carries request-scoped values (request ID) through context.Context
// without coupling inner layers to the HTTP framework.
package reqctx

import "context"

type key struct{}

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, key{}, id)
}

func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(key{}).(string)
	return id
}
