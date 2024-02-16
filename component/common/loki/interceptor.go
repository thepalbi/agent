package loki

import (
	"context"
)

var _ Appender = (*Interceptor)(nil)

// Interceptor is a Appender which invokes callback functions upon
// getting data. Interceptor should not be modified once created. All callback
// fields are optional.
type Interceptor struct {
	onAppend func(ctx context.Context, e Entry, next Appender) (Entry, error)

	// next is the next Appender in the chain
	next Appender
}

func NewInterceptor(next Appender, opts ...InterceptorOption) *Interceptor {
	i := &Interceptor{
		next: next,
	}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

// InterceptorOption is an option argument passed to NewInterceptor.
type InterceptorOption func(*Interceptor)

// WithAppendHook returns an InterceptorOption which hooks into calls to
// Append.
func WithAppendHook(f func(context.Context, Entry, Appender) (Entry, error)) InterceptorOption {
	return func(i *Interceptor) {
		i.onAppend = f
	}
}

// Append implements Appender.
func (i *Interceptor) Append(ctx context.Context, entry Entry) (Entry, error) {
	// if there's a hook in place, intercept
	if i.onAppend != nil {
		return i.onAppend(ctx, entry, i.next)
	}

	return i.next.Append(ctx, entry)
}
