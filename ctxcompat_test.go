package dre

import (
	"context"
	"time"
)

func contextBackground() context.Context { return context.Background() }

func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
