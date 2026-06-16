package ratelimit

import (
	"context"
	"time"
)

type tokens chan struct{}

type TokenBucket struct {
	capacity int
	tokens   tokens
	ticker   *time.Ticker
	cancel   context.CancelFunc
}

func NewTokenBucket(rate int, capacity int) *TokenBucket {
	tokens := make(tokens, capacity)
	for i := 0; i < capacity; i++ {
		tokens <- struct{}{}
	}
	everyMs := 1000 / rate
	ctx, cancel := context.WithCancel(context.Background())
	tb := &TokenBucket{
		capacity: capacity,
		tokens:   tokens,
		ticker:   time.NewTicker(time.Duration(everyMs) * time.Millisecond),
		cancel:   cancel,
	}
	tb.start(ctx)
	return tb
}

func (tb *TokenBucket) start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-tb.ticker.C:
				select {
				case tb.tokens <- struct{}{}:
				default:
				}
			case <-ctx.Done():
				tb.ticker.Stop()
				return
			}
		}
	}()
}

func (tb *TokenBucket) Stop() {
	tb.cancel()
}

func (tb *TokenBucket) TryAcquire() bool {
	select {
	case <-tb.tokens:
		return true
	default:
		return false
	}
}
