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

// One gotcha worth knowing, since it's not obvious from either picture: in this code ZAdd runs inside the pipeline, so the current request gets stored in Redis even when it ends up denied. The count was read before adding, so the deny decision is still correct — but the denied timestamp sits in the set until it ages out of the window. For most apps that's harmless. If you want denied requests to not count against the user at all, you'd move the ZAdd to only run after confirming count < limit (which means giving up the single-pipeline atomicity, or using a Lua script to do the check-and-add atomically on the Redis side).
