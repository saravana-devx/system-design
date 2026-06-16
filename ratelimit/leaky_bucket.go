package ratelimit

import (
	"context"
	"time"
)

type LeakyBucket struct {
	queue  chan struct{} // the bucket (bounded queue)
	ticker *time.Ticker  // fixed-rate leak
	cancel context.CancelFunc
}

func NewLeakyBucket(rate int, capacity int) *LeakyBucket {
	lb := &LeakyBucket{
		queue:  make(chan struct{}, capacity),
		ticker: time.NewTicker(time.Duration(1000/rate) * time.Millisecond),
	}
	ctx, cancel := context.WithCancel(context.Background())
	lb.cancel = cancel
	lb.start(ctx)
	return lb
}

// start drains one item from the queue on every tick (the "leak")
func (lb *LeakyBucket) start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-lb.ticker.C:
				select {
				case <-lb.queue: // drain one request → process it
				default: // bucket empty, nothing to do
				}
			case <-ctx.Done():
				lb.ticker.Stop()
				return
			}
		}
	}()
}

// Add puts a request in the bucket. Returns false if the bucket is full (drop it).
func (lb *LeakyBucket) Add() bool {
	select {
	case lb.queue <- struct{}{}:
		return true // queued ✓
	default:
		return false // bucket full → drop ✗
	}
}

func (lb *LeakyBucket) Stop() {
	lb.cancel()
}
