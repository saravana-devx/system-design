package ratelimit

import (
    "sync"
    "time"
)

type SlidingWindow struct {
    limit      int
    windowSize time.Duration
    timestamps []time.Time  // stores when each request came in
    mu         sync.Mutex
}

func NewSlidingWindow(limit int, windowSize time.Duration) *SlidingWindow {
    return &SlidingWindow{
        limit:      limit,
        windowSize: windowSize,
    }
}

func (sw *SlidingWindow) Allow() bool {
    sw.mu.Lock()
    defer sw.mu.Unlock()

    now := time.Now()
    windowStart := now.Add(-sw.windowSize)  // e.g. now minus 60 seconds

    // throw away timestamps older than the window
    valid := sw.timestamps[:0]
    for _, t := range sw.timestamps {
        if t.After(windowStart) {
            valid = append(valid, t)
        }
    }
    sw.timestamps = valid

    if len(sw.timestamps) < sw.limit {
        sw.timestamps = append(sw.timestamps, now)
        return true   // ✅ allowed
    }
    return false      // ❌ too many in this window
}