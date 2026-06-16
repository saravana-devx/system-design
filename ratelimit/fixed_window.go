package ratelimit

import (
    "sync"
    "time"
)

type FixedWindow struct {
    limit      int        // max requests per window
    count      int        // current request count
    windowSize time.Duration
    windowStart time.Time
    mu         sync.Mutex // protects count and windowStart
}

func NewFixedWindow(limit int, windowSize time.Duration) *FixedWindow {
    return &FixedWindow{
        limit:       limit,
        windowSize:  windowSize,
        windowStart: time.Now(),
    }
}

func (fw *FixedWindow) Allow() bool {
    fw.mu.Lock()
    defer fw.mu.Unlock()

    // if current window has expired, reset
    if time.Since(fw.windowStart) >= fw.windowSize {
        fw.count = 0
        fw.windowStart = time.Now()
    }

    if fw.count < fw.limit {
        fw.count++
        return true  // ✅ allowed
    }
    return false     // ❌ limit hit
}