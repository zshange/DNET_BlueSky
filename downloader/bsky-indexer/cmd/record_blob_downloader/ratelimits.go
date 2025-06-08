package main

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

const defaultRateLimit = 10

type Limiter struct {
	mu      sync.RWMutex
	limiter map[string]*rate.Limiter
}

func NewLimiterWithConfig() *Limiter {
	return &Limiter{
		limiter: map[string]*rate.Limiter{},
	}
}

func (l *Limiter) getLimiter(name string) *rate.Limiter {
	l.mu.RLock()
	limiter := l.limiter[name]
	l.mu.RUnlock()
	
	if limiter != nil {
		return limiter
	}

	limiter = rate.NewLimiter(defaultRateLimit, defaultRateLimit*2)
	l.mu.Lock()
	l.limiter[name] = limiter
	l.mu.Unlock()
	return limiter
}

func (l *Limiter) Wait(ctx context.Context, name string) error {
	return l.getLimiter(name).Wait(ctx)
}

 