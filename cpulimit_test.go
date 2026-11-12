package cpulimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Run with -race: exercises Wait/AboveLimit/Stop concurrently with the
// measuring goroutine while the CPU usage crosses the limit in both directions.
func TestConcurrentUse(t *testing.T) {
	l := &Limiter{MaxCPUUsage: 0.5, MeasureInterval: time.Millisecond, Measurements: 1, CurrentProcessOnly: true}
	if err := l.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				l.WaitContext(ctx)
				l.AboveLimit()
				// Alternate between burning CPU and idling to cross the limit
				for end := time.Now().Add(10 * time.Millisecond); time.Now().Before(end); {
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	l.Stop()
	l.Wait() // must not block after Stop
}
