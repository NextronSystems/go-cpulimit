package main

import (
	"time"

	"github.com/NextronSystems/go-cpulimit"
)

func main() {
	limiter, err := cpulimit.New(cpulimit.Config{
		MaxCPUUsage:     50.0,                   // throttle if current cpu usage is over 50%
		MeasureInterval: time.Millisecond * 333, // measure cpu usage in an interval of 333 milliseconds
		Measurements:    3,                      // use the average of the last 3 measurements for cpu usage calculation
	})
	if err != nil {
		panic(err)
	}
	defer limiter.Stop()
	for {
		limiter.Wait() // wait until cpu usage is below 50%
		/*
		 * do some work
		 */
	}
}
