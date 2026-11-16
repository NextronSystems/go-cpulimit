package cpulimit

import (
	"context"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/process"
)

const (
	// DefaultLimit is 80%
	DefaultLimit float64 = 80.0
	// DefaultInterval is 333 ms
	DefaultInterval time.Duration = time.Millisecond * 333
	// DefaultMeasurements is 3
	DefaultMeasurements int = 3
)

// Config configures a Limiter. Zero values are replaced by the defaults.
type Config struct {
	// MaxCPUUsage specifies the maximum CPU usage; wait will block if the
	// average CPU usage during the previous measurements exceeds this value.
	MaxCPUUsage float64
	// MeasureInterval specifies how often the CPU usage should be measured.
	MeasureInterval time.Duration
	// Measurements specifies how many measurements should be retained for the
	// average CPU usage calculation.
	Measurements int
	// CurrentProcessOnly specifies that only the CPU usage of the current process
	// should be measured; otherwise, the full CPU usage is measured.
	CurrentProcessOnly bool
}

// Limiter limits the CPU usage. Create one with New.
type Limiter struct {
	cfg Config
	// maxCPUUsage holds the float64 bits of the current limit; it is adjustable
	// while the limiter runs, see SetMaxCPUUsage.
	maxCPUUsage atomic.Uint64

	stop atomic.Bool
	wg   sync.WaitGroup
	self *process.Process

	// waitForBelowLimit will be blocking while the CPU usage is above MaxCPUUsage,
	// and will be closed when the CPU usage is below MaxCPUUsage.
	waitForBelowLimit   atomic.Pointer[chan struct{}]
	currentlyBelowLimit atomic.Bool // Indicates if the CPU usage is currently below the limit
}

// New creates a CPU limiter and starts measuring the CPU usage.
// Call Stop to release it.
func New(cfg Config) (*Limiter, error) {
	if cfg.MaxCPUUsage == 0.0 {
		cfg.MaxCPUUsage = DefaultLimit
	}
	if cfg.MeasureInterval == 0 {
		cfg.MeasureInterval = DefaultInterval
	}
	if cfg.Measurements == 0 {
		cfg.Measurements = DefaultMeasurements
	}
	l := &Limiter{cfg: cfg}
	l.SetMaxCPUUsage(cfg.MaxCPUUsage)
	if cfg.CurrentProcessOnly {
		var err error
		l.self, err = process.NewProcess(int32(os.Getpid()))
		if err != nil {
			return nil, err
		}
	}
	l.newWaitChannel()
	l.wg.Add(1)
	go l.run()
	return l, nil
}

// Stop stops the limiter. After this stop, Wait() won't block anymore.
func (l *Limiter) Stop() {
	l.stop.Store(true)
	l.wg.Wait()
}

// MaxCPUUsage returns the current maximum CPU usage.
func (l *Limiter) MaxCPUUsage() float64 {
	return math.Float64frombits(l.maxCPUUsage.Load())
}

// SetMaxCPUUsage changes the maximum CPU usage. It is safe to call concurrently
// with all other methods and takes effect with the next measurement.
// Unlike in Config, a zero value is not replaced by the default.
func (l *Limiter) SetMaxCPUUsage(limit float64) {
	l.maxCPUUsage.Store(math.Float64bits(limit))
}

// Wait waits until the CPU usage is below MaxCPUUsage
func (l *Limiter) Wait() {
	l.WaitContext(context.Background())
}

func (l *Limiter) WaitContext(ctx context.Context) {
	if l.currentlyBelowLimit.Load() {
		// If the CPU usage is already below the limit, we can return immediately
		return
	}
	if ctx.Err() != nil {
		// Same if the context is already done
		return
	}
	// Otherwise, we wait for the CPU usage to go below the limit
	// or the context to be done
	select {
	case <-l.waitChannel():
	case <-ctx.Done():
	}
}

// waitChannel returns the current waitForBelowLimit channel.
func (l *Limiter) waitChannel() chan struct{} {
	return *l.waitForBelowLimit.Load()
}

func (l *Limiter) newWaitChannel() {
	ch := make(chan struct{})
	l.waitForBelowLimit.Store(&ch)
}

// AboveLimit reports if the CPU usage is above MaxCPUUsage
func (l *Limiter) AboveLimit() bool {
	return !l.currentlyBelowLimit.Load()
}

func (l *Limiter) run() {
	defer l.wg.Done()
	var (
		busy1    float64
		busy2    float64
		all1     float64
		all2     float64
		cpuUsage float64
		m        = make([]float64, l.cfg.Measurements) // measurements
	)
	defer func() {
		// Close the channel if it was blocking, so that Wait() can return
		if !l.currentlyBelowLimit.Load() {
			close(l.waitChannel())
		}
	}()
	tk := time.NewTicker(l.cfg.MeasureInterval)
	defer tk.Stop()
	busy2, all2 = l.getBusy()
	var counter int
	for range tk.C {
		if l.stop.Load() {
			break
		}
		busy1, all1 = busy2, all2
		busy2, all2 = l.getBusy()
		cpuUsage = getCPUUsage(busy1, all1, busy2, all2)
		m[counter] = cpuUsage
		if average(m) > l.MaxCPUUsage() {
			if l.currentlyBelowLimit.Load() {
				// Create a new, blocking channel to signal that the CPU usage is above the limit
				l.newWaitChannel()
				l.currentlyBelowLimit.Store(false)
			}
		} else {
			if !l.currentlyBelowLimit.Load() {
				// Close the channel to signal that the CPU usage is below the limit
				close(l.waitChannel())
				l.currentlyBelowLimit.Store(true)
			}
		}
		counter++
		if counter > l.cfg.Measurements-1 {
			counter = 0
		}
	}
}

func average(m []float64) (avg float64) {
	for _, elem := range m {
		avg += elem
	}
	avg /= float64(len(m))
	return
}

func (l *Limiter) getBusy() (busy, all float64) {
	if l.cfg.CurrentProcessOnly {
		busy, _ = busyFromTimes(getProcessCpuTimes(l.self))
		_, all = busyFromTimes(getGlobalCpuTimes())
	} else {
		busy, all = busyFromTimes(getGlobalCpuTimes())
	}
	return
}

func getGlobalCpuTimes() cpu.TimesStat {
	ts, err := cpu.Times(false)
	if err != nil {
		panic(err)
	}
	return ts[0]
}

func getProcessCpuTimes(proc *process.Process) cpu.TimesStat {
	t, err := proc.Times()
	if err != nil {
		panic(err)
	}
	return *t
}

func busyFromTimes(t cpu.TimesStat) (busy, all float64) {
	busy = t.User + t.System + t.Nice + t.Iowait + t.Irq + t.Softirq + t.Steal + t.Guest + t.GuestNice
	all = busy + t.Idle
	return
}

func getCPUUsage(busy1, all1, busy2, all2 float64) float64 {
	if all1 == all2 {
		return 0.0
	}
	usage := ((busy2 - busy1) / (all2 - all1)) * 100.0
	return usage
}
