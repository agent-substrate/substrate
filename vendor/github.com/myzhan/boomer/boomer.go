package boomer

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var defaultBoomer = &Boomer{logger: log.Default()}

// Mode is the running mode of boomer, both standalone and distributed are supported.
type Mode int

const (
	// DistributedMode requires connecting to a master.
	DistributedMode Mode = iota
	// StandaloneMode will run without a master.
	StandaloneMode
)

// A Boomer is used to run tasks.
// This type is exposed, so users can create and control a Boomer instance programmatically.
// A non-nil logger is supposed to be set.
type Boomer struct {
	masterHost  string
	masterPort  int
	mode        Mode
	rateLimiter RateLimiter
	slaveRunner *slaveRunner

	localRunner *localRunner
	spawnCount  int
	spawnRate   float64

	cpuProfileFile     string
	cpuProfileDuration time.Duration

	memoryProfileFile     string
	memoryProfileDuration time.Duration

	outputs []Output

	logger *log.Logger
}

// NewBoomer returns a new Boomer.
func NewBoomer(masterHost string, masterPort int) *Boomer {
	return &Boomer{
		masterHost: masterHost,
		masterPort: masterPort,
		mode:       DistributedMode,
		logger:     log.Default(),
	}
}

// NewStandaloneBoomer returns a new Boomer, which can run without master.
func NewStandaloneBoomer(spawnCount int, spawnRate float64) *Boomer {
	return &Boomer{
		spawnCount: spawnCount,
		spawnRate:  spawnRate,
		mode:       StandaloneMode,
		logger:     log.Default(),
	}
}

// WithLogger allows user to use their own logger.
// If the logger is nil, it will not take effect.
func (b *Boomer) WithLogger(logger *log.Logger) *Boomer {
	if logger == nil {
		return b
	}
	b.logger = logger
	if b.slaveRunner != nil {
		b.slaveRunner.setLogger(logger)
	}
	if b.localRunner != nil {
		b.localRunner.setLogger(logger)
	}
	return b
}

// SetRateLimiter allows user to use their own rate limiter.
// It must be called before the test is started.
func (b *Boomer) SetRateLimiter(rateLimiter RateLimiter) {
	b.rateLimiter = rateLimiter
}

// SetMode only accepts boomer.DistributedMode and boomer.StandaloneMode.
func (b *Boomer) SetMode(mode Mode) {
	switch mode {
	case DistributedMode:
		b.mode = DistributedMode
	case StandaloneMode:
		b.mode = StandaloneMode
	default:
		b.logger.Println("Invalid mode, ignored!")
	}
}

// AddOutput accepts outputs which implements the boomer.Output interface.
func (b *Boomer) AddOutput(o Output) {
	b.outputs = append(b.outputs, o)
}

// EnableCPUProfile will start cpu profiling after run.
func (b *Boomer) EnableCPUProfile(cpuProfileFile string, duration time.Duration) {
	b.cpuProfileFile = cpuProfileFile
	b.cpuProfileDuration = duration
}

// EnableMemoryProfile will start memory profiling after run.
func (b *Boomer) EnableMemoryProfile(memoryProfileFile string, duration time.Duration) {
	b.memoryProfileFile = memoryProfileFile
	b.memoryProfileDuration = duration
}

// Run accepts a slice of Task and connects to the locust master.
func (b *Boomer) Run(tasks ...*Task) {
	if b.cpuProfileFile != "" {
		err := StartCPUProfile(b.cpuProfileFile, b.cpuProfileDuration)
		if err != nil {
			b.logger.Printf("Error starting cpu profiling, %v", err)
		}
	}
	if b.memoryProfileFile != "" {
		err := StartMemoryProfile(b.memoryProfileFile, b.memoryProfileDuration)
		if err != nil {
			b.logger.Printf("Error starting memory profiling, %v", err)
		}
	}

	switch b.mode {
	case DistributedMode:
		b.slaveRunner = newSlaveRunner(b.masterHost, b.masterPort, tasks, b.rateLimiter)
		b.slaveRunner.setLogger(b.logger)
		b.logger.Println("new slave runner")
		for _, o := range b.outputs {
			b.slaveRunner.addOutput(o)
		}
		b.slaveRunner.run()
	case StandaloneMode:
		b.localRunner = newLocalRunner(tasks, b.rateLimiter, b.spawnCount, b.spawnRate)
		b.localRunner.setLogger(b.logger)
		b.logger.Println("new local runner")
		for _, o := range b.outputs {
			b.localRunner.addOutput(o)
		}
		b.localRunner.run()
	default:
		b.logger.Println("Invalid mode, expected boomer.DistributedMode or boomer.StandaloneMode")
	}
}

// RecordSuccess reports a success.
func (b *Boomer) RecordSuccess(requestType, name string, responseTime int64, responseLength int64) {
	if b.localRunner == nil && b.slaveRunner == nil {
		return
	}
	switch b.mode {
	case DistributedMode:
		b.slaveRunner.stats.requestSuccessChan <- &requestSuccess{
			requestType:    requestType,
			name:           name,
			responseTime:   responseTime,
			responseLength: responseLength,
		}
	case StandaloneMode:
		b.localRunner.stats.requestSuccessChan <- &requestSuccess{
			requestType:    requestType,
			name:           name,
			responseTime:   responseTime,
			responseLength: responseLength,
		}
	}
}

// RecordFailure reports a failure.
func (b *Boomer) RecordFailure(requestType, name string, responseTime int64, exception string) {
	if b.localRunner == nil && b.slaveRunner == nil {
		return
	}
	switch b.mode {
	case DistributedMode:
		b.slaveRunner.stats.requestFailureChan <- &requestFailure{
			requestType:  requestType,
			name:         name,
			responseTime: responseTime,
			error:        exception,
		}
	case StandaloneMode:
		b.localRunner.stats.requestFailureChan <- &requestFailure{
			requestType:  requestType,
			name:         name,
			responseTime: responseTime,
			error:        exception,
		}
	}
}

func (b *Boomer) SendCustomMessage(messageType string, data interface{}) {
	if b.localRunner == nil && b.slaveRunner == nil {
		return
	}
	switch b.mode {
	case DistributedMode:
		b.slaveRunner.sendCustomMessage(messageType, data)
	case StandaloneMode:
		b.localRunner.sendCustomMessage(messageType, data)
	}
}

// Quit will send a quit message to the master.
func (b *Boomer) Quit() {
	Events.Publish(EVENT_QUIT)
	var ticker = time.NewTicker(3 * time.Second)

	switch b.mode {
	case DistributedMode:
		// wait for quit message is sent to master
		select {
		case <-b.slaveRunner.client.disconnectedChannel():
			break
		case <-ticker.C:
			b.logger.Println("Timeout waiting for sending quit message to master, boomer will quit any way.")
			break
		}
		b.slaveRunner.shutdown()
	case StandaloneMode:
		b.localRunner.shutdown()
	}
}

// Run tasks without connecting to the master.
func runTasksForTest(tasks ...*Task) {
	taskNames := strings.Split(runTasks, ",")
	for _, task := range tasks {
		if task.Name == "" {
			continue
		} else {
			for _, name := range taskNames {
				if name == task.Name {
					log.Println("Running " + task.Name)
					task.Fn()
				}
			}
		}
	}
}

// Run accepts a slice of Task and connects to a locust master.
// It's a convenience function to use the defaultBoomer.
func Run(tasks ...*Task) {
	if !flag.Parsed() {
		flag.Parse()
	}

	if runTasks != "" {
		runTasksForTest(tasks...)
		return
	}

	initLegacyEventHandlers()

	rateLimiter, err := createRateLimiter(maxRPS, requestIncreaseRate)
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	defaultBoomer.SetRateLimiter(rateLimiter)
	defaultBoomer.masterHost = masterHost
	defaultBoomer.masterPort = masterPort
	defaultBoomer.EnableMemoryProfile(memoryProfileFile, memoryProfileDuration)
	defaultBoomer.EnableCPUProfile(cpuProfileFile, cpuProfileDuration)

	defaultBoomer.Run(tasks...)

	quitByMe := false
	quitChan := make(chan bool)

	Events.SubscribeOnce(EVENT_QUIT, func() {
		if !quitByMe {
			close(quitChan)
		}
	})

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-c:
		quitByMe = true
		defaultBoomer.Quit()
	case <-quitChan:
	}

	log.Println("shutdown")
}

// RecordSuccess reports a success.
// It's a convenience function to use the defaultBoomer.
func RecordSuccess(requestType, name string, responseTime int64, responseLength int64) {
	defaultBoomer.RecordSuccess(requestType, name, responseTime, responseLength)
}

// RecordFailure reports a failure.
// It's a convenience function to use the defaultBoomer.
func RecordFailure(requestType, name string, responseTime int64, exception string) {
	defaultBoomer.RecordFailure(requestType, name, responseTime, exception)
}
