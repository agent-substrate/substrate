package boomer

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	stateInit     = "ready"
	stateSpawning = "spawning"
	stateRunning  = "running"
	stateStopped  = "stopped"
	stateQuitting = "quitting"
)

const (
	slaveReportInterval    = 3 * time.Second
	heartbeatInterval      = 1 * time.Second
	masterHeartbeatTimeout = 60 * time.Second
)

type runner struct {
	state string

	tasks           []*Task
	totalTaskWeight int
	runTask         []*Task // goroutine execute tasks according to the list

	rateLimiter      RateLimiter
	rateLimitEnabled bool
	stats            *requestStats

	// TODO: we save user_class_count in spawn message and send it back to master without modification, may be a bad idea?
	userClassesCountFromMaster map[string]int64

	numClients int32
	spawnRate  float64

	// Cancellation method for all running workers(goroutines)
	cancelFuncs []context.CancelFunc

	// close this channel will stop all goroutines used in runner, including running workers.
	shutdownChan chan bool

	outputs []Output

	logger *log.Logger
}

func (r *runner) setLogger(logger *log.Logger) {
	if logger != nil {
		r.logger = logger
	}
}

// safeRun runs fn and recovers from unexpected panics.
// it prevents panics from Task.Fn crashing boomer.
func (r *runner) safeRun(fn func()) {
	defer func() {
		// don't panic
		err := recover()
		if err != nil {
			stackTrace := debug.Stack()
			errMsg := fmt.Sprintf("%v", err)
			os.Stderr.Write([]byte(errMsg))
			os.Stderr.Write([]byte("\n"))
			os.Stderr.Write(stackTrace)
		}
	}()
	fn()
}

func (r *runner) addOutput(o Output) {
	r.outputs = append(r.outputs, o)
}

func (r *runner) outputOnStart() {
	size := len(r.outputs)
	if size == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(size)
	for _, output := range r.outputs {
		go func(o Output) {
			o.OnStart()
			wg.Done()
		}(output)
	}
	wg.Wait()
}

func (r *runner) outputOnEevent(data map[string]interface{}) {
	size := len(r.outputs)
	if size == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(size)
	for _, output := range r.outputs {
		go func(o Output) {
			o.OnEvent(data)
			wg.Done()
		}(output)
	}
	wg.Wait()
}

func (r *runner) outputOnStop() {
	size := len(r.outputs)
	if size == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(size)
	for _, output := range r.outputs {
		go func(o Output) {
			o.OnStop()
			wg.Done()
		}(output)
	}
	wg.Wait()
}

// addWorkers start the goroutines and add it to cancelFuncs
func (r *runner) addWorkers(gapCount int) {
	for i := 0; i < gapCount; i++ {
		select {
		case <-r.shutdownChan:
			return
		default:
			ctx, cancel := context.WithCancel(context.TODO())
			r.cancelFuncs = append(r.cancelFuncs, cancel)
			go func(ctx context.Context) {
				index := 0
				for {
					select {
					case <-ctx.Done():
						return
					case <-r.shutdownChan:
						return
					default:
						if r.rateLimitEnabled {
							blocked := r.rateLimiter.Acquire()
							if !blocked {
								task := r.getTask(index)
								r.safeRun(task.Fn)
								index++
								if index == r.totalTaskWeight {
									index = 0
								}
							}
						} else {
							task := r.getTask(index)
							r.safeRun(task.Fn)
							index++
							if index == r.totalTaskWeight {
								index = 0
							}
						}
					}
					runtime.Gosched()
				}
			}(ctx)
		}
	}
}

// reduceWorkers Stop the goroutines and remove it from the cancelFuncs
func (r *runner) reduceWorkers(gapCount int) {
	if gapCount == 0 {
		return
	}
	num := len(r.cancelFuncs) - gapCount
	for _, cancelFunc := range r.cancelFuncs[num:] {
		cancelFunc()
	}

	r.cancelFuncs = r.cancelFuncs[:num]

}

func (r *runner) spawnWorkers(spawnCount int, spawnCompleteFunc func()) {
	r.logger.Println("The total number of clients required is ", spawnCount)

	var gapCount int
	if spawnCount > int(r.numClients) {
		gapCount = spawnCount - int(r.numClients)
		r.logger.Printf("The current number of clients is %v, %v clients will be added\n", r.numClients, gapCount)
		r.addWorkers(gapCount)
	} else {
		gapCount = int(r.numClients) - spawnCount
		r.logger.Printf("The current number of clients is %v, %v clients will be removed\n", r.numClients, gapCount)
		r.reduceWorkers(gapCount)
	}

	r.numClients = int32(spawnCount)

	if spawnCompleteFunc != nil {
		go spawnCompleteFunc() //For faster time
	}
}

// setTasks will set the runner's task list AND the total task weight
// which is used to get a task later
func (r *runner) setTasks(t []*Task) {
	r.tasks = t
	if len(r.tasks) == 1 {
		r.totalTaskWeight = 1
		r.runTask = t
		return
	}

	weightSum := 0
	for _, task := range r.tasks {
		if task.Weight <= 0 { //Ensure that user input values are legal
			task.Weight = 1
		}
		weightSum += task.Weight
	}
	r.totalTaskWeight = weightSum

	r.runTask = make([]*Task, r.totalTaskWeight)
	index := 0
	for weightSum > 0 { //Assign task order according to weight
		for _, task := range r.tasks {
			if task.Weight > 0 {
				r.runTask[index] = task
				index++
				task.Weight--
				weightSum--
			}
		}
	}
}

func (r *runner) getTask(index int) *Task {
	return r.runTask[index]
}

func (r *runner) startSpawning(spawnCount int, spawnRate float64, spawnCompleteFunc func()) {
	Events.Publish(EVENT_SPAWN, spawnCount, spawnRate)

	r.spawnWorkers(spawnCount, spawnCompleteFunc)
}

func (r *runner) stop() {
	// publish the boomer stop event
	// user's code can subscribe to this event and do thins like cleaning up
	Events.Publish(EVENT_STOP)

	r.reduceWorkers(int(r.numClients)) //Stop all goroutines
	r.numClients = 0
}

type localRunner struct {
	runner

	spawnCount int
}

func newLocalRunner(tasks []*Task, rateLimiter RateLimiter, spawnCount int, spawnRate float64) (r *localRunner) {
	r = &localRunner{}
	r.setLogger(log.Default())
	r.setTasks(tasks)
	r.spawnRate = spawnRate
	r.spawnCount = spawnCount
	r.shutdownChan = make(chan bool)

	if rateLimiter != nil {
		r.rateLimitEnabled = true
		r.rateLimiter = rateLimiter
	}

	r.stats = newRequestStats()
	return r
}

func (r *localRunner) run() {
	r.state = stateInit
	r.stats.start()
	r.outputOnStart()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			select {
			case data := <-r.stats.messageToRunnerChan:
				data["user_count"] = r.numClients
				r.outputOnEevent(data)
			case <-r.shutdownChan:
				Events.Publish(EVENT_QUIT)
				r.stop()
				wg.Done()
				r.outputOnStop()
				return
			}
		}
	}()

	if r.rateLimitEnabled {
		r.rateLimiter.Start()
	}
	r.startSpawning(r.spawnCount, r.spawnRate, nil)

	wg.Wait()
}

func (r *localRunner) shutdown() {
	if r.stats != nil {
		r.stats.close()
	}
	if r.rateLimitEnabled {
		r.rateLimiter.Stop()
	}
	close(r.shutdownChan)
}

func (r *localRunner) sendCustomMessage(messageType string, data interface{}) {
	// Running in standalone mode, sending message to self
	msg := newCustomMessage(messageType, data, "local")
	Events.Publish(messageType, msg)
}

// SlaveRunner connects to the master, spawns goroutines and collects stats.
type slaveRunner struct {
	runner

	nodeID                       string
	masterHost                   string
	masterPort                   int
	waitForAck                   sync.WaitGroup
	ackReceived                  int32
	lastReceivedSpawnTimestamp   int64
	lastMasterHeartbeatTimestamp time.Time
	client                       client
}

func newSlaveRunner(masterHost string, masterPort int, tasks []*Task, rateLimiter RateLimiter) (r *slaveRunner) {
	r = &slaveRunner{}
	r.setLogger(log.Default())
	r.masterHost = masterHost
	r.masterPort = masterPort
	r.setTasks(tasks)
	r.waitForAck = sync.WaitGroup{}
	r.nodeID = getNodeID()
	r.shutdownChan = make(chan bool)

	if rateLimiter != nil {
		r.rateLimitEnabled = true
		r.rateLimiter = rateLimiter
	}

	r.stats = newRequestStats()
	return r
}

func (r *slaveRunner) spawnComplete() {
	data := make(map[string]interface{})
	data["count"] = r.numClients
	data["user_classes_count"] = r.userClassesCountFromMaster
	r.client.sendChannel() <- newGenericMessage("spawning_complete", data, r.nodeID)
	r.state = stateRunning
}

func (r *slaveRunner) onQuiting() {
	if r.state != stateQuitting {
		r.client.sendChannel() <- newGenericMessage("quit", nil, r.nodeID)
	}
}

func (r *slaveRunner) shutdown() {
	if r.stats != nil {
		r.stats.close()
	}
	if r.client != nil {
		r.client.close()
	}
	if r.rateLimitEnabled {
		r.rateLimiter.Stop()
	}
	r.cancelFuncs = nil
	r.numClients = 0
	close(r.shutdownChan)
}

func (r *slaveRunner) sumUsersAmount(msg *genericMessage) int {
	userClassesCount := msg.Data["user_classes_count"]
	userClassesCountMap := userClassesCount.(map[interface{}]interface{})

	// Save the original field and send it back to master in spawnComplete message.
	r.userClassesCountFromMaster = make(map[string]int64)
	amount := 0
	for class, num := range userClassesCountMap {
		c, ok := class.(string)
		n, ok2 := castToInt64(num)
		if !ok || !ok2 {
			log.Printf("user_classes_count in spawn message can't be casted to map[string]int64, current type is map[%T]%T, ignored!\n", class, num)
			continue
		}
		r.userClassesCountFromMaster[c] = n
		amount = amount + int(n)
	}
	return amount
}

// TODO: Since locust 2.0, spawn rate and user count are both handled by master.
// But user count is divided by user classes defined in locustfile, because locust assumes that
// master and workers use the same locustfile. Before we find a better way to deal with this,
// boomer sums up the total amout of users in spawn message and uses task weight to spawn goroutines like before.
func (r *slaveRunner) onSpawnMessage(msg *genericMessage) {
	if timeStamp, ok := msg.Data["timestamp"]; ok {
		if timeStampInt64, ok := castToInt64(timeStamp); ok {
			if timeStampInt64 <= r.lastReceivedSpawnTimestamp {
				r.logger.Println("Discard spawn message with older or equal timestamp than timestamp of previous spawn message")
				return
			} else {
				r.lastReceivedSpawnTimestamp = timeStampInt64
			}
		}
	}

	r.client.sendChannel() <- newGenericMessage("spawning", nil, r.nodeID)
	workers := r.sumUsersAmount(msg)
	r.startSpawning(workers, float64(workers), r.spawnComplete)
}

// TODO: consider to add register_message instead of publishing any unknown type as custom_message.
func (r *slaveRunner) onCustomMessage(msg *CustomMessage) {
	if msg == nil {
		return
	}
	Events.Publish(msg.Type, msg)
}

func (r *slaveRunner) onAckMessage(msg *genericMessage) {
	// Maybe we should add a state for waiting?
	if !atomic.CompareAndSwapInt32(&r.ackReceived, 0, 1) {
		r.logger.Println("Receive duplicate ack message, ignored")
		return
	}
	r.waitForAck.Done()
	Events.Publish(EVENT_CONNECTED)
}

func (r *slaveRunner) sendClientReadyAndWaitForAck() {
	r.waitForAck = sync.WaitGroup{}
	r.waitForAck.Add(1)

	atomic.StoreInt32(&r.ackReceived, 0)
	// locust allows workers to bypass version check by sending -1 as version
	r.client.sendChannel() <- newClientReadyMessage("client_ready", -1, r.nodeID)

	go func() {
		if waitTimeout(&r.waitForAck, 5*time.Second) {
			r.logger.Println("Timeout waiting for ack message from master, you may use a locust version before 2.10.0 or have a network issue.")
		}
	}()
}

// Runner acts as a state machine.
func (r *slaveRunner) onMessage(msgInterface message) {
	var msgType string
	var customMsg *CustomMessage
	var genericMsg *genericMessage

	genericMsg, ok := msgInterface.(*genericMessage)
	if ok {
		msgType = genericMsg.Type
	} else {
		customMsg, ok = msgInterface.(*CustomMessage)
		if !ok {
			r.logger.Println("Receive unknown type of message from master.")
			return
		} else {
			msgType = customMsg.Type
		}
	}

	switch msgType {
	case "heartbeat":
		r.lastMasterHeartbeatTimestamp = time.Now()
		return
	}

	switch r.state {
	case stateInit:
		switch msgType {
		case "ack":
			r.onAckMessage(genericMsg)
		case "spawn":
			r.state = stateSpawning
			r.stats.clearStatsChan <- true
			r.onSpawnMessage(genericMsg)
		case "quit":
			Events.Publish(EVENT_QUIT)
		default:
			r.onCustomMessage(customMsg)
		}
	case stateSpawning:
		fallthrough
	case stateRunning:
		switch msgType {
		case "spawn":
			r.state = stateSpawning
			r.onSpawnMessage(genericMsg)
		case "stop":
			r.stop()
			r.state = stateStopped
			r.logger.Println("Recv stop message from master, all the goroutines are stopped")
			r.client.sendChannel() <- newGenericMessage("client_stopped", nil, r.nodeID)
			r.sendClientReadyAndWaitForAck()
			r.state = stateInit
		case "quit":
			r.stop()
			r.logger.Println("Recv quit message from master, all the goroutines are stopped")
			Events.Publish(EVENT_QUIT)
			r.state = stateInit
		default:
			r.onCustomMessage(customMsg)
		}
	case stateStopped:
		switch msgType {
		case "spawn":
			r.state = stateSpawning
			r.stats.clearStatsChan <- true
			r.onSpawnMessage(genericMsg)
		case "quit":
			Events.Publish(EVENT_QUIT)
			r.state = stateInit
		default:
			r.onCustomMessage(customMsg)
		}
	}
}

func (r *slaveRunner) sendCustomMessage(messageType string, data interface{}) {
	msg := newCustomMessage(messageType, data, r.nodeID)
	r.client.sendChannel() <- msg
}

func (r *slaveRunner) startListener() {
	go func() {
		for {
			select {
			case msg := <-r.client.recvChannel():
				r.onMessage(msg)
			case <-r.shutdownChan:
				return
			}
		}
	}()
}

func (r *slaveRunner) run() {
	r.state = stateInit
	r.client = newClient(r.masterHost, r.masterPort, r.nodeID)

	err := r.client.connect()
	if err != nil {
		if strings.Contains(err.Error(), "Socket type DEALER is not compatible with PULL") {
			r.logger.Println("Newer version of locust changes ZMQ socket to DEALER and ROUTER, you should update your locust version.")
		} else {
			r.logger.Printf("Failed to connect to master(%s:%d) with error %v\n", r.masterHost, r.masterPort, err)
		}
		return
	}

	// listen to master
	r.startListener()

	r.stats.start()
	r.outputOnStart()

	if r.rateLimitEnabled {
		r.rateLimiter.Start()
	}

	r.sendClientReadyAndWaitForAck()

	// report to master
	go func() {
		for {
			select {
			case data := <-r.stats.messageToRunnerChan:
				if r.state == stateInit || r.state == stateStopped {
					continue
				}
				data["user_count"] = r.numClients
				data["user_classes_count"] = r.userClassesCountFromMaster
				r.client.sendChannel() <- newGenericMessage("stats", data, r.nodeID)
				r.outputOnEevent(data)
			case <-r.shutdownChan:
				r.outputOnStop()
				return
			}
		}
	}()

	// heartbeat
	// See: https://github.com/locustio/locust/commit/a8c0d7d8c588f3980303358298870f2ea394ab93
	go func() {
		var ticker = time.NewTicker(heartbeatInterval)
		for {
			select {
			case <-ticker.C:
				// check for master heartbeat timeout
				if !r.lastMasterHeartbeatTimestamp.IsZero() && time.Now().Sub(r.lastMasterHeartbeatTimestamp) > masterHeartbeatTimeout {
					r.logger.Printf("Didn't get heartbeat from master in over %vs, shutting down.\n", masterHeartbeatTimeout.Seconds())
					r.shutdown()
					return
				}
				// send client heartbeat message
				CPUUsage := GetCurrentCPUUsage()
				MemUsage := GetCurrentMemUsage()
				data := map[string]interface{}{
					"state":                r.state,
					"current_cpu_usage":    CPUUsage,
					"current_memory_usage": MemUsage,
				}
				r.client.sendChannel() <- newGenericMessage("heartbeat", data, r.nodeID)
			case <-r.shutdownChan:
				return
			}
		}
	}()

	Events.Subscribe(EVENT_QUIT, r.onQuiting)
}
