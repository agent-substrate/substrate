// This file is kept to ensure backward compatibility.

package boomer

import (
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"reflect"
	"sync"
	"time"
)

var masterHost string
var masterPort int
var maxRPS int64
var requestIncreaseRate string
var runTasks string
var memoryProfileFile string
var memoryProfileDuration time.Duration
var cpuProfileFile string
var cpuProfileDuration time.Duration

var successRetiredWarning = &sync.Once{}
var failureRetiredWarning = &sync.Once{}

func createRateLimiter(maxRPS int64, requestIncreaseRate string) (rateLimiter RateLimiter, err error) {
	if requestIncreaseRate != "-1" {
		if maxRPS > 0 {
			log.Println("The max RPS that boomer may generate is limited to", maxRPS, "with a increase rate", requestIncreaseRate)
			rateLimiter, err = NewRampUpRateLimiter(maxRPS, requestIncreaseRate, time.Second)
		} else {
			log.Println("The max RPS that boomer may generate is limited by a increase rate", requestIncreaseRate)
			rateLimiter, err = NewRampUpRateLimiter(math.MaxInt64, requestIncreaseRate, time.Second)
		}
	} else {
		if maxRPS > 0 {
			log.Println("The max RPS that boomer may generate is limited to", maxRPS)
			rateLimiter = NewStableRateLimiter(maxRPS, time.Second)
		}
	}
	return rateLimiter, err
}

// According to locust, responseTime should be int64, in milliseconds.
// But previous version of boomer required responseTime to be float64, so sad.
func convertResponseTime(origin interface{}) int64 {
	responseTime := int64(0)
	if _, ok := origin.(float64); ok {
		responseTime = int64(origin.(float64))
	} else if _, ok := origin.(int64); ok {
		responseTime = origin.(int64)
	} else {
		panic(fmt.Sprintf("responseTime should be float64 or int64, not %s", reflect.TypeOf(origin)))
	}
	return responseTime
}

func legacySuccessHandler(requestType string, name string, responseTime interface{}, responseLength int64) {
	successRetiredWarning.Do(func() {
		log.Println("boomer.Events.Publish(\"request_success\") is less performant and deprecated, use boomer.RecordSuccess() instead.")
	})
	defaultBoomer.RecordSuccess(requestType, name, convertResponseTime(responseTime), responseLength)
}

func legacyFailureHandler(requestType string, name string, responseTime interface{}, exception string) {
	failureRetiredWarning.Do(func() {
		log.Println("boomer.Events.Publish(\"request_failure\") is less performant and deprecated, use boomer.RecordFailure() instead.")
	})
	defaultBoomer.RecordFailure(requestType, name, convertResponseTime(responseTime), exception)
}

func initLegacyEventHandlers() {
	Events.Subscribe("request_success", legacySuccessHandler)
	Events.Subscribe("request_failure", legacyFailureHandler)
}

// WeighingTaskSet is a implementation of the TaskSet interface.
// When a Task is added to WeighingTaskSet, it's weight is used
// to calculate the probability to be called.

// Deprecated: Boomer use task's weight to calculate the probability to be called now.
type WeighingTaskSet struct {
	weight int
	offset int
	tasks  []*Task
	index  []int
	lock   sync.RWMutex
}

// NewWeighingTaskSet returns a new WeighingTaskSet.
func NewWeighingTaskSet() *WeighingTaskSet {
	log.Println("Don't use WeighingTaskSet, it will be removed soon.")
	return &WeighingTaskSet{
		weight: 0,
		offset: 0,
		tasks:  make([]*Task, 0),
		index:  make([]int, 0),
	}
}

// AddTask add a Task to the Weighing TaskSet.
// If the task's weight is <= 0, it will be ignored.
func (ts *WeighingTaskSet) AddTask(task *Task) {
	if task.Weight <= 0 {
		return
	}
	ts.lock.Lock()
	ts.tasks = append(ts.tasks, task)
	ts.offset = ts.offset + task.Weight
	ts.index = append(ts.index, ts.offset)
	ts.lock.Unlock()
}

func (ts *WeighingTaskSet) binarySearch(roll int) (task *Task) {
	left := 0
	right := len(ts.index) - 1
	for left <= right {
		mid := (left + right) / 2
		if left == mid {
			if roll < ts.index[mid] {
				return ts.tasks[mid]
			}
			return ts.tasks[mid+1]
		}
		if ts.index[mid-1] <= roll && roll < ts.index[mid] {
			return ts.tasks[mid]
		}
		if roll < ts.index[mid-1] {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return nil
}

// GetTask returns a task in the task set.
func (ts *WeighingTaskSet) GetTask(roll int) (task *Task) {
	if roll < 0 || roll >= ts.offset {
		return nil
	}
	ts.lock.RLock()
	task = ts.binarySearch(roll)
	ts.lock.RUnlock()
	return task
}

// SetWeight sets the weight of the task set.
func (ts *WeighingTaskSet) SetWeight(weight int) {
	ts.weight = weight
}

// GetWeight returns the weight of the task set.
func (ts *WeighingTaskSet) GetWeight() (weight int) {
	return ts.weight
}

// Run will pick up a task in the task set randomly and run.
// It can is used as a Task.Fn.
func (ts *WeighingTaskSet) Run() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	roll := r.Intn(ts.offset)
	task := ts.GetTask(roll)
	task.Fn()
}

func init() {
	flag.Int64Var(&maxRPS, "max-rps", 0, "Max RPS that boomer can generate, disabled by default.")
	flag.StringVar(&requestIncreaseRate, "request-increase-rate", "-1", "Request increase rate, disabled by default.")
	flag.StringVar(&runTasks, "run-tasks", "", "Run tasks without connecting to the master, multiply tasks is separated by comma. Usually, it's for debug purpose.")
	flag.StringVar(&masterHost, "master-host", "127.0.0.1", "Host or IP address of locust master for distributed load testing.")
	flag.IntVar(&masterPort, "master-port", 5557, "The port to connect to that is used by the locust master for distributed load testing.")
	flag.StringVar(&memoryProfileFile, "mem-profile", "", "Enable memory profiling.")
	flag.DurationVar(&memoryProfileDuration, "mem-profile-duration", 30*time.Second, "Memory profile duration.")
	flag.StringVar(&cpuProfileFile, "cpu-profile", "", "Enable CPU profiling.")
	flag.DurationVar(&cpuProfileDuration, "cpu-profile-duration", 30*time.Second, "CPU profile duration.")
}
