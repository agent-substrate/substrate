package boomer

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shirou/gopsutil/process"
)

func castToInt64(num interface{}) (ret int64, ok bool) {
	t_int64, ok := num.(int64)
	if ok {
		return t_int64, true
	}
	t_uint64, ok := num.(uint64)
	if ok {
		return int64(t_uint64), true
	}
	return int64(0), false
}

func round(val float64, roundOn float64, places int) (newVal float64) {
	var round float64
	pow := math.Pow(10, float64(places))
	digit := pow * val
	_, div := math.Modf(digit)
	if div >= roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}
	newVal = round / pow
	return
}

// MD5 returns the md5 hash of strings.
func MD5(slice ...string) string {
	h := md5.New()
	for _, v := range slice {
		io.WriteString(h, v)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

// generate a random nodeID like locust does, using the same algorithm.
func getNodeID() (nodeID string) {
	hostname, _ := os.Hostname()
	id := strings.Replace(uuid.New().String(), "-", "", -1)
	nodeID = fmt.Sprintf("%s_%s", hostname, id)
	return
}

// Now returns the current timestamp in milliseconds.
func Now() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// StartMemoryProfile starts memory profiling and save the results in file.
func StartMemoryProfile(file string, duration time.Duration) (err error) {
	f, err := os.Create(file)
	if err != nil {
		return err
	}

	log.Println("Start memory profiling for", duration)
	time.AfterFunc(duration, func() {
		err = pprof.WriteHeapProfile(f)
		if err != nil {
			log.Println(err)
		}
		f.Close()
		log.Println("Stop memory profiling after", duration)
	})
	return nil
}

// StartCPUProfile starts cpu profiling and save the results in file.
func StartCPUProfile(file string, duration time.Duration) (err error) {
	f, err := os.Create(file)
	if err != nil {
		return err
	}

	log.Println("Start cpu profiling for", duration)
	err = pprof.StartCPUProfile(f)
	if err != nil {
		f.Close()
		return err
	}

	time.AfterFunc(duration, func() {
		pprof.StopCPUProfile()
		f.Close()
		log.Println("Stop CPU profiling after", duration)
	})
	return nil
}

// GetCurrentCPUUsage get current CPU usage
func GetCurrentCPUUsage() float64 {
	currentPid := os.Getpid()
	p, err := process.NewProcess(int32(currentPid))
	if err != nil {
		log.Printf("Fail to get CPU percent, %v\n", err)
		return 0.0
	}
	percent, err := p.CPUPercent()
	if err != nil {
		log.Printf("Fail to get CPU percent, %v\n", err)
		return 0.0
	}
	return percent / float64(runtime.NumCPU())
}

func GetCurrentMemUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}
