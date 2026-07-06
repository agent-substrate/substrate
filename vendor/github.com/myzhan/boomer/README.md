# boomer [![Build Status](https://github.com/myzhan/boomer/actions/workflows/unittest.yml/badge.svg)](https://github.com/myzhan/boomer/actions) [![Go Report Card](https://goreportcard.com/badge/github.com/myzhan/boomer)](https://goreportcard.com/report/github.com/myzhan/boomer) [![Coverage Status](https://codecov.io/gh/myzhan/boomer/branch/master/graph/badge.svg)](https://codecov.io/gh/myzhan/boomer) [![Documentation Status](https://readthedocs.org/projects/boomer/badge/?version=latest)](https://boomer.readthedocs.io/en/latest/?badge=latest)

## Links

* Locust Website: <a href="http://locust.io">locust.io</a>
* Locust Documentation: <a href="http://docs.locust.io">docs.locust.io</a>
* Boomer Documentation: <a href="https://boomer.readthedocs.io">boomer.readthedocs.io</a>

## Description

Boomer is a better load generator for locust, written in golang. It can spawn thousands of goroutines to run your code concurrently.

It will listen and report to the locust master automatically, your test results will be displayed on the master's web UI.

Use it as a library, not a general-purpose benchmarking tool.

## Versioning

Boomer used to support all versions of locust, even if locust didn't keep backward compatibility.

Now boomer follows locust's versioning, and the master branch works with locust's master branch.

If locust introduces breaking changes, boomer will have a tagged version that works previous version of locust.

## Install

```bash
# Install the master branch
$ go get github.com/myzhan/boomer@master
# Install a tagged version that works with locust 1.6.0
$ go get github.com/myzhan/boomer@v1.6.0
```

### Build

Boomer use [gomq](https://github.com/myzhan/gomq) by default, which is a pure Go implementation of the ZeroMQ protocol.

Because of the instability of gomq, you can switch to [goczmq](https://github.com/zeromq/goczmq).

```bash
# use gomq
$ go build -o a.out main.go
# use goczmq
$ go build -tags 'goczmq' -o a.out main.go
```

If you fail to compile boomer with gomq, try to update gomq first.

```bash
$ go get -u github.com/myzhan/gomq
```

## Examples(main.go)
This is a example of boomer's API. You can find more in the "examples" directory.

```go
package main

import "time"
import "github.com/myzhan/boomer"

func foo(){
    start := time.Now()
    time.Sleep(100 * time.Millisecond)
    elapsed := time.Since(start)

    /*
    Report your test result as a success, if you write it in locust, it will looks like this
    events.request_success.fire(request_type="http", name="foo", response_time=100, response_length=10)
    */
    boomer.RecordSuccess("http", "foo", elapsed.Nanoseconds()/int64(time.Millisecond), int64(10))
}

func bar(){
    start := time.Now()
    time.Sleep(100 * time.Millisecond)
    elapsed := time.Since(start)

    /*
    Report your test result as a failure, if you write it in locust, it will looks like this
    events.request_failure.fire(request_type="udp", name="bar", response_time=100, exception=Exception("udp error"))
    */
    boomer.RecordFailure("udp", "bar", elapsed.Nanoseconds()/int64(time.Millisecond), "udp error")
}

func main(){
    task1 := &boomer.Task{
        Name: "foo",
        // The weight is used to distribute goroutines over multiple tasks.
        Weight: 10,
        Fn: foo,
    }

    task2 := &boomer.Task{
        Name: "bar",
        Weight: 20,
        Fn: bar,
    }

    boomer.Run(task1, task2)
}
```

## Run

For debug purpose, you can run tasks without connecting to the master.

```bash
$ go build -o a.out main.go
./a.out --run-tasks foo,bar
```

Otherwise, start the master using the included `dummy.py`.

```bash
$ locust --master -f dummy.py
```

--max-rps means the max count that all the Task.Fn can be called in one second.

The result may be misleading if you call boomer.RecordSuccess() more than once in Task.Fn.

```bash
$ go build -o a.out main.go
$ ./a.out --max-rps 10000
```

If you want the RPS increase from zero to max-rps or infinity.

```
$ go build -o a.out main.go
# The default interval is 1 second
$ ./a.out --request-increase-rate 10
# Change the interval to 1 minute
# Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h"
$ ./a.out --request-increase-rate 10/1m
```

So far, dummy.py is necessary when starting a master, because locust needs such a file.

Don't worry, dummy.py has nothing to do with your test.

## Profiling

You may think there are bottlenecks in your load generator, don't hesitate to do profiling.

Both CPU and memory profiling are supported.

It's not suggested to run CPU profiling and memory profiling at the same time.

### CPU Profiling

```bash
# 1. run locust master.
# 2. run boomer with cpu profiling for 30 seconds.
$ go run main.go -cpu-profile cpu.pprof -cpu-profile-duration 30s
# 3. start test in the WebUI.
# 4. run pprof.
$ go tool pprof cpu.pprof
Type: cpu
Time: Nov 14, 2018 at 8:04pm (CST)
Duration: 30.17s, Total samples = 12.07s (40.01%)
Entering interactive mode (type "help" for commands, "o" for options)
(pprof) web
```

### Memory Profiling

```bash
# 1. run locust master.
# 2. run boomer with memory profiling for 30 seconds.
$ go run main.go -mem-profile mem.pprof -mem-profile-duration 30s
# 3. start test in the WebUI.
# 4. run pprof and try 'go tool pprof --help' to learn more.
$ go tool pprof -alloc_space mem.pprof
Type: alloc_space
Time: Nov 14, 2018 at 8:26pm (CST)
Entering interactive mode (type "help" for commands, "o" for options)
(pprof) top
```

## Exporter
If you are not satisfied with the build-in web monitor in Locust, you can run prometheus_exporter.py instead of dummy.py as your master.

Try this

```bash
$ locust --master -f prometheus_exporter.py
```

Thanks to Prometheus and Grafana, you will get an awesome dashboard: [Locust for Prometheus](https://grafana.com/grafana/dashboards/12081)

## Contributing

If you are enjoying boomer and willing to add new features to it, you are welcome.

Also, good examples are welcome!!!

## License

Open source licensed under the MIT license (see _LICENSE_ file for details).
