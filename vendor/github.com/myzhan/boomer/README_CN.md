# boomer [![Build Status](https://github.com/myzhan/boomer/actions/workflows/unittest.yml/badge.svg)](https://github.com/myzhan/boomer/actions) [![Go Report Card](https://goreportcard.com/badge/github.com/myzhan/boomer)](https://goreportcard.com/report/github.com/myzhan/boomer) [![Coverage Status](https://codecov.io/gh/myzhan/boomer/branch/master/graph/badge.svg)](https://codecov.io/gh/myzhan/boomer) [![Documentation Status](https://readthedocs.org/projects/boomer/badge/?version=latest)](https://boomer.readthedocs.io/en/latest/?badge=latest)

## boomer是什么？

boomer 完整地实现了 locust 的通讯协议，运行在 slave 模式下，用 goroutine 来执行用户提供的测试函数，然后将测试结果上报给运行在 master 模式下的 locust。

与 locust 原生的实现相比，解决了两个问题。一是单台施压机上，能充分利用多个 CPU 核心来施压，二是再也不用提防阻塞 IO 操作导致 gevent 阻塞。

## 版本

boomer 的版本号跟随 locust 的版本，如果 locust 引入不兼容的改动，master 分支会跟随着 locust 做不兼容的改动。同时，当前版本会打上 tag，以便用户继续使用旧版本。

## 安装

```bash
# 安装 master 分支
$ go get github.com/myzhan/boomer@master
# 安装 v1.6.0 版本
$ go get github.com/myzhan/boomer@v1.6.0
```

### 编译
boomer 默认使用 gomq，一个纯 Go 语言实现的 ZeroMQ 客户端。

由于 gomq 还不稳定，可以改用 [goczmq](https://github.com/zeromq/goczmq)。

```bash
# 默认使用 gomq
$ go build -o a.out main.go
# 使用 goczmq
$ go build -tags 'goczmq' -o a.out main.go
```

如果使用 gomq 编译失败，先尝试更新 gomq 的版本。

```bash
$ go get -u github.com/myzhan/gomq
```

## 例子(main.go)
下面演示一下 boomer 的 API，可以在 examples 目录下找到更多的例子。

```go
package main

import "time"
import "github.com/myzhan/boomer"

func foo(){
    start := time.Now()
    time.Sleep(100 * time.Millisecond)
    elapsed := time.Since(start)

    /*
    汇报一个成功的结果，实际使用时，根据实际场景，自行判断成功还是失败
    */
    boomer.RecordSuccess("http", "foo", elapsed.Nanoseconds()/int64(time.Millisecond), int64(10))
}

func bar(){
    start := time.Now()
    time.Sleep(100 * time.Millisecond)
    elapsed := time.Since(start)

    /*
    汇报一个失败的结果，实际使用时，根据实际场景，自行判断成功还是失败
    */
    boomer.RecordFailure("udp", "bar", elapsed.Nanoseconds()/int64(time.Millisecond), "udp error")
}

func main(){
    task1 := &boomer.Task{
        // 同时跑多个 tasks 的时候，Weight 字段用于分配 goroutines
        Weight: 10,
        Fn: foo,
    }

    task2 := &boomer.Task{
        Weight: 20,
        Fn: bar,
    }

    // 连接到 master，等待页面上下发指令，支持多个 Task
    boomer.Run(task1, task2)
}
```

## 使用

为了方便调试，可以单独运行 task，不必连接到 master。

```bash
$ go build -o a.out main.go
$ ./a.out --run-tasks foo,bar
```

--max-rps 表示一秒内所有 Task.Fn 函数能被调用的最多次数。

下面这种情况，如果在同一个 Task.Fn 函数里面多次调用 boomer.RecordSuccess()，那么统计到的 RPS 会超过 10000。

```bash
$ go build -o a.out main.go
$ ./a.out --max-rps 10000
```

线性增长的 RPS，从 0 开始，每秒增加 10 个请求。

```bash
$ go build -o a.out main.go
# 默认间隔 1 秒增加 1 次
$ ./a.out --request-increase-rate 10
# 间隔 1 分钟增加 1 次
# 有效的时间单位 "ns", "us" (or "µs"), "ms", "s", "m", "h"
$ ./a.out --request-increase-rate 10/1m
```

locust 启动时，需要一个 locustfile，随便一个符合它要求的即可，这里提供了一个 dummy.py。

由于我们实际上使用 boomer 来施压，这个文件并不会影响到测试。

## 调优

如果你觉得压测工具有性能问题，可以使用内置的 pprof 来获取运行时的 CPU 和内存信息，进行排查和调优。

虽然支持，但是不建议同时运行 CPU 和内存信息采样。

### CPU 调优

```bash
# 1. 启动 locust。
# 2. 启动 boomer，进行 30 秒的 CPU 信息采样。
$ go run main.go -cpu-profile cpu.pprof -cpu-profile-duration 30s
# 3. 在 Web 界面上启动测试。
# 4. 运行 pprof。
$ go tool pprof cpu.pprof
Type: cpu
Time: Nov 14, 2018 at 8:04pm (CST)
Duration: 30.17s, Total samples = 12.07s (40.01%)
Entering interactive mode (type "help" for commands, "o" for options)
(pprof) web
```

### 内存调优

```bash
# 1. 启动 locust。
# 2. 启动 boomer，进行 30 秒的内存信息采样。
$ go run main.go -mem-profile mem.pprof -mem-profile-duration 30s
# 3. 在 Web 界面上启动测试。
# 4. 运行 pprof。
$ go tool pprof -alloc_space mem.pprof
Type: alloc_space
Time: Nov 14, 2018 at 8:26pm (CST)
Entering interactive mode (type "help" for commands, "o" for options)
(pprof) top
```

## 贡献

欢迎给 boomer 提交 PR，无论是新增功能或者是补充使用例子。

## License

Open source licensed under the MIT license (see _LICENSE_ file for details).
