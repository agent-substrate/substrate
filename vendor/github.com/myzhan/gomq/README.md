# GoMQ [![Build Status](https://travis-ci.org/myzhan/gomq.svg?branch=master)](https://travis-ci.org/myzhan/gomq) [![Doc Status](https://godoc.org/github.com/myzhan/gomq?status.png)](https://godoc.org/github.com/myzhan/gomq)

## Introduction
A pure Go implementation of the [ZeroMQ Message Transport Protocol](http://rfc.zeromq.org/spec:37). **Danger Will Robinson, Danger!** This code is very young. There will be false starts, APIs will change and things will break. If you are looking to use ZeroMQ with Go in a production project, we suggest using [GoCZMQ](http://github.com/zeromq/goczmq), which depends on the [CZMQ](http://github.com/zeromq/czmq) C API. 

## Problem Statement
We want to use ZeroMQ in Go projects. While GoCZMQ provides a way to do this, dealing with C dependencies while writing Go is not fun - and we like fun.

## Proposed Solution
GoMQ will be a pure Go implementation of a subset of ZMTP, wrapped with a friendly API. GoMQ will only implement ZMTP version 3.x and greater, and will not be backwards compatible with previous versions of ZMTP. The initial implementation aims to support the following ZMTP features:
* ZMQ_CLIENT / ZMQ_SERVER sockets
* ZMQ_RADAR / ZMQ_DISH sockets
* The NULL security mechanism
* The PLAIN security mechanism
* The CURVE securty mechanism

## Contribution Guidelines
GoMQ adheres to the [Collective Code Construction Contract](http://rfc.zeromq.org/spec:22). For details, see [CONTRIBUTING.md](https://github.com/myzhan/gomq/blob/master/CONTRIBUTING.md). We believe that building a community is an essential component of succesful open source software, and not just a side effect. [People before code!](http://hintjens.com/blog:95)

## Setting Up Your Development Environment
While the end goal of GoMQ is a pure Go implementation of ZeroMQ with no dependencies on cgo, we currently require cgo for our test suite. The friction this creates in getting started is unfortunate but we feel it's the best way to test interoperability between our implemenation and the reference implementation of the protocol. Assuming that you already have a working Go development environment ( see: [The Go Programming Language: Getting Started](https://golang.org/doc/install) ) you will additionally need the following libraries:
* [libsodium](https://github.com/jedisct1/libsodium)
* [libzmq](https://github.com/zeromq/libzmq)
* [czmq](https://github.com/zeromq/czmq)

Because we are implementing ZeroMQ socket types that are not yet included in a stable release, we are developing against git master checkouts of libzmq and czmq. These build instructions were tested on Ubuntu 15.10. If someone would like to provide guides for getting started on Windows, that would be great!

### Linux & OSX

Note: Each of these libraries need you to run `sudo ldconfig` if you are on Linux. You can skip it if you are on OSX.

*Install libsodium*
```
wget https://download.libsodium.org/libsodium/releases/libsodium-1.0.8.tar.gz
wget https://download.libsodium.org/libsodium/releases/libsodium-1.0.8.tar.gz.sig
wget https://download.libsodium.org/jedi.gpg.asc
gpg --import jedi.gpg.asc
gpg --verify libsodium-1.0.8.tar.gz.sig libsodium-1.0.8.tar.gz
tar zxvf libsodium-1.0.8.tar.gz
cd libsodium-1.0.8
./configure; make check
sudo make install
sudo ldconfig
```

On OSX, verify that, the output of `ls -al /usr/local/lib/libsodium.dylib` is:
`lrwxr-xr-x  1 root  admin    18B Feb 21 10:35 /usr/local/lib/libsodium.dylib@ -> libsodium.18.dylib`

*Building libzmq from master*
```
git clone git@github.com:zeromq/libzmq.git
cd libzmq
./autogen.sh
./configure --with-libsodium
make check
sudo make install
sudo ldconfig
```

On OSX, verify that, the output of `ls -al /usr/local/lib/libzmq.dylib` is:
`lrwxr-xr-x  1 dhanush  admin    29B Feb 21 15:55 /usr/local/lib/libzmq.dylib@ -> /usr/local/lib/libzmq.5.dylib`

*Building czmq from master*
```
git clone git@github.com:zeromq/czmq.git
cd libzmq
./autogen.sh
./configure
make check
sudo make install
sudo ldconfig
```

On OSX, verify that, the output of `ls -al /usr/local/lib/libczmq.dylib` is:
`lrwxr-xr-x  1 root  admin    15B Feb 21 15:57 /usr/local/lib/libczmq.dylib@ -> libczmq.3.dylib`

Note: if for some reason libzmq or czmq do not build properly or have failing tests, don't panic! This is an excellent opportunity to get involved in the community. If you can figure out the problem on you own and fix it, send us a pull request. If you're stumped, feel free to hop on the [ZeroMQ mailing list](http://zeromq.org/docs:mailing-lists) and describe the problem you're running into.

## Getting Started
You should now be ready to get started. Fork gomq, clone it, and make sure the tests now work in your environment:

```
 go test -v
=== RUN   TestNewClient
--- PASS: TestNewClient (0.00s)
	socket_test.go:60: server received: "HELLO"
	socket_test.go:32: client received: "WORLD"
	socket_test.go:70: server received: "GOODBYE"
=== RUN   TestExternalServer
--- PASS: TestExternalServer (0.25s)
	socket_test.go:94: client received: "WORLD"
PASS
ok		github.com/myzhan/gomq	0.255s
```

Now you're ready. Remember: pull requests should always be simple solutions to minimal problems. If you're stuck, want to discuss ideas or just want to say hello, some of us are usually lurking in the #zeromq channel on the [gophers slack](https://blog.gopheracademy.com/gophers-slack-community/).

## Helpful Reference Material
* [The Collective Code Construction Contract](http://rfc.zeromq.org/spec:22)
* [The ZeroMQ Message Transport Protocol Specification](http://rfc.zeromq.org/spec:37)
* [The ZeroMQ Guide](http://zguide.zeromq.org/page:all)
